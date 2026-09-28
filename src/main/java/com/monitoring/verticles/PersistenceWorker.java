package com.monitoring.verticles;

import com.monitoring.util.EventBusAddresses;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Worker verticle handling disk persistence for targets and buffered audit logs.
 * Uses a dedicated 5-thread WorkerExecutor for isolated disk I/O.
 */
public class PersistenceWorker extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(PersistenceWorker.class);

    private String targetsFilePath = "data/targets.json";
    private String auditLogFilePath = "data/audit.jsonl";
    private long flushIntervalMs = 1000L;
    private int bufferCapacity = 100;
    private int poolSize = 5;

    // Thread-safe concurrent data structures
    private final ConcurrentHashMap<String, JsonObject> persistedTargets = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<JsonObject> auditBuffer = new ConcurrentLinkedQueue<>();
    private final AtomicInteger bufferSize = new AtomicInteger(0);

    // Dedicated file locks for synchronized disk operations
    private final Object targetFileLock = new Object();
    private final Object auditFileLock = new Object();

    private WorkerExecutor executor;
    private long timerId = -1L;

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject config = config().getJsonObject("persistence", new JsonObject());
        this.targetsFilePath = config.getString("targetsFilePath", "data/targets.json");
        this.auditLogFilePath = config.getString("auditLogFilePath", "data/audit.jsonl");
        this.flushIntervalMs = config.getLong("flushIntervalMs", 1000L);
        this.bufferCapacity = config.getInteger("bufferCapacity", 100);
        this.poolSize = config.getInteger("poolSize", 5);

        // Dedicated 5-thread worker executor for persistence
        this.executor = vertx.createSharedWorkerExecutor("persistence-worker-pool", poolSize);

        executor.executeBlocking(() -> {
            loadTargetsFromDisk();
            return null;
        }).onComplete(ar -> {
            vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.PERSIST_TARGET_SAVE, msg -> handleSaveTarget(msg.body()));
            vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.PERSIST_TARGET_DELETE, msg -> handleDeleteTarget(msg.body()));
            vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.PERSIST_TARGET_LOAD, this::handleLoadTargets);
            vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.AUDIT_LOG, msg -> handleAuditLog(msg.body()));

            timerId = vertx.setPeriodic(flushIntervalMs, id -> flushAuditBuffer());

            log.info("PersistenceWorker started (targets: {}, audit: {}, poolSize: {})", targetsFilePath, auditLogFilePath, poolSize);
            startPromise.complete();
        });
    }

    private void handleSaveTarget(JsonObject target) {
        String id = target.getString("id");
        if (id != null) {
            persistedTargets.put(id, target.copy());
            writeTargetsToDisk();
        }
    }

    private void handleDeleteTarget(JsonObject payload) {
        String id = payload.getString("id");
        if (id != null && persistedTargets.remove(id) != null) {
            writeTargetsToDisk();
        }
    }

    private void handleLoadTargets(Message<JsonObject> msg) {
        JsonArray array = new JsonArray(new ArrayList<>(persistedTargets.values()));
        msg.reply(new JsonObject().put("targets", array).put("count", array.size()));
    }

    private void handleAuditLog(JsonObject entry) {
        if (entry == null) return;
        auditBuffer.offer(entry);
        if (bufferSize.incrementAndGet() >= bufferCapacity) {
            flushAuditBuffer();
        }
    }

    private void loadTargetsFromDisk() {
        synchronized (targetFileLock) {
            Path path = Path.of(targetsFilePath);
            if (!Files.exists(path)) return;

            try {
                String content = Files.readString(path);
                if (!content.isBlank()) {
                    JsonArray array = new JsonObject(content).getJsonArray("targets", new JsonArray());
                    for (int i = 0; i < array.size(); i++) {
                        JsonObject target = array.getJsonObject(i);
                        persistedTargets.put(target.getString("id"), target);
                    }
                    log.info("Loaded {} targets from {}", persistedTargets.size(), targetsFilePath);
                }
            } catch (Exception e) {
                log.warn("Could not load {}: {}", targetsFilePath, e.getMessage());
            }
        }
    }

    private void writeTargetsToDisk() {
        executor.executeBlocking(() -> {
            synchronized (targetFileLock) {
                try {
                    JsonArray array = new JsonArray(new ArrayList<>(persistedTargets.values()));
                    String payload = new JsonObject().put("targets", array).encodePrettily();

                    Path path = Path.of(targetsFilePath);
                    if (path.getParent() != null) {
                        Files.createDirectories(path.getParent());
                    }
                    Files.writeString(path, payload);
                } catch (Exception e) {
                    log.error("Failed to write to {}: {}", targetsFilePath, e.getMessage());
                }
            }
            return null;
        });
    }

    private void flushAuditBuffer() {
        if (auditBuffer.isEmpty()) return;

        List<JsonObject> batch = new ArrayList<>();
        JsonObject entry;
        while ((entry = auditBuffer.poll()) != null) {
            batch.add(entry);
            bufferSize.decrementAndGet();
        }

        if (batch.isEmpty()) return;

        StringBuilder sb = new StringBuilder();
        for (JsonObject logEntry : batch) {
            sb.append(logEntry.encode()).append('\n');
        }
        String logsToWrite = sb.toString();

        executor.executeBlocking(() -> {
            synchronized (auditFileLock) {
                try {
                    Path path = Path.of(auditLogFilePath);
                    if (path.getParent() != null) {
                        Files.createDirectories(path.getParent());
                    }
                    Files.writeString(path, logsToWrite, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                } catch (Exception e) {
                    log.error("Failed to flush audit logs to {}: {}", auditLogFilePath, e.getMessage());
                }
            }
            return null;
        });
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        if (timerId != -1L) {
            vertx.cancelTimer(timerId);
        }
        flushAuditBuffer();
        if (executor != null) {
            executor.close().onComplete(ar -> stopPromise.complete());
        } else {
            stopPromise.complete();
        }
    }
}
