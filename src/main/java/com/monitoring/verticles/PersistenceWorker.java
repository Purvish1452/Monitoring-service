package com.monitoring.verticles;

import com.monitoring.util.EventBusAddresses;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PersistenceWorker extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(PersistenceWorker.class);

    private String targetsFilePath = "data/targets.json";
    private String auditLogFilePath = "data/audit.jsonl";
    private long flushIntervalMs = 1000L;
    private int bufferCapacity = 100;

    private final Map<String, JsonObject> persistedTargets = new HashMap<>();
    private final List<JsonObject> auditBuffer = new ArrayList<>();
    private long timerId = -1L;

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject config = config().getJsonObject("persistence", new JsonObject());
        this.targetsFilePath = config.getString("targetsFilePath", "data/targets.json");
        this.auditLogFilePath = config.getString("auditLogFilePath", "data/audit.jsonl");
        this.flushIntervalMs = config.getLong("flushIntervalMs", 1000L);
        this.bufferCapacity = config.getInteger("bufferCapacity", 100);

        ensureDirectories()
                .compose(v -> loadTargets())
                .onComplete(ar -> {
                    vertx.eventBus().localConsumer(EventBusAddresses.PERSIST_TARGET_SAVE, this::handleSaveTarget);
                    vertx.eventBus().localConsumer(EventBusAddresses.PERSIST_TARGET_DELETE, this::handleDeleteTarget);
                    vertx.eventBus().localConsumer(EventBusAddresses.PERSIST_TARGET_LOAD, this::handleLoadTargets);
                    vertx.eventBus().localConsumer(EventBusAddresses.AUDIT_LOG, this::handleAuditLog);

                    timerId = vertx.setPeriodic(flushIntervalMs, id -> flushAuditBuffer());

                    log.info("PersistenceWorker started (targets: {}, audit: {})", targetsFilePath, auditLogFilePath);
                    startPromise.complete();
                });
    }

    private Future<Void> loadTargets() {
        return vertx.fileSystem().readFile(targetsFilePath)
                .onSuccess(buffer -> {
                    try {
                        String content = buffer.toString();
                        if (!content.isBlank()) {
                            JsonArray array = new JsonObject(content).getJsonArray("targets", new JsonArray());
                            for (int i = 0; i < array.size(); i++) {
                                JsonObject target = array.getJsonObject(i);
                                persistedTargets.put(target.getString("id"), target);
                            }
                            log.info("Loaded {} targets from {}", persistedTargets.size(), targetsFilePath);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to parse targets file: {}", e.getMessage());
                    }
                })
                .onFailure(err -> log.debug("No existing targets file at {}", targetsFilePath))
                .mapEmpty();
    }

    private void handleSaveTarget(Message<JsonObject> msg) {
        JsonObject target = msg.body();
        if (target != null && target.getString("id") != null) {
            persistedTargets.put(target.getString("id"), target.copy());
            writeTargetsToDisk();
        }
    }

    private void handleDeleteTarget(Message<JsonObject> msg) {
        JsonObject payload = msg.body();
        if (payload != null) {
            String id = payload.getString("id");
            if (id != null && persistedTargets.remove(id) != null) {
                writeTargetsToDisk();
            }
        }
    }

    private void handleLoadTargets(Message<JsonObject> msg) {
        JsonArray array = new JsonArray(new ArrayList<>(persistedTargets.values()));
        msg.reply(new JsonObject().put("targets", array).put("count", array.size()));
    }

    private void handleAuditLog(Message<JsonObject> msg) {
        JsonObject entry = msg.body();
        if (entry == null) return;
        auditBuffer.add(entry);
        if (auditBuffer.size() >= bufferCapacity) {
            flushAuditBuffer();
        }
    }

    private void writeTargetsToDisk() {
        JsonArray array = new JsonArray(new ArrayList<>(persistedTargets.values()));
        String payload = new JsonObject().put("targets", array).encodePrettily();
        vertx.fileSystem().writeFile(targetsFilePath, Buffer.buffer(payload))
                .onFailure(err -> log.error("Failed to write targets file", err));
    }

    private void flushAuditBuffer() {
        if (auditBuffer.isEmpty()) return;

        StringBuilder sb = new StringBuilder();
        for (JsonObject entry : auditBuffer) {
            sb.append(entry.encode()).append('\n');
        }
        auditBuffer.clear();

        appendToFile(auditLogFilePath, Buffer.buffer(sb.toString()))
                .onFailure(err -> log.error("Failed to append audit log", err));
    }

    private Future<Void> appendToFile(String path, Buffer buffer) {
        OpenOptions options = new OpenOptions().setAppend(true).setCreate(true).setWrite(true);
        return vertx.fileSystem().open(path, options)
                .compose(file -> file.write(buffer).compose(v -> file.close()));
    }

    private Future<Void> ensureDirectories() {
        Path targetParent = Path.of(targetsFilePath).getParent();
        Path auditParent = Path.of(auditLogFilePath).getParent();

        Future<Void> f1 = targetParent != null ? vertx.fileSystem().mkdirs(targetParent.toString()) : Future.succeededFuture();
        Future<Void> f2 = auditParent != null ? vertx.fileSystem().mkdirs(auditParent.toString()) : Future.succeededFuture();

        return Future.all(f1, f2).mapEmpty();
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        if (timerId != -1L) {
            vertx.cancelTimer(timerId);
        }

        if (auditBuffer.isEmpty()) {
            stopPromise.complete();
            return;
        }

        StringBuilder sb = new StringBuilder();
        for (JsonObject entry : auditBuffer) {
            sb.append(entry.encode()).append('\n');
        }
        auditBuffer.clear();

        appendToFile(auditLogFilePath, Buffer.buffer(sb.toString()))
                .onComplete(ar -> stopPromise.complete());
    }
}