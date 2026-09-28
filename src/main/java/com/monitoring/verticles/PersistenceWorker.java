package com.monitoring.verticles;

import com.monitoring.util.EventBusAddresses;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Worker verticle handling disk persistence for targets and buffered audit logs.
 */
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

        loadTargetsFromDisk();

        vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.PERSIST_TARGET_SAVE, msg -> handleSaveTarget(msg.body()));
        vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.PERSIST_TARGET_DELETE, msg -> handleDeleteTarget(msg.body()));
        vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.PERSIST_TARGET_LOAD, this::handleLoadTargets);
        vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.AUDIT_LOG, msg -> handleAuditLog(msg.body()));

        timerId = vertx.setPeriodic(flushIntervalMs, id -> flushAuditBuffer());

        log.info("PersistenceWorker started (targets: {}, audit: {})", targetsFilePath, auditLogFilePath);
        startPromise.complete();
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
        JsonArray array = new JsonArray();
        persistedTargets.values().forEach(array::add);
        msg.reply(new JsonObject().put("targets", array).put("count", array.size()));
    }

    private void handleAuditLog(JsonObject entry) {
        if (entry != null) {
            auditBuffer.add(entry);
            if (auditBuffer.size() >= bufferCapacity) {
                flushAuditBuffer();
            }
        }
    }

    private void loadTargetsFromDisk() {
        File file = new File(targetsFilePath);
        if (!file.exists()) {
            return;
        }

        try {
            String content = Files.readString(file.toPath(), StandardCharsets.UTF_8);
            if (!content.isBlank()) {
                JsonObject json = new JsonObject(content);
                JsonArray array = json.getJsonArray("targets", new JsonArray());
                for (int i = 0; i < array.size(); i++) {
                    JsonObject target = array.getJsonObject(i);
                    String id = target.getString("id");
                    if (id != null) {
                        persistedTargets.put(id, target);
                    }
                }
                log.info("Loaded {} targets from {}", persistedTargets.size(), targetsFilePath);
            }
        } catch (Exception e) {
            log.warn("Failed to read {}: {}", targetsFilePath, e.getMessage());
        }
    }

    private void writeTargetsToDisk() {
        try {
            File file = new File(targetsFilePath);
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }

            JsonArray array = new JsonArray();
            persistedTargets.values().forEach(array::add);
            JsonObject json = new JsonObject().put("targets", array);

            Files.writeString(file.toPath(), json.encodePrettily(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("Failed to write to {}", targetsFilePath, e);
        }
    }

    private void flushAuditBuffer() {
        if (auditBuffer.isEmpty()) {
            return;
        }

        List<JsonObject> toFlush = new ArrayList<>(auditBuffer);
        auditBuffer.clear();

        try {
            File file = new File(auditLogFilePath);
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }

            StringBuilder sb = new StringBuilder();
            for (JsonObject entry : toFlush) {
                sb.append(entry.encode()).append("\n");
            }

            Files.writeString(file.toPath(), sb.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception e) {
            log.error("Failed to flush audit logs to {}", auditLogFilePath, e);
        }
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        if (timerId != -1L) {
            vertx.cancelTimer(timerId);
        }
        flushAuditBuffer();
        stopPromise.complete();
    }
}
