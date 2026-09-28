package com.monitoring.verticles;

import com.monitoring.util.EventBusAddresses;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * In-memory registry for monitored targets.
 */
public class TargetManager extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(TargetManager.class);

    private final Map<String, JsonObject> targets = new HashMap<>();

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.TARGET_REGISTER, this::handleRegister);
        vertx.eventBus().<JsonArray>localConsumer(EventBusAddresses.TARGET_REGISTER_BULK, this::handleRegisterBulk);
        vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.TARGET_LIST, this::handleList);
        vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.TARGET_GET, this::handleGet);
        vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.TARGET_DELETE, this::handleDelete);
        vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.TARGET_BULK_CHECK, this::handleBulkCheck);
        vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.TARGET_STATE_UPDATE, this::handleStateUpdate);

        // Restore saved targets on startup
        vertx.eventBus().<JsonObject>request(EventBusAddresses.PERSIST_TARGET_LOAD, new JsonObject())
                .onSuccess(reply -> {
                    JsonArray savedTargets = reply.body().getJsonArray("targets", new JsonArray());
                    for (int i = 0; i < savedTargets.size(); i++) {
                        JsonObject target = savedTargets.getJsonObject(i);
                        String id = target.getString("id");
                        if (id != null) {
                            targets.put(id, target);
                            vertx.eventBus().send(EventBusAddresses.SCHEDULER_TARGET_ADD, target);
                        }
                    }
                    if (!savedTargets.isEmpty()) {
                        log.info("Restored {} targets from disk storage", savedTargets.size());
                    }
                })
                .onFailure(err -> log.debug("Persistence load unavailable: {}", err.getMessage()));

        log.info("TargetManager started");
        startPromise.complete();
    }

    private void handleRegister(Message<JsonObject> msg) {
        JsonObject target = msg.body();
        if (!isValid(target)) {
            msg.fail(400, "Target must include either 'url' (HTTP) or 'ip' + 'port' (TCP)");
            return;
        }

        normalize(target);
        String id = target.getString("id");
        targets.put(id, target);

        vertx.eventBus().send(EventBusAddresses.SCHEDULER_TARGET_ADD, target);
        vertx.eventBus().send(EventBusAddresses.PERSIST_TARGET_SAVE, target);
        vertx.eventBus().send(EventBusAddresses.AUDIT_LOG, new JsonObject()
                .put("action", "TARGET_REGISTER")
                .put("targetId", id)
                .put("timestamp", System.currentTimeMillis()));

        msg.reply(new JsonObject().put("status", "CREATED").put("target", target));
    }

    private void handleRegisterBulk(Message<JsonArray> msg) {
        JsonArray bulkList = msg.body();
        int count = 0;

        for (int i = 0; i < bulkList.size(); i++) {
            JsonObject target = bulkList.getJsonObject(i);
            if (isValid(target)) {
                normalize(target);
                String id = target.getString("id");
                targets.put(id, target);
                vertx.eventBus().send(EventBusAddresses.SCHEDULER_TARGET_ADD, target);
                vertx.eventBus().send(EventBusAddresses.PERSIST_TARGET_SAVE, target);
                count++;
            }
        }

        vertx.eventBus().send(EventBusAddresses.AUDIT_LOG, new JsonObject()
                .put("action", "TARGET_REGISTER_BULK")
                .put("count", count)
                .put("timestamp", System.currentTimeMillis()));

        msg.reply(new JsonObject()
                .put("status", "ACCEPTED")
                .put("registeredCount", count)
                .put("totalReceived", bulkList.size()));
    }

    private void handleList(Message<JsonObject> msg) {
        JsonArray array = new JsonArray();
        targets.values().forEach(array::add);
        msg.reply(new JsonObject().put("targets", array).put("count", array.size()));
    }

    private void handleGet(Message<JsonObject> msg) {
        String id = msg.body().getString("targetId");
        JsonObject target = (id != null) ? targets.get(id) : null;
        if (target != null) {
            msg.reply(new JsonObject().put("found", true).put("target", target));
        } else {
            msg.reply(new JsonObject().put("found", false));
        }
    }

    private void handleDelete(Message<JsonObject> msg) {
        String id = msg.body().getString("targetId");
        if (id == null || !targets.containsKey(id)) {
            msg.fail(404, "Target not found: " + id);
            return;
        }

        targets.remove(id);
        vertx.eventBus().send(EventBusAddresses.SCHEDULER_TARGET_REMOVE, new JsonObject().put("id", id));
        vertx.eventBus().send(EventBusAddresses.PERSIST_TARGET_DELETE, new JsonObject().put("id", id));
        vertx.eventBus().send(EventBusAddresses.AUDIT_LOG, new JsonObject()
                .put("action", "TARGET_DELETE")
                .put("targetId", id)
                .put("timestamp", System.currentTimeMillis()));

        msg.reply(new JsonObject().put("status", "DELETED").put("targetId", id));
    }

    private void handleBulkCheck(Message<JsonObject> msg) {
        for (JsonObject target : targets.values()) {
            vertx.eventBus().send(EventBusAddresses.CHECK_EXECUTE, target);
        }
        msg.reply(new JsonObject()
                .put("status", "TRIGGERED")
                .put("dispatchedCount", targets.size()));
    }

    private void handleStateUpdate(Message<JsonObject> msg) {
        JsonObject update = msg.body();
        String id = update.getString("id");
        JsonObject target = targets.get(id);
        if (target != null) {
            target.put("state", update.getString("state"))
                    .put("avg1m", update.getDouble("avg1m"))
                    .put("avg5m", update.getDouble("avg5m"))
                    .put("lastLatencyMs", update.getLong("lastLatencyMs"));
        }
    }

    private boolean isValid(JsonObject target) {
        if (target == null) return false;
        boolean hasUrl = target.containsKey("url") && target.getString("url") != null && !target.getString("url").isBlank();
        boolean hasTcp = target.containsKey("ip") && target.containsKey("port") && target.getInteger("port") != null;
        return hasUrl || hasTcp;
    }

    private void normalize(JsonObject target) {
        String id = target.getString("id");
        if (id == null || id.isBlank()) {
            target.put("id", "target-" + UUID.randomUUID().toString().substring(0, 8));
        }
        if (!target.containsKey("type")) {
            target.put("type", target.containsKey("url") ? "HTTP" : "TCP");
        }
        if (target.getInteger("intervalSeconds", 0) < 1) {
            target.put("intervalSeconds", 1);
        }
        if (target.getLong("timeoutMs", 0L) < 50) {
            target.put("timeoutMs", 2000L);
        }
    }
}
