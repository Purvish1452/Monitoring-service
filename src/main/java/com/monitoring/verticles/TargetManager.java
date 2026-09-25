package com.monitoring.verticles;

import com.monitoring.util.EventBusAddresses;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory registry for monitored targets.
 */
public class TargetManager extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(TargetManager.class);

    private final Map<String, JsonObject> targets = new ConcurrentHashMap<>();

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.TARGET_REGISTER, this::handleRegister);
        vertx.eventBus().<JsonArray>consumer(EventBusAddresses.TARGET_REGISTER_BULK, this::handleRegisterBulk);
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.TARGET_LIST, this::handleList);
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.TARGET_GET, this::handleGet);
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.TARGET_DELETE, this::handleDelete);
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.TARGET_BULK_CHECK, this::handleBulkCheck);

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

        msg.reply(new JsonObject().put("status", "CREATED").put("target", target));
    }

    private void handleRegisterBulk(Message<JsonArray> msg) {
        JsonArray bulkList = msg.body();
        int count = 0;

        for (int i = 0; i < bulkList.size(); i++) {
            JsonObject target = bulkList.getJsonObject(i);
            if (isValid(target)) {
                normalize(target);
                targets.put(target.getString("id"), target);
                vertx.eventBus().send(EventBusAddresses.SCHEDULER_TARGET_ADD, target);
                count++;
            }
        }

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
