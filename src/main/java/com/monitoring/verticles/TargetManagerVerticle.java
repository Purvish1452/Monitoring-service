package com.monitoring.verticles;

import com.monitoring.util.EventBusAddresses;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TargetManagerVerticle:
 * 1. Maintains in-memory target registry (Map<String, JsonObject>).
 * 2. Handles target registration, bulk registration, lookup, listing, and deletion.
 * 3. Syncs target state changes to TargetSchedulerVerticle via EventBus.
 * 4. Implements immediate bulk re-check trigger (AC 7: non-blocking HTTP 202).
 */
public class TargetManagerVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(TargetManagerVerticle.class);

    private final Map<String, JsonObject> targets = new ConcurrentHashMap<>();

    @Override
    public void start(Promise<Void> startPromise) {
        // 1. Single Target Registration
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.TARGET_REGISTER, msg -> {
            JsonObject target = msg.body();
            if (!isValidTarget(target)) {
                msg.fail(400, "Invalid target payload: must include either 'url' (HTTP) or 'ip'+'port' (TCP)");
                return;
            }

            normalizeTarget(target);
            String id = target.getString("id");
            targets.put(id, target);

            // Notify scheduler to register target
            vertx.eventBus().send(EventBusAddresses.SCHEDULER_TARGET_ADD, target);

            // Notify persistence worker
            vertx.eventBus().send(EventBusAddresses.PERSIST_TARGET_SAVE, target);

            msg.reply(new JsonObject().put("status", "CREATED").put("target", target));
        });

        // 2. Bulk Target Registration
        vertx.eventBus().<JsonArray>consumer(EventBusAddresses.TARGET_REGISTER_BULK, msg -> {
            JsonArray bulkList = msg.body();
            int registeredCount = 0;

            for (int i = 0; i < bulkList.size(); i++) {
                JsonObject target = bulkList.getJsonObject(i);
                if (isValidTarget(target)) {
                    normalizeTarget(target);
                    String id = target.getString("id");
                    targets.put(id, target);

                    vertx.eventBus().send(EventBusAddresses.SCHEDULER_TARGET_ADD, target);
                    registeredCount++;
                }
            }

            msg.reply(new JsonObject()
                    .put("status", "ACCEPTED")
                    .put("registeredCount", registeredCount)
                    .put("totalReceived", bulkList.size()));
        });

        // 3. List All Targets
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.TARGET_LIST, msg -> {
            JsonArray array = new JsonArray();
            for (JsonObject t : targets.values()) {
                array.add(t);
            }
            msg.reply(new JsonObject().put("targets", array).put("count", array.size()));
        });

        // 4. Get Target by ID
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.TARGET_GET, msg -> {
            String targetId = msg.body().getString("targetId");
            JsonObject target = (targetId != null) ? targets.get(targetId) : null;
            if (target != null) {
                msg.reply(new JsonObject().put("found", true).put("target", target));
            } else {
                msg.reply(new JsonObject().put("found", false));
            }
        });

        // 5. Delete Target
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.TARGET_DELETE, msg -> {
            String targetId = msg.body().getString("targetId");
            if (targetId == null || !targets.containsKey(targetId)) {
                msg.fail(404, "Target not found: " + targetId);
                return;
            }

            targets.remove(targetId);

            // Notify scheduler & persistence to remove target
            vertx.eventBus().send(EventBusAddresses.SCHEDULER_TARGET_REMOVE, new JsonObject().put("id", targetId));
            vertx.eventBus().send(EventBusAddresses.PERSIST_TARGET_DELETE, new JsonObject().put("id", targetId));

            msg.reply(new JsonObject().put("status", "DELETED").put("targetId", targetId));
        });

        // 6. Bulk Immediate Re-Check (AC 7)
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.TARGET_BULK_CHECK, msg -> {
            int dispatchedCount = 0;
            for (JsonObject target : targets.values()) {
                vertx.eventBus().send(EventBusAddresses.CHECK_EXECUTE, target);
                dispatchedCount++;
            }

            // Immediately reply with 202 Accepted (satisfying AC 7)
            msg.reply(new JsonObject()
                    .put("status", "TRIGGERED")
                    .put("dispatchedCount", dispatchedCount));
        });

        logger.info("TargetManagerVerticle started successfully.");
        startPromise.complete();
    }

    private boolean isValidTarget(JsonObject target) {
        if (target == null) {
            return false;
        }
        boolean hasUrl = target.containsKey("url") && target.getString("url") != null && !target.getString("url").isBlank();
        boolean hasTcp = target.containsKey("ip") && target.containsKey("port") && target.getInteger("port") != null;
        return hasUrl || hasTcp;
    }

    private void normalizeTarget(JsonObject target) {
        if (!target.containsKey("id") || target.getString("id") == null || target.getString("id").isBlank()) {
            target.put("id", "target-" + java.util.UUID.randomUUID().toString().substring(0, 8));
        }
        if (!target.containsKey("type")) {
            if (target.containsKey("url")) {
                target.put("type", "HTTP");
            } else {
                target.put("type", "TCP");
            }
        }
        if (!target.containsKey("intervalSeconds") || target.getInteger("intervalSeconds") < 1) {
            target.put("intervalSeconds", 1);
        }
        if (!target.containsKey("timeoutMs") || target.getLong("timeoutMs") < 50) {
            target.put("timeoutMs", 2000L);
        }
    }
}
