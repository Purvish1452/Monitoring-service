package com.monitoring.verticles;

import com.monitoring.stats.SlidingWindowStats;
import com.monitoring.stats.TargetHealthFSM;
import com.monitoring.util.EventBusAddresses;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tracks sliding window latency statistics and manages health state alerts.
 */
public class StatsManager extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(StatsManager.class);

    private final Map<String, SlidingWindowStats> statsMap = new HashMap<>();
    private final Map<String, TargetHealthFSM> fsmMap = new HashMap<>();
    private final Map<String, JsonObject> activeAlerts = new HashMap<>();
    private final List<JsonObject> alertHistory = new ArrayList<>();

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.CHECK_RESULT, msg -> handleCheckResult(msg.body()));
        vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.ALERTS_LIST, this::handleListAlerts);
        vertx.eventBus().<JsonObject>localConsumer(EventBusAddresses.STATS_GET, this::handleGetStats);

        log.info("StatsManager started");
        startPromise.complete();
    }

    private void handleCheckResult(JsonObject result) {
        String targetId = result.getString("targetId");
        if (targetId == null || targetId.isBlank()) {
            return;
        }

        long timestamp = result.getLong("timestamp", System.currentTimeMillis());
        String status = result.getString("status", "FAILURE");
        long latencyMs = result.getLong("latencyMs", 0L);

        // 1. Update rolling window statistics
        SlidingWindowStats stats = statsMap.computeIfAbsent(targetId, k -> new SlidingWindowStats());
        stats.record(timestamp, latencyMs);

        // 2. Evaluate health state machine
        TargetHealthFSM fsm = fsmMap.computeIfAbsent(targetId, k -> new TargetHealthFSM());
        TargetHealthFSM.Transition transition = fsm.update(status, latencyMs, stats.getAverageLatency1m(timestamp));

        // 3. Sync live state and rolling averages with TargetManager
        JsonObject stateUpdate = new JsonObject()
                .put("id", targetId)
                .put("state", fsm.getState().name())
                .put("avg1m", stats.getAverageLatency1m(timestamp))
                .put("avg5m", stats.getAverageLatency5m(timestamp))
                .put("lastLatencyMs", latencyMs);
        vertx.eventBus().send(EventBusAddresses.TARGET_STATE_UPDATE, stateUpdate);

        // 4. Handle state transitions and alerts
        if (transition.changed()) {
            JsonObject alert = new JsonObject()
                    .put("targetId", targetId)
                    .put("timestamp", timestamp)
                    .put("fromState", transition.from().name())
                    .put("toState", transition.to().name())
                    .put("reason", transition.reason())
                    .put("severity", transition.to().name());

            if (transition.to() == TargetHealthFSM.State.HEALTHY) {
                activeAlerts.remove(targetId);
            } else {
                activeAlerts.put(targetId, alert);
            }

            if (alertHistory.size() >= 500) {
                alertHistory.remove(0);
            }
            alertHistory.add(alert);

            vertx.eventBus().publish(EventBusAddresses.ALERT_EVENTS, alert);
            vertx.eventBus().send(EventBusAddresses.AUDIT_LOG, new JsonObject()
                    .put("action", "STATE_TRANSITION")
                    .put("targetId", targetId)
                    .put("fromState", transition.from().name())
                    .put("toState", transition.to().name())
                    .put("reason", transition.reason())
                    .put("timestamp", timestamp));

            log.info("Target {} state changed: {} -> {} ({})", targetId, transition.from(), transition.to(), transition.reason());
        }
    }

    private void handleListAlerts(Message<JsonObject> msg) {
        JsonArray array = new JsonArray();
        activeAlerts.values().forEach(array::add);
        msg.reply(new JsonObject().put("alerts", array).put("count", array.size()));
    }

    private void handleGetStats(Message<JsonObject> msg) {
        String targetId = msg.body().getString("targetId");
        SlidingWindowStats stats = statsMap.get(targetId);
        TargetHealthFSM fsm = fsmMap.get(targetId);

        if (stats == null && fsm == null) {
            msg.reply(new JsonObject().put("found", false));
            return;
        }

        long now = System.currentTimeMillis();
        JsonObject data = stats != null ? stats.toJsonObject(now) : new JsonObject();
        data.put("targetId", targetId);
        data.put("state", fsm != null ? fsm.getState().name() : "HEALTHY");
        data.put("found", true);

        msg.reply(data);
    }
}