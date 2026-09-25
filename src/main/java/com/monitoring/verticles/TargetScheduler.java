package com.monitoring.verticles;

import com.monitoring.util.EventBusAddresses;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Schedules periodic target health checks using a priority queue and a single tick timer.
 */
public class TargetScheduler extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(TargetScheduler.class);

    private record Task(String id, JsonObject config, long nextRun, long intervalMs, long version) {}

    private final PriorityQueue<Task> queue = new PriorityQueue<>(Comparator.comparingLong(Task::nextRun));
    private final Map<String, Long> activeVersions = new ConcurrentHashMap<>();

    private long timerId = -1L;
    private int tickIntervalMs = 20;
    private long versionSeq = 0L;
    private boolean paused = false;

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject config = config().getJsonObject("monitoring", new JsonObject());
        tickIntervalMs = config.getInteger("tickIntervalMs", 20);

        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.SCHEDULER_TARGET_ADD, this::handleAdd);
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.SCHEDULER_TARGET_REMOVE, this::handleRemove);
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.SCHEDULER_PAUSE, this::handlePause);

        timerId = vertx.setPeriodic(tickIntervalMs, id -> onTick());

        log.info("TargetScheduler started (tick: {}ms)", tickIntervalMs);
        startPromise.complete();
    }

    private void handleAdd(Message<JsonObject> msg) {
        JsonObject target = msg.body();
        String id = target.getString("id");
        int intervalSec = target.getInteger("intervalSeconds", 1);
        long intervalMs = intervalSec * 1000L;

        long version = ++versionSeq;
        activeVersions.put(id, version);

        queue.offer(new Task(id, target, System.currentTimeMillis(), intervalMs, version));
    }

    private void handleRemove(Message<JsonObject> msg) {
        String id = msg.body().getString("id");
        if (id != null) {
            activeVersions.remove(id);
        }
    }

    private void handlePause(Message<JsonObject> msg) {
        this.paused = msg.body().getBoolean("pause", true);
        msg.reply(new JsonObject().put("paused", paused));
    }

    private void onTick() {
        if (paused) {
            return;
        }

        long now = System.currentTimeMillis();

        while (!queue.isEmpty()) {
            Task task = queue.peek();
            if (task.nextRun() > now) {
                break;
            }

            queue.poll();

            Long activeVersion = activeVersions.get(task.id());
            if (activeVersion == null || activeVersion != task.version()) {
                continue; // Skip deleted or updated target
            }

            vertx.eventBus().send(EventBusAddresses.CHECK_EXECUTE, task.config());

            long nextRun = Math.max(task.nextRun() + task.intervalMs(), now + task.intervalMs());
            queue.offer(new Task(task.id(), task.config(), nextRun, task.intervalMs(), task.version()));
        }
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        if (timerId != -1L) {
            vertx.cancelTimer(timerId);
        }
        queue.clear();
        activeVersions.clear();
        stopPromise.complete();
    }
}
