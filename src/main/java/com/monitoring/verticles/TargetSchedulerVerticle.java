package com.monitoring.verticles;

import com.monitoring.util.EventBusAddresses;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TargetSchedulerVerticle:
 * 1. Maintains a high-performance PriorityQueue of scheduled targets ordered by nextRunTimestampMs.
 * 2. Uses a single periodic Event Loop tick timer (20ms) to inspect due targets (eliminates 5000+ separate timers).
 * 3. Enforces interval coalescing to eliminate catch-up storms.
 * 4. Dispatches due targets to CheckManagerVerticle over EventBus (CHECK_EXECUTE).
 */
public class TargetSchedulerVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(TargetSchedulerVerticle.class);

    private static class ScheduledEntry {
        final String targetId;
        final JsonObject targetConfig;
        final long nextRunTimestampMs;
        final long intervalMs;
        final long version;

        ScheduledEntry(String targetId, JsonObject targetConfig, long nextRunTimestampMs, long intervalMs, long version) {
            this.targetId = targetId;
            this.targetConfig = targetConfig;
            this.nextRunTimestampMs = nextRunTimestampMs;
            this.intervalMs = intervalMs;
            this.version = version;
        }
    }

    private final PriorityQueue<ScheduledEntry> scheduleQueue = new PriorityQueue<>(Comparator.comparingLong(e -> e.nextRunTimestampMs));
    private final Map<String, Long> activeTargetVersions = new ConcurrentHashMap<>();

    private long timerId = -1L;
    private int tickIntervalMs = 20;
    private long globalVersionCounter = 0L;
    private boolean isPaused = false;

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject monitoringConfig = config().getJsonObject("monitoring", new JsonObject());
        this.tickIntervalMs = monitoringConfig.getInteger("tickIntervalMs", 20);

        // 1. Add / Update Target in Scheduler
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.SCHEDULER_TARGET_ADD, msg -> {
            JsonObject target = msg.body();
            String id = target.getString("id");
            int intervalSec = target.getInteger("intervalSeconds", 1);
            long intervalMs = intervalSec * 1000L;

            long version = ++globalVersionCounter;
            activeTargetVersions.put(id, version);

            long now = System.currentTimeMillis();
            // Stagger initial check slightly to spread load if needed
            ScheduledEntry entry = new ScheduledEntry(id, target, now, intervalMs, version);
            scheduleQueue.offer(entry);
        });

        // 2. Remove Target from Scheduler
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.SCHEDULER_TARGET_REMOVE, msg -> {
            String id = msg.body().getString("id");
            if (id != null) {
                activeTargetVersions.remove(id);
            }
        });

        // 3. Pause / Resume Scheduler Control
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.SCHEDULER_PAUSE, msg -> {
            boolean pause = msg.body().getBoolean("pause", true);
            this.isPaused = pause;
            msg.reply(new JsonObject().put("paused", isPaused));
        });

        // Start single 20ms tick timer on the Event Loop
        this.timerId = vertx.setPeriodic(tickIntervalMs, id -> onTick());

        logger.info("TargetSchedulerVerticle started with tickIntervalMs={}", tickIntervalMs);
        startPromise.complete();
    }

    private void onTick() {
        if (isPaused) {
            return;
        }

        long now = System.currentTimeMillis();

        while (!scheduleQueue.isEmpty()) {
            ScheduledEntry head = scheduleQueue.peek();

            if (head.nextRunTimestampMs <= now) {
                scheduleQueue.poll();

                // Validate that this entry is still active and matches current version
                Long activeVersion = activeTargetVersions.get(head.targetId);
                if (activeVersion == null || activeVersion != head.version) {
                    // Target was deleted or updated; discard stale entry
                    continue;
                }

                // Dispatch check to CheckManagerVerticle over EventBus
                vertx.eventBus().send(EventBusAddresses.CHECK_EXECUTE, head.targetConfig);

                // Interval coalescing: if missed multiple ticks, schedule strictly from max(scheduled + interval, now + interval)
                long nextRun = Math.max(head.nextRunTimestampMs + head.intervalMs, now + head.intervalMs);

                ScheduledEntry nextEntry = new ScheduledEntry(
                        head.targetId,
                        head.targetConfig,
                        nextRun,
                        head.intervalMs,
                        head.version
                );

                scheduleQueue.offer(nextEntry);
            } else {
                // Head is in the future; remaining elements in PriorityQueue are also in the future
                break;
            }
        }
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        if (timerId != -1L) {
            vertx.cancelTimer(timerId);
        }
        scheduleQueue.clear();
        activeTargetVersions.clear();
        stopPromise.complete();
    }
}
