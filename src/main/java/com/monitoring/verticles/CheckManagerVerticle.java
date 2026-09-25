package com.monitoring.verticles;

import com.monitoring.util.EventBusAddresses;
import com.monitoring.util.RetryHelper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * CheckManagerVerticle:
 * 1. Controls HOW MANY checks can run simultaneously (maxConcurrentChecks, default 500).
 * 2. Enforces per-target running=true guard to prevent overlapping checks on the same target.
 * 3. Owns reusable WebClient and NetClient instances (non-blocking I/O).
 * 4. Implements exact non-blocking retry timing and strict timeout enforcement.
 */
public class CheckManagerVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(CheckManagerVerticle.class);

    private WebClient webClient;
    private NetClient netClient;

    private int maxConcurrentChecks = 500;
    private int currentInFlight = 0;

    private final Set<String> runningTargets = new HashSet<>();
    private final Queue<JsonObject> pendingQueue = new ArrayDeque<>();

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject monitoringConfig = config().getJsonObject("monitoring", new JsonObject());
        this.maxConcurrentChecks = monitoringConfig.getInteger("maxConcurrentChecks", 500);

        // Configure reusable non-blocking WebClient with connection pool
        WebClientOptions webOptions = new WebClientOptions()
                .setKeepAlive(true)
                .setConnectTimeout(5000)
                .setFollowRedirects(true);
        this.webClient = WebClient.create(vertx, webOptions);

        // Configure reusable non-blocking NetClient for TCP checks
        NetClientOptions netOptions = new NetClientOptions()
                .setConnectTimeout(5000);
        this.netClient = vertx.createNetClient(netOptions);

        // Register check execution consumer on EventBus
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.CHECK_EXECUTE, msg -> {
            JsonObject targetConfig = msg.body();
            handleCheckRequest(targetConfig);
        });

        logger.info("CheckManagerVerticle started with maxConcurrentChecks={}", maxConcurrentChecks);
        startPromise.complete();
    }

    private void handleCheckRequest(JsonObject targetConfig) {
        String targetId = targetConfig.getString("id");
        if (targetId == null || targetId.isBlank()) {
            targetId = "target-" + java.util.UUID.randomUUID().toString().substring(0, 8);
            targetConfig.put("id", targetId);
        }

        // Per-target running guard: prevent overlapping checks for the same target
        if (runningTargets.contains(targetId)) {
            logger.debug("Target {} is already running a check, coalescing/skipping duplicate dispatch", targetId);
            return;
        }

        if (currentInFlight >= maxConcurrentChecks) {
            // Queue pending work compactly
            pendingQueue.offer(targetConfig);
            return;
        }

        // Dispatch check
        startCheck(targetConfig);
    }

    private void startCheck(JsonObject targetConfig) {
        String targetId = targetConfig.getString("id");
        runningTargets.add(targetId);
        currentInFlight++;

        executeAttempt(targetConfig, 1);
    }

    private void executeAttempt(JsonObject targetConfig, int attemptNumber) {
        String targetId = targetConfig.getString("id");
        String type = targetConfig.getString("type", "HTTP").toUpperCase();
        long timeoutMs = targetConfig.getLong("timeoutMs", 3000L);
        JsonObject retryPolicy = targetConfig.getJsonObject("retryPolicy");
        int maxAttempts = RetryHelper.getMaxAttempts(retryPolicy);

        long startTimeMs = System.currentTimeMillis();

        if ("TCP".equals(type)) {
            executeTcpProbe(targetConfig, startTimeMs, timeoutMs)
                    .onSuccess(latencyMs -> finishCheck(targetId, "SUCCESS", latencyMs, attemptNumber, 200, null))
                    .onFailure(err -> handleAttemptFailure(targetConfig, attemptNumber, maxAttempts, retryPolicy, startTimeMs, err));
        } else {
            executeHttpProbe(targetConfig, startTimeMs, timeoutMs)
                    .onSuccess(result -> {
                        int statusCode = result.getInteger("statusCode", 200);
                        long latencyMs = result.getLong("latencyMs", 0L);
                        if (statusCode >= 200 && statusCode < 400) {
                            finishCheck(targetId, "SUCCESS", latencyMs, attemptNumber, statusCode, null);
                        } else {
                            String errorMsg = "HTTP Status " + statusCode;
                            handleAttemptFailure(targetConfig, attemptNumber, maxAttempts, retryPolicy, startTimeMs, new RuntimeException(errorMsg));
                        }
                    })
                    .onFailure(err -> handleAttemptFailure(targetConfig, attemptNumber, maxAttempts, retryPolicy, startTimeMs, err));
        }
    }

    private io.vertx.core.Future<JsonObject> executeHttpProbe(JsonObject targetConfig, long startTimeMs, long timeoutMs) {
        String url = targetConfig.getString("url");
        if (url == null || url.isBlank()) {
            return io.vertx.core.Future.failedFuture("Missing HTTP URL");
        }

        return webClient.getAbs(url)
                .timeout(timeoutMs)
                .send()
                .map(response -> {
                    long latencyMs = System.currentTimeMillis() - startTimeMs;
                    return new JsonObject()
                            .put("statusCode", response.statusCode())
                            .put("latencyMs", latencyMs);
                });
    }

    private io.vertx.core.Future<Long> executeTcpProbe(JsonObject targetConfig, long startTimeMs, long timeoutMs) {
        String host = targetConfig.getString("ip", targetConfig.getString("host", "127.0.0.1"));
        int port = targetConfig.getInteger("port", 80);

        return netClient.connect(port, host)
                .timeout(timeoutMs, TimeUnit.MILLISECONDS)
                .compose(socket -> {
                    long latencyMs = System.currentTimeMillis() - startTimeMs;
                    // Handshake succeeded, immediately close raw socket
                    socket.close();
                    return io.vertx.core.Future.succeededFuture(latencyMs);
                });
    }

    private void handleAttemptFailure(JsonObject targetConfig, int attemptNumber, int maxAttempts,
                                      JsonObject retryPolicy, long startTimeMs, Throwable error) {
        String targetId = targetConfig.getString("id");
        long latencyMs = System.currentTimeMillis() - startTimeMs;

        if (attemptNumber < maxAttempts) {
            // Non-blocking exponential retry delay using vertx.setTimer
            long delayMs = RetryHelper.calculateDelayMs(attemptNumber + 1, retryPolicy);
            logger.debug("Target {} attempt {} failed ({}), retrying in {} ms", targetId, attemptNumber, error.getMessage(), delayMs);

            vertx.setTimer(delayMs, timerId -> executeAttempt(targetConfig, attemptNumber + 1));
        } else {
            // All retry attempts exhausted
            String status = isTimeout(error) ? "TIMEOUT" : "FAILURE";
            finishCheck(targetId, status, latencyMs, attemptNumber, 0, error.getMessage());
        }
    }

    private boolean isTimeout(Throwable error) {
        if (error == null) return false;
        if (error instanceof TimeoutException) return true;
        String msg = error.getMessage();
        return msg != null && (msg.toLowerCase().contains("time") || msg.toLowerCase().contains("timeout"));
    }

    private void finishCheck(String targetId, String status, long latencyMs, int attempts, int statusCode, String errorMessage) {
        long now = System.currentTimeMillis();

        JsonObject checkResult = new JsonObject()
                .put("targetId", targetId)
                .put("timestamp", now)
                .put("status", status)
                .put("latencyMs", Math.max(0, latencyMs))
                .put("attempts", attempts)
                .put("statusCode", statusCode)
                .put("errorMessage", errorMessage);

        // 1. Emit check result for Stats, Alerts, and Persistence (Publish-Subscribe)
        vertx.eventBus().publish(EventBusAddresses.CHECK_RESULT, checkResult);

        // 2. Notify Scheduler that this target's check is completed (for interval coalescing)
        vertx.eventBus().publish(EventBusAddresses.CHECK_COMPLETED, new JsonObject().put("targetId", targetId));

        // 3. Release slot and process next queued check
        runningTargets.remove(targetId);
        currentInFlight--;

        drainPendingQueue();
    }

    private void drainPendingQueue() {
        while (currentInFlight < maxConcurrentChecks && !pendingQueue.isEmpty()) {
            JsonObject nextTarget = pendingQueue.poll();
            String targetId = nextTarget.getString("id");
            if (targetId != null && !runningTargets.contains(targetId)) {
                startCheck(nextTarget);
            }
        }
    }

    public int getCurrentInFlight() {
        return currentInFlight;
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        if (webClient != null) {
            webClient.close();
        }
        if (netClient != null) {
            netClient.close();
        }
        stopPromise.complete();
    }
}
