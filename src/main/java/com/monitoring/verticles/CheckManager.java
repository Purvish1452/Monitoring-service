package com.monitoring.verticles;

import com.monitoring.util.EventBusAddresses;
import com.monitoring.util.RetryHelper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Manages HTTP and TCP probe executions with concurrency throttling and retry handling.
 */
public class CheckManager extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(CheckManager.class);

    private WebClient webClient;
    private NetClient netClient;

    private int maxConcurrentChecks = 500;
    private int currentInFlight = 0;

    private final Set<String> runningTargets = new HashSet<>();
    private final Queue<JsonObject> pendingQueue = new ArrayDeque<>();

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject config = config().getJsonObject("monitoring", new JsonObject());
        maxConcurrentChecks = config.getInteger("maxConcurrentChecks", 500);

        webClient = WebClient.create(vertx, new WebClientOptions()
                .setKeepAlive(true)
                .setConnectTimeout(5000)
                .setFollowRedirects(true));

        netClient = vertx.createNetClient(new NetClientOptions().setConnectTimeout(5000));

        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.CHECK_EXECUTE, msg -> handleCheckRequest(msg.body()));

        log.info("CheckManager started with maxConcurrentChecks={}", maxConcurrentChecks);
        startPromise.complete();
    }

    private void handleCheckRequest(JsonObject target) {
        String id = target.getString("id");
        if (id == null || id.isBlank()) {
            id = "target-" + UUID.randomUUID().toString().substring(0, 8);
            target.put("id", id);
        }

        if (runningTargets.contains(id)) {
            return;
        }

        if (currentInFlight >= maxConcurrentChecks) {
            pendingQueue.offer(target);
            return;
        }

        startCheck(target);
    }

    private void startCheck(JsonObject target) {
        runningTargets.add(target.getString("id"));
        currentInFlight++;
        executeAttempt(target, 1);
    }

    private void executeAttempt(JsonObject target, int attempt) {
        String id = target.getString("id");
        String type = target.getString("type", "HTTP").toUpperCase();
        long timeoutMs = target.getLong("timeoutMs", 3000L);
        JsonObject retryPolicy = target.getJsonObject("retryPolicy");
        int maxAttempts = RetryHelper.getMaxAttempts(retryPolicy);

        long startTime = System.currentTimeMillis();

        if ("TCP".equals(type)) {
            executeTcpProbe(target, startTime, timeoutMs)
                    .onSuccess(latencyMs -> finishCheck(id, "SUCCESS", latencyMs, attempt, 200, null))
                    .onFailure(err -> handleFailure(target, attempt, maxAttempts, retryPolicy, startTime, err));
        } else {
            executeHttpProbe(target, startTime, timeoutMs)
                    .onSuccess(res -> {
                        int code = res.getInteger("statusCode", 200);
                        long latencyMs = res.getLong("latencyMs", 0L);
                        if (code >= 200 && code < 400) {
                            finishCheck(id, "SUCCESS", latencyMs, attempt, code, null);
                        } else {
                            handleFailure(target, attempt, maxAttempts, retryPolicy, startTime, new RuntimeException("HTTP Status " + code));
                        }
                    })
                    .onFailure(err -> handleFailure(target, attempt, maxAttempts, retryPolicy, startTime, err));
        }
    }

    private Future<JsonObject> executeHttpProbe(JsonObject target, long startTime, long timeoutMs) {
        String url = target.getString("url");
        if (url == null || url.isBlank()) {
            return Future.failedFuture("Missing HTTP URL");
        }

        return webClient.getAbs(url)
                .timeout(timeoutMs)
                .send()
                .map(res -> new JsonObject()
                        .put("statusCode", res.statusCode())
                        .put("latencyMs", System.currentTimeMillis() - startTime));
    }

    private Future<Long> executeTcpProbe(JsonObject target, long startTime, long timeoutMs) {
        String host = target.getString("ip", target.getString("host", "127.0.0.1"));
        int port = target.getInteger("port", 80);

        return netClient.connect(port, host)
                .timeout(timeoutMs, TimeUnit.MILLISECONDS)
                .compose(socket -> {
                    long latencyMs = System.currentTimeMillis() - startTime;
                    socket.close();
                    return Future.succeededFuture(latencyMs);
                });
    }

    private void handleFailure(JsonObject target, int attempt, int maxAttempts,
                               JsonObject retryPolicy, long startTime, Throwable err) {
        String id = target.getString("id");
        long latencyMs = System.currentTimeMillis() - startTime;

        if (attempt < maxAttempts) {
            long delay = RetryHelper.calculateDelayMs(attempt + 1, retryPolicy);
            vertx.setTimer(delay, timerId -> executeAttempt(target, attempt + 1));
        } else {
            String status = isTimeout(err) ? "TIMEOUT" : "FAILURE";
            finishCheck(id, status, latencyMs, attempt, 0, err.getMessage());
        }
    }

    private boolean isTimeout(Throwable err) {
        if (err == null) return false;
        if (err instanceof TimeoutException) return true;
        String msg = err.getMessage();
        return msg != null && msg.toLowerCase().contains("timeout");
    }

    private void finishCheck(String id, String status, long latencyMs, int attempts, int statusCode, String error) {
        JsonObject result = new JsonObject()
                .put("targetId", id)
                .put("timestamp", System.currentTimeMillis())
                .put("status", status)
                .put("latencyMs", Math.max(0, latencyMs))
                .put("attempts", attempts)
                .put("statusCode", statusCode)
                .put("errorMessage", error);

        vertx.eventBus().publish(EventBusAddresses.CHECK_RESULT, result);
        vertx.eventBus().publish(EventBusAddresses.CHECK_COMPLETED, new JsonObject().put("targetId", id));

        runningTargets.remove(id);
        currentInFlight--;
        drainQueue();
    }

    private void drainQueue() {
        while (currentInFlight < maxConcurrentChecks && !pendingQueue.isEmpty()) {
            JsonObject next = pendingQueue.poll();
            String id = next.getString("id");
            if (id != null && !runningTargets.contains(id)) {
                startCheck(next);
            }
        }
    }

    public int getCurrentInFlight() {
        return currentInFlight;
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        if (webClient != null) webClient.close();
        if (netClient != null) netClient.close();
        stopPromise.complete();
    }
}
