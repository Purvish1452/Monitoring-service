package com.monitoring.verticles;

import com.monitoring.util.EventBusAddresses;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpServerVerticle:
 * 1. Serves non-blocking REST API for single and bulk target registration.
 * 2. Provides isolated /health endpoint that answers immediately in-memory in < 2ms (satisfying AC 2).
 * 3. Serves decoupled static Web UI (HTML, CSS, JS) via StaticHandler from webroot/.
 */
public class HttpServerVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(HttpServerVerticle.class);

    private HttpServer server;

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject httpConfig = config().getJsonObject("http", new JsonObject());
        int port = httpConfig.getInteger("port", 8080);
        String host = httpConfig.getString("host", "0.0.0.0");

        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        // AC 2: Isolated health endpoint - answers immediately from in-memory state in < 2ms
        router.get("/health").handler(this::handleHealth);
        router.get("/api/health").handler(this::handleHealth);

        // Target Management Endpoints
        router.post("/api/targets").handler(this::handleRegisterTarget);
        router.post("/api/targets/bulk").handler(this::handleRegisterBulkTargets);
        router.get("/api/targets").handler(this::handleListTargets);
        router.get("/api/targets/:id").handler(this::handleGetTarget);
        router.delete("/api/targets/:id").handler(this::handleDeleteTarget);

        // Bulk and Immediate Check Endpoints
        router.post("/api/targets/bulk-check").handler(this::handleBulkCheck);
        router.post("/api/targets/:id/check").handler(this::handleCheckNow);

        // Alert & Metrics Endpoints
        router.get("/api/alerts").handler(this::handleListAlerts);
        router.get("/api/metrics").handler(this::handleGetMetrics);

        // Decoupled Static Frontend Handler (webroot/)
        StaticHandler staticHandler = StaticHandler.create("webroot")
                .setIndexPage("index.html")
                .setCachingEnabled(true);

        router.route("/dashboard").handler(ctx -> ctx.reroute("/index.html"));
        router.route("/*").handler(staticHandler);

        io.vertx.core.http.HttpServerOptions serverOptions = new io.vertx.core.http.HttpServerOptions()
                .setReuseAddress(true)
                .setReusePort(true);

        vertx.createHttpServer(serverOptions)
                .requestHandler(router)
                .listen(port, host)
                .onSuccess(httpServer -> {
                    this.server = httpServer;
                    logger.info("HttpServerVerticle listening on {}:{}", host, port);
                    startPromise.complete();
                })
                .onFailure(err -> {
                    logger.error("Failed to start HTTP server on port {}", port, err);
                    startPromise.fail(err);
                });
    }

    private void handleHealth(RoutingContext ctx) {
        // Immediate in-memory response: zero EventBus or database latency
        ctx.response()
                .putHeader("content-type", "application/json")
                .end("{\"status\":\"UP\"}");
    }

    private void handleRegisterTarget(RoutingContext ctx) {
        JsonObject body = ctx.body().asJsonObject();
        if (body == null || !body.containsKey("id") || (!body.containsKey("url") && !body.containsKey("port"))) {
            ctx.response().setStatusCode(400).end(new JsonObject().put("error", "Invalid target payload. Must include 'id' and either 'url' or 'port'").encode());
            return;
        }

        vertx.eventBus().<JsonObject>request(EventBusAddresses.TARGET_REGISTER, body)
                .onSuccess(reply -> ctx.response().setStatusCode(201).putHeader("content-type", "application/json").end(reply.body().encode()))
                .onFailure(err -> ctx.response().setStatusCode(500).end(new JsonObject().put("error", err.getMessage()).encode()));
    }

    private void handleRegisterBulkTargets(RoutingContext ctx) {
        JsonArray body = ctx.body().asJsonArray();
        if (body == null || body.isEmpty()) {
            ctx.response().setStatusCode(400).end(new JsonObject().put("error", "Payload must be a non-empty JsonArray of targets").encode());
            return;
        }

        vertx.eventBus().<JsonObject>request(EventBusAddresses.TARGET_REGISTER_BULK, body)
                .onSuccess(reply -> ctx.response().setStatusCode(202).putHeader("content-type", "application/json").end(reply.body().encode()))
                .onFailure(err -> ctx.response().setStatusCode(500).end(new JsonObject().put("error", err.getMessage()).encode()));
    }

    private void handleListTargets(RoutingContext ctx) {
        vertx.eventBus().<JsonObject>request(EventBusAddresses.TARGET_LIST, new JsonObject())
                .onSuccess(reply -> ctx.response().putHeader("content-type", "application/json").end(reply.body().encode()))
                .onFailure(err -> ctx.response().setStatusCode(500).end(new JsonObject().put("error", err.getMessage()).encode()));
    }

    private void handleGetTarget(RoutingContext ctx) {
        String targetId = ctx.pathParam("id");
        vertx.eventBus().<JsonObject>request(EventBusAddresses.TARGET_GET, new JsonObject().put("targetId", targetId))
                .onSuccess(reply -> {
                    JsonObject result = reply.body();
                    if (result.getBoolean("found", false)) {
                        ctx.response().putHeader("content-type", "application/json").end(result.encode());
                    } else {
                        ctx.response().setStatusCode(404).end(new JsonObject().put("error", "Target not found").encode());
                    }
                })
                .onFailure(err -> ctx.response().setStatusCode(500).end(new JsonObject().put("error", err.getMessage()).encode()));
    }

    private void handleDeleteTarget(RoutingContext ctx) {
        String targetId = ctx.pathParam("id");
        vertx.eventBus().<JsonObject>request(EventBusAddresses.TARGET_DELETE, new JsonObject().put("targetId", targetId))
                .onSuccess(reply -> ctx.response().putHeader("content-type", "application/json").end(reply.body().encode()))
                .onFailure(err -> ctx.response().setStatusCode(500).end(new JsonObject().put("error", err.getMessage()).encode()));
    }

    private void handleBulkCheck(RoutingContext ctx) {
        vertx.eventBus().<JsonObject>request(EventBusAddresses.TARGET_BULK_CHECK, new JsonObject())
                .onSuccess(reply -> ctx.response().setStatusCode(202).putHeader("content-type", "application/json").end(reply.body().encode()))
                .onFailure(err -> ctx.response().setStatusCode(500).end(new JsonObject().put("error", err.getMessage()).encode()));
    }

    private void handleCheckNow(RoutingContext ctx) {
        String targetId = ctx.pathParam("id");
        vertx.eventBus().<JsonObject>request(EventBusAddresses.TARGET_GET, new JsonObject().put("targetId", targetId))
                .onSuccess(reply -> {
                    JsonObject targetInfo = reply.body();
                    if (targetInfo.getBoolean("found", false)) {
                        JsonObject config = targetInfo.getJsonObject("target");
                        vertx.eventBus().send(EventBusAddresses.CHECK_EXECUTE, config);
                        ctx.response().setStatusCode(202).end(new JsonObject().put("status", "TRIGGERED").put("targetId", targetId).encode());
                    } else {
                        ctx.response().setStatusCode(404).end(new JsonObject().put("error", "Target not found").encode());
                    }
                })
                .onFailure(err -> ctx.response().setStatusCode(500).end(new JsonObject().put("error", err.getMessage()).encode()));
    }

    private void handleListAlerts(RoutingContext ctx) {
        vertx.eventBus().<JsonObject>request(EventBusAddresses.ALERTS_LIST, new JsonObject())
                .onSuccess(reply -> ctx.response().putHeader("content-type", "application/json").end(reply.body().encode()))
                .onFailure(err -> ctx.response().setStatusCode(500).end(new JsonObject().put("error", err.getMessage()).encode()));
    }

    private void handleGetMetrics(RoutingContext ctx) {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory();

        JsonObject metrics = new JsonObject()
                .put("status", "UP")
                .put("heapUsedMb", usedMemory / (1024 * 1024))
                .put("heapTotalMb", totalMemory / (1024 * 1024))
                .put("heapMaxMb", maxMemory / (1024 * 1024))
                .put("availableProcessors", runtime.availableProcessors());

        ctx.response().putHeader("content-type", "application/json").end(metrics.encode());
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        if (server != null) {
            server.close().onComplete(ar -> stopPromise.complete());
        } else {
            stopPromise.complete();
        }
    }
}
