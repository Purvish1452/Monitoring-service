package com.monitoring.verticles;

import com.monitoring.util.EventBusAddresses;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles HTTP REST endpoints and serves the static frontend dashboard.
 */
public class HttpServer extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(HttpServer.class);

    private io.vertx.core.http.HttpServer server;

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject httpConfig = config().getJsonObject("http", new JsonObject());
        int port = httpConfig.getInteger("port", 8080);
        String host = httpConfig.getString("host", "0.0.0.0");

        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        // Health & Metrics
        router.get("/health").handler(this::handleHealth);
        router.get("/api/health").handler(this::handleHealth);
        router.get("/api/metrics").handler(this::handleGetMetrics);
        router.get("/api/alerts").handler(this::handleListAlerts);

        // Target Management
        router.post("/api/targets").handler(this::handleRegisterTarget);
        router.post("/api/targets/bulk").handler(this::handleRegisterBulkTargets);
        router.get("/api/targets").handler(this::handleListTargets);
        router.get("/api/targets/:id").handler(this::handleGetTarget);
        router.delete("/api/targets/:id").handler(this::handleDeleteTarget);

        // Manual & Bulk Checks
        router.post("/api/targets/bulk-check").handler(this::handleBulkCheck);
        router.post("/api/targets/:id/check").handler(this::handleCheckNow);

        // Static Dashboard Frontend (webroot/)
        router.route("/dashboard").handler(ctx -> ctx.reroute("/index.html"));
        router.route("/*").handler(StaticHandler.create("webroot").setIndexPage("index.html"));

        HttpServerOptions options = new HttpServerOptions()
                .setReuseAddress(true)
                .setReusePort(true);

        vertx.createHttpServer(options)
                .requestHandler(router)
                .listen(port, host)
                .onSuccess(httpServer -> {
                    this.server = httpServer;
                    log.info("HTTP Server listening on {}:{}", host, port);
                    startPromise.complete();
                })
                .onFailure(err -> {
                    log.error("Failed to start HTTP Server on port {}", port, err);
                    startPromise.fail(err);
                });
    }

    private void handleHealth(RoutingContext ctx) {
        sendJson(ctx, 200, new JsonObject().put("status", "UP"));
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

        sendJson(ctx, 200, metrics);
    }

    private void handleRegisterTarget(RoutingContext ctx) {
        JsonObject body = ctx.body().asJsonObject();
        if (body == null || (!body.containsKey("url") && !body.containsKey("port"))) {
            sendError(ctx, 400, "Must include either 'url' (HTTP) or 'port' (TCP)");
            return;
        }

        vertx.eventBus().<JsonObject>request(EventBusAddresses.TARGET_REGISTER, body)
                .onSuccess(reply -> sendJson(ctx, 201, reply.body()))
                .onFailure(err -> sendError(ctx, 500, err.getMessage()));
    }

    private void handleRegisterBulkTargets(RoutingContext ctx) {
        JsonArray body = ctx.body().asJsonArray();
        if (body == null || body.isEmpty()) {
            sendError(ctx, 400, "Payload must be a non-empty array of targets");
            return;
        }

        vertx.eventBus().<JsonObject>request(EventBusAddresses.TARGET_REGISTER_BULK, body)
                .onSuccess(reply -> sendJson(ctx, 202, reply.body()))
                .onFailure(err -> sendError(ctx, 500, err.getMessage()));
    }

    private void handleListTargets(RoutingContext ctx) {
        vertx.eventBus().<JsonObject>request(EventBusAddresses.TARGET_LIST, new JsonObject())
                .onSuccess(reply -> sendJson(ctx, 200, reply.body()))
                .onFailure(err -> sendError(ctx, 500, err.getMessage()));
    }

    private void handleGetTarget(RoutingContext ctx) {
        String targetId = ctx.pathParam("id");
        vertx.eventBus().<JsonObject>request(EventBusAddresses.TARGET_GET, new JsonObject().put("targetId", targetId))
                .onSuccess(reply -> {
                    JsonObject result = reply.body();
                    if (result.getBoolean("found", false)) {
                        sendJson(ctx, 200, result);
                    } else {
                        sendError(ctx, 404, "Target not found");
                    }
                })
                .onFailure(err -> sendError(ctx, 500, err.getMessage()));
    }

    private void handleDeleteTarget(RoutingContext ctx) {
        String targetId = ctx.pathParam("id");
        vertx.eventBus().<JsonObject>request(EventBusAddresses.TARGET_DELETE, new JsonObject().put("targetId", targetId))
                .onSuccess(reply -> sendJson(ctx, 200, reply.body()))
                .onFailure(err -> sendError(ctx, 500, err.getMessage()));
    }

    private void handleBulkCheck(RoutingContext ctx) {
        vertx.eventBus().<JsonObject>request(EventBusAddresses.TARGET_BULK_CHECK, new JsonObject())
                .onSuccess(reply -> sendJson(ctx, 202, reply.body()))
                .onFailure(err -> sendError(ctx, 500, err.getMessage()));
    }

    private void handleCheckNow(RoutingContext ctx) {
        String targetId = ctx.pathParam("id");
        vertx.eventBus().<JsonObject>request(EventBusAddresses.TARGET_GET, new JsonObject().put("targetId", targetId))
                .onSuccess(reply -> {
                    JsonObject result = reply.body();
                    if (result.getBoolean("found", false)) {
                        vertx.eventBus().send(EventBusAddresses.CHECK_EXECUTE, result.getJsonObject("target"));
                        sendJson(ctx, 202, new JsonObject().put("status", "TRIGGERED").put("targetId", targetId));
                    } else {
                        sendError(ctx, 404, "Target not found");
                    }
                })
                .onFailure(err -> sendError(ctx, 500, err.getMessage()));
    }

    private void handleListAlerts(RoutingContext ctx) {
        vertx.eventBus().<JsonObject>request(EventBusAddresses.ALERTS_LIST, new JsonObject())
                .onSuccess(reply -> sendJson(ctx, 200, reply.body()))
                .onFailure(err -> sendError(ctx, 500, err.getMessage()));
    }

    private void sendJson(RoutingContext ctx, int statusCode, JsonObject body) {
        ctx.response()
                .setStatusCode(statusCode)
                .putHeader("content-type", "application/json")
                .end(body.encode());
    }

    private void sendError(RoutingContext ctx, int statusCode, String message) {
        sendJson(ctx, statusCode, new JsonObject().put("error", message));
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
