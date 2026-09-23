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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HttpServerVerticle:
 * 1. Serves non-blocking REST API for single and bulk target registration.
 * 2. Provides isolated /health endpoint that answers immediately in-memory in < 2ms (satisfying AC 2).
 * 3. Serves live Web UI Dashboard for browser interaction.
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

        // Web UI Dashboard
        router.get("/").handler(this::handleDashboard);
        router.get("/dashboard").handler(this::handleDashboard);

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

    private void handleDashboard(RoutingContext ctx) {
        String html = """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <title>Network Endpoint Monitoring Dashboard</title>
                    <style>
                        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 24px; background: #0f172a; color: #f8fafc; }
                        .container { max-width: 1200px; margin: 0 auto; }
                        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; padding-bottom: 16px; margin-bottom: 24px; }
                        .badge { padding: 4px 10px; border-radius: 6px; font-size: 12px; font-weight: 600; text-transform: uppercase; }
                        .badge-up { background: #059669; color: white; }
                        .badge-down { background: #dc2626; color: white; }
                        .badge-degraded { background: #d97706; color: white; }
                        .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 16px; margin-bottom: 24px; }
                        .card { background: #1e293b; border-radius: 8px; padding: 20px; border: 1px solid #334155; }
                        .card-title { font-size: 13px; color: #94a3b8; text-transform: uppercase; margin-bottom: 8px; font-weight: 600; }
                        .card-value { font-size: 28px; font-weight: 700; color: #38bdf8; }
                        table { width: 100%; border-collapse: collapse; background: #1e293b; border-radius: 8px; overflow: hidden; }
                        th, td { padding: 12px 16px; text-align: left; border-bottom: 1px solid #334155; font-size: 14px; }
                        th { background: #334155; color: #cbd5e1; font-weight: 600; }
                        button { background: #0284c7; color: white; border: none; padding: 8px 16px; border-radius: 6px; font-weight: 600; cursor: pointer; }
                        button:hover { background: #0369a1; }
                    </style>
                </head>
                <body>
                <div class="container">
                    <div class="header">
                        <h2>⚡ High-Throughput Network Monitoring Service</h2>
                        <span class="badge badge-up">System Online (Java 21 & Vert.x 5)</span>
                    </div>
                    <div class="cards">
                        <div class="card"><div class="card-title">Health Status</div><div class="card-value" id="valHealth">UP</div></div>
                        <div class="card"><div class="card-title">Registered Targets</div><div class="card-value" id="valTargets">0</div></div>
                        <div class="card"><div class="card-title">Active Alerts</div><div class="card-value" id="valAlerts" style="color:#ef4444">0</div></div>
                        <div class="card"><div class="card-title">Heap Memory</div><div class="card-value" id="valHeap">0 MB</div></div>
                    </div>
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
                        <h3>Monitored Targets</h3>
                        <button onclick="triggerBulkCheck()">⚡ Re-Check All Now</button>
                    </div>
                    <table>
                        <thead>
                            <tr><th>Target ID</th><th>Type</th><th>Endpoint</th><th>1m Avg (ms)</th><th>5m Avg (ms)</th><th>Status</th></tr>
                        </thead>
                        <tbody id="targetTableBody">
                            <tr><td colspan="6" style="text-align:center; color:#64748b;">No targets registered yet. Use POST /api/targets or upload bulk targets.</td></tr>
                        </tbody>
                    </table>
                </div>
                <script>
                    function fetchMetrics() {
                        fetch('/api/metrics').then(r=>r.json()).then(d=>{
                            document.getElementById('valHeap').innerText = d.heapUsedMb + ' / ' + d.heapMaxMb + ' MB';
                        }).catch(()=>{});
                        fetch('/api/targets').then(r=>r.json()).then(d=>{
                            let targets = d.targets || [];
                            document.getElementById('valTargets').innerText = targets.length;
                            let tbody = document.getElementById('targetTableBody');
                            if (targets.length === 0) return;
                            tbody.innerHTML = targets.map(t => `
                                <tr>
                                    <td><b>${t.id}</b></td>
                                    <td>${t.type || 'HTTP'}</td>
                                    <td>${t.url || (t.ip + ':' + t.port)}</td>
                                    <td>${t.avg1m ? t.avg1m.toFixed(2) : '-'}</td>
                                    <td>${t.avg5m ? t.avg5m.toFixed(2) : '-'}</td>
                                    <td><span class="badge badge-${(t.state||'HEALTHY').toLowerCase()}">${t.state || 'HEALTHY'}</span></td>
                                </tr>
                            `).join('');
                        }).catch(()=>{});
                        fetch('/api/alerts').then(r=>r.json()).then(d=>{
                            let alerts = d.alerts || [];
                            document.getElementById('valAlerts').innerText = alerts.length;
                        }).catch(()=>{});
                    }
                    function triggerBulkCheck() {
                        fetch('/api/targets/bulk-check', {method: 'POST'}).then(()=>alert('Bulk re-check triggered!'));
                    }
                    setInterval(fetchMetrics, 1000);
                    fetchMetrics();
                </script>
                </body>
                </html>
                """;
        ctx.response().putHeader("content-type", "text/html; charset=utf-8").end(html);
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
