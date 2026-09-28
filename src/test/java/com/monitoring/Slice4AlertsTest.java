package com.monitoring;

import com.monitoring.stats.TargetHealthFSM;
import com.monitoring.util.EventBusAddresses;
import com.monitoring.verticles.CheckManager;
import com.monitoring.verticles.HttpServer;
import com.monitoring.verticles.StatsManager;
import com.monitoring.verticles.TargetManager;
import com.monitoring.verticles.TargetScheduler;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Slice4AlertsTest {

    private Vertx vertx;
    private WebClient webClient;
    private final int port = 18082;

    @BeforeAll
    void setUp(VertxTestContext testContext) {
        vertx = Vertx.vertx();
        webClient = WebClient.create(vertx);

        JsonObject config = new JsonObject()
                .put("http", new JsonObject().put("port", port).put("host", "127.0.0.1"))
                .put("monitoring", new JsonObject().put("maxConcurrentChecks", 100).put("tickIntervalMs", 20));

        DeploymentOptions options = new DeploymentOptions().setConfig(config);

        vertx.deployVerticle(new TargetManager(), options)
                .compose(v -> vertx.deployVerticle(new TargetScheduler(), options))
                .compose(v -> vertx.deployVerticle(new StatsManager(), options))
                .compose(v -> vertx.deployVerticle(new CheckManager(), options))
                .compose(v -> vertx.deployVerticle(new HttpServer(), options))
                .onComplete(testContext.succeedingThenComplete());
    }

    @AfterAll
    void tearDown(VertxTestContext testContext) {
        if (webClient != null) {
            webClient.close();
        }
        if (vertx != null) {
            vertx.close().onComplete(testContext.succeedingThenComplete());
        } else {
            testContext.completeNow();
        }
    }

    @Test
    @DisplayName("1. FSM Anti-Flapping: 1 and 2 failures stay HEALTHY, 3rd triggers CRITICAL")
    void testAntiFlappingFailureThreshold() {
        TargetHealthFSM fsm = new TargetHealthFSM();

        // 1st failure: remains HEALTHY
        TargetHealthFSM.Transition t1 = fsm.update("FAILURE", 0, 0.0);
        assertFalse(t1.changed());
        assertEquals(TargetHealthFSM.State.HEALTHY, fsm.getState());

        // 2nd failure: remains HEALTHY
        TargetHealthFSM.Transition t2 = fsm.update("FAILURE", 0, 0.0);
        assertFalse(t2.changed());
        assertEquals(TargetHealthFSM.State.HEALTHY, fsm.getState());

        // 3rd failure: transitions to CRITICAL
        TargetHealthFSM.Transition t3 = fsm.update("FAILURE", 0, 0.0);
        assertTrue(t3.changed());
        assertEquals(TargetHealthFSM.State.HEALTHY, t3.from());
        assertEquals(TargetHealthFSM.State.CRITICAL, t3.to());
        assertEquals(TargetHealthFSM.State.CRITICAL, fsm.getState());
    }

    @Test
    @DisplayName("2. FSM Recovery: 1 success stays CRITICAL, 2nd success recovers to HEALTHY")
    void testAntiFlappingRecoveryThreshold() {
        TargetHealthFSM fsm = new TargetHealthFSM();

        // Push to CRITICAL
        fsm.update("FAILURE", 0, 0.0);
        fsm.update("FAILURE", 0, 0.0);
        fsm.update("FAILURE", 0, 0.0);
        assertEquals(TargetHealthFSM.State.CRITICAL, fsm.getState());

        // 1st success: remains CRITICAL
        TargetHealthFSM.Transition t1 = fsm.update("SUCCESS", 20, 20.0);
        assertFalse(t1.changed());
        assertEquals(TargetHealthFSM.State.CRITICAL, fsm.getState());

        // 2nd success: recovers to HEALTHY
        TargetHealthFSM.Transition t2 = fsm.update("SUCCESS", 25, 22.5);
        assertTrue(t2.changed());
        assertEquals(TargetHealthFSM.State.CRITICAL, t2.from());
        assertEquals(TargetHealthFSM.State.HEALTHY, t2.to());
        assertEquals(TargetHealthFSM.State.HEALTHY, fsm.getState());
    }

    @Test
    @DisplayName("3. FSM Degraded State: latency >= 1500ms transitions to DEGRADED")
    void testDegradedLatencyThreshold() {
        TargetHealthFSM fsm = new TargetHealthFSM();

        // Normal success: HEALTHY
        fsm.update("SUCCESS", 50, 50.0);
        assertEquals(TargetHealthFSM.State.HEALTHY, fsm.getState());

        // High latency success: DEGRADED
        TargetHealthFSM.Transition t1 = fsm.update("SUCCESS", 1600, 1600.0);
        assertTrue(t1.changed());
        assertEquals(TargetHealthFSM.State.DEGRADED, t1.to());
        assertEquals(TargetHealthFSM.State.DEGRADED, fsm.getState());

        // Normal latency success: returns to HEALTHY
        TargetHealthFSM.Transition t2 = fsm.update("SUCCESS", 50, 50.0);
        assertTrue(t2.changed());
        assertEquals(TargetHealthFSM.State.HEALTHY, t2.to());
        assertEquals(TargetHealthFSM.State.HEALTHY, fsm.getState());
    }

    @Test
    @DisplayName("4. Verify StatsManager processes check.result, emits alerts, and serves /api/alerts")
    void testStatsManagerAlertGenerationAndQuery(VertxTestContext testContext) {
        String targetId = "target-alert-test-1";

        // Register target first
        JsonObject target = new JsonObject()
                .put("id", targetId)
                .put("type", "HTTP")
                .put("url", "http://127.0.0.1:" + port + "/non-existent")
                .put("intervalSeconds", 60);

        webClient.post(port, "127.0.0.1", "/api/targets")
                .sendJsonObject(target)
                .compose(resp -> {
                    assertEquals(201, resp.statusCode());

                    // Publish 3 failures on EventBus
                    for (int i = 0; i < 3; i++) {
                        JsonObject failure = new JsonObject()
                                .put("targetId", targetId)
                                .put("timestamp", System.currentTimeMillis())
                                .put("status", "FAILURE")
                                .put("latencyMs", 0L)
                                .put("statusCode", 500);
                        vertx.eventBus().publish(EventBusAddresses.CHECK_RESULT, failure);
                    }

                    // Query /api/alerts
                    return webClient.get(port, "127.0.0.1", "/api/alerts").send();
                })
                .onSuccess(alertResp -> testContext.verify(() -> {
                    assertEquals(200, alertResp.statusCode());
                    JsonObject alertBody = alertResp.bodyAsJsonObject();
                    assertNotNull(alertBody);
                    assertTrue(alertBody.getInteger("count") >= 1);

                    JsonArray alerts = alertBody.getJsonArray("alerts");
                    boolean found = false;
                    for (int i = 0; i < alerts.size(); i++) {
                        if (targetId.equals(alerts.getJsonObject(i).getString("targetId"))) {
                            assertEquals("CRITICAL", alerts.getJsonObject(i).getString("toState"));
                            found = true;
                            break;
                        }
                    }
                    assertTrue(found, "Alert for " + targetId + " must be present in active alerts list");

                    // Verify GET /api/targets returns the target with state=CRITICAL
                    webClient.get(port, "127.0.0.1", "/api/targets/" + targetId)
                            .send()
                            .onSuccess(getResp -> testContext.verify(() -> {
                                assertEquals(200, getResp.statusCode());
                                JsonObject getBody = getResp.bodyAsJsonObject();
                                JsonObject registered = getBody.getJsonObject("target");
                                assertEquals("CRITICAL", registered.getString("state"));
                                testContext.completeNow();
                            }))
                            .onFailure(testContext::failNow);
                }))
                .onFailure(testContext::failNow);
    }
}
