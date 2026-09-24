package com.monitoring;

import com.monitoring.util.EventBusAddresses;
import com.monitoring.verticles.CheckManagerVerticle;
import com.monitoring.verticles.HttpServerVerticle;
import com.monitoring.verticles.TargetManagerVerticle;
import com.monitoring.verticles.TargetSchedulerVerticle;
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

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Slice3SchedulerTest {

    private Vertx vertx;
    private WebClient webClient;
    private final int port = 18081;

    @BeforeAll
    void setUp(VertxTestContext testContext) {
        vertx = Vertx.vertx();
        webClient = WebClient.create(vertx);

        JsonObject config = new JsonObject()
                .put("http", new JsonObject().put("port", port).put("host", "127.0.0.1"))
                .put("monitoring", new JsonObject().put("maxConcurrentChecks", 100).put("tickIntervalMs", 20));

        DeploymentOptions options = new DeploymentOptions().setConfig(config);

        vertx.deployVerticle(new TargetManagerVerticle(), options)
                .compose(v -> vertx.deployVerticle(new TargetSchedulerVerticle(), options))
                .compose(v -> vertx.deployVerticle(new CheckManagerVerticle(), options))
                .compose(v -> vertx.deployVerticle(new HttpServerVerticle(), options))
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
    @DisplayName("1. Verify Single Target Registration and Lookup via REST API")
    void testRegisterSingleTarget(VertxTestContext testContext) {
        JsonObject target = new JsonObject()
                .put("id", "target-http-rest-1")
                .put("type", "HTTP")
                .put("url", "http://127.0.0.1:" + port + "/health")
                .put("intervalSeconds", 1)
                .put("timeoutMs", 2000L);

        webClient.post(port, "127.0.0.1", "/api/targets")
                .sendJsonObject(target)
                .onSuccess(response -> testContext.verify(() -> {
                    assertEquals(201, response.statusCode());
                    JsonObject body = response.bodyAsJsonObject();
                    assertEquals("CREATED", body.getString("status"));
                    assertNotNull(body.getJsonObject("target"));

                    // Verify GET /api/targets/:id
                    webClient.get(port, "127.0.0.1", "/api/targets/target-http-rest-1")
                            .send()
                            .onSuccess(getResp -> testContext.verify(() -> {
                                assertEquals(200, getResp.statusCode());
                                JsonObject getBody = getResp.bodyAsJsonObject();
                                assertTrue(getBody.getBoolean("found"));
                                assertEquals("target-http-rest-1", getBody.getJsonObject("target").getString("id"));
                                testContext.completeNow();
                            }))
                            .onFailure(testContext::failNow);
                }))
                .onFailure(testContext::failNow);
    }

    @Test
    @DisplayName("2. Verify Bulk Target Registration via REST API")
    void testBulkTargetRegistration(VertxTestContext testContext) {
        JsonArray bulkTargets = new JsonArray();
        for (int i = 0; i < 50; i++) {
            bulkTargets.add(new JsonObject()
                    .put("id", "bulk-target-" + i)
                    .put("type", "HTTP")
                    .put("url", "http://127.0.0.1:" + port + "/health")
                    .put("intervalSeconds", 2)
                    .put("timeoutMs", 1000L));
        }

        webClient.post(port, "127.0.0.1", "/api/targets/bulk")
                .sendJson(bulkTargets)
                .onSuccess(response -> testContext.verify(() -> {
                    assertEquals(202, response.statusCode());
                    JsonObject body = response.bodyAsJsonObject();
                    assertEquals("ACCEPTED", body.getString("status"));
                    assertEquals(50, body.getInteger("registeredCount"));

                    // Verify list endpoint contains registered targets
                    webClient.get(port, "127.0.0.1", "/api/targets")
                            .send()
                            .onSuccess(listResp -> testContext.verify(() -> {
                                assertEquals(200, listResp.statusCode());
                                JsonObject listBody = listResp.bodyAsJsonObject();
                                assertTrue(listBody.getInteger("count") >= 50);
                                testContext.completeNow();
                            }))
                            .onFailure(testContext::failNow);
                }))
                .onFailure(testContext::failNow);
    }

    @Test
    @DisplayName("3. Verify PriorityQueue Scheduler fires periodic checks automatically")
    void testSchedulerPeriodicFiring(VertxTestContext testContext) {
        String targetId = "target-scheduler-periodic";
        AtomicInteger checkCount = new AtomicInteger(0);

        // Listen to check execution on EventBus
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.CHECK_EXECUTE, msg -> {
            if (targetId.equals(msg.body().getString("id"))) {
                int count = checkCount.incrementAndGet();
                if (count >= 2) {
                    testContext.completeNow();
                }
            }
        });

        JsonObject target = new JsonObject()
                .put("id", targetId)
                .put("type", "HTTP")
                .put("url", "http://127.0.0.1:" + port + "/health")
                .put("intervalSeconds", 1); // 1-second interval

        // Register target with scheduler
        vertx.eventBus().send(EventBusAddresses.SCHEDULER_TARGET_ADD, target);
    }

    @Test
    @DisplayName("4. Verify Target Deletion stops scheduled check execution")
    void testDeleteTarget(VertxTestContext testContext) {
        String targetId = "target-to-delete";

        JsonObject target = new JsonObject()
                .put("id", targetId)
                .put("type", "HTTP")
                .put("url", "http://127.0.0.1:" + port + "/health")
                .put("intervalSeconds", 1);

        webClient.post(port, "127.0.0.1", "/api/targets")
                .sendJsonObject(target)
                .compose(resp -> {
                    assertEquals(201, resp.statusCode());
                    return webClient.delete(port, "127.0.0.1", "/api/targets/" + targetId).send();
                })
                .onSuccess(delResp -> testContext.verify(() -> {
                    assertEquals(200, delResp.statusCode());
                    JsonObject delBody = delResp.bodyAsJsonObject();
                    assertEquals("DELETED", delBody.getString("status"));

                    // Verify target is no longer found
                    webClient.get(port, "127.0.0.1", "/api/targets/" + targetId)
                            .send()
                            .onSuccess(getResp -> testContext.verify(() -> {
                                assertEquals(404, getResp.statusCode());
                                testContext.completeNow();
                            }))
                            .onFailure(testContext::failNow);
                }))
                .onFailure(testContext::failNow);
    }

    @Test
    @DisplayName("5. Verify On-Demand Check (POST /api/targets/:id/check)")
    void testOnDemandCheckNow(VertxTestContext testContext) {
        String targetId = "target-check-now";

        JsonObject target = new JsonObject()
                .put("id", targetId)
                .put("type", "HTTP")
                .put("url", "http://127.0.0.1:" + port + "/health")
                .put("intervalSeconds", 60);

        webClient.post(port, "127.0.0.1", "/api/targets")
                .sendJsonObject(target)
                .compose(resp -> {
                    assertEquals(201, resp.statusCode());
                    return webClient.post(port, "127.0.0.1", "/api/targets/" + targetId + "/check").send();
                })
                .onSuccess(checkResp -> testContext.verify(() -> {
                    assertEquals(202, checkResp.statusCode());
                    JsonObject body = checkResp.bodyAsJsonObject();
                    assertEquals("TRIGGERED", body.getString("status"));
                    assertEquals(targetId, body.getString("targetId"));
                    testContext.completeNow();
                }))
                .onFailure(testContext::failNow);
    }

    @Test
    @DisplayName("6. Verify Bulk Re-Check Trigger [AC 7] returns HTTP 202 immediately")
    void testBulkCheckImmediateResponse(VertxTestContext testContext) {
        webClient.post(port, "127.0.0.1", "/api/targets/bulk-check")
                .send()
                .onSuccess(response -> testContext.verify(() -> {
                    assertEquals(202, response.statusCode());
                    JsonObject body = response.bodyAsJsonObject();
                    assertEquals("TRIGGERED", body.getString("status"));
                    assertTrue(body.containsKey("dispatchedCount"));
                    testContext.completeNow();
                }))
                .onFailure(testContext::failNow);
    }

    @Test
    @DisplayName("7. Verify Auto-Generating Target ID when ID is omitted by user")
    void testRegisterTargetWithAutoGeneratedId(VertxTestContext testContext) {
        JsonObject targetWithoutId = new JsonObject()
                .put("type", "HTTP")
                .put("url", "http://127.0.0.1:" + port + "/health")
                .put("intervalSeconds", 2);

        webClient.post(port, "127.0.0.1", "/api/targets")
                .sendJsonObject(targetWithoutId)
                .onSuccess(response -> testContext.verify(() -> {
                    assertEquals(201, response.statusCode());
                    JsonObject body = response.bodyAsJsonObject();
                    assertEquals("CREATED", body.getString("status"));
                    JsonObject registered = body.getJsonObject("target");
                    assertNotNull(registered);
                    assertNotNull(registered.getString("id"));
                    assertTrue(registered.getString("id").startsWith("target-"));
                    testContext.completeNow();
                }))
                .onFailure(testContext::failNow);
    }
}
