package com.monitoring;

import com.monitoring.util.EventBusAddresses;
import com.monitoring.verticles.CheckManagerVerticle;
import com.monitoring.verticles.HttpServerVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetServer;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Slice1FoundationTest {

    private Vertx vertx;
    private WebClient webClient;
    private final int port = 18080;
    private final int testTcpPort = 19001;
    private NetServer tcpEchoServer;

    private final Map<String, Consumer<JsonObject>> resultListeners = new ConcurrentHashMap<>();

    @BeforeAll
    void setUp(VertxTestContext testContext) {
        vertx = Vertx.vertx();
        webClient = WebClient.create(vertx);

        JsonObject config = new JsonObject()
                .put("http", new JsonObject().put("port", port).put("host", "127.0.0.1"))
                .put("monitoring", new JsonObject().put("maxConcurrentChecks", 100));

        DeploymentOptions options = new DeploymentOptions().setConfig(config);

        // Single EventBus consumer that routes results to the registered listener for that targetId
        vertx.eventBus().<JsonObject>consumer(EventBusAddresses.CHECK_RESULT, msg -> {
            JsonObject result = msg.body();
            String targetId = result.getString("targetId");
            Consumer<JsonObject> listener = resultListeners.remove(targetId);
            if (listener != null) {
                listener.accept(result);
            }
        });

        // Start test TCP server
        vertx.createNetServer()
                .connectHandler(socket -> socket.close())
                .listen(testTcpPort, "127.0.0.1")
                .compose(server -> {
                    this.tcpEchoServer = server;
                    return vertx.deployVerticle(new CheckManagerVerticle(), options);
                })
                .compose(v -> vertx.deployVerticle(new HttpServerVerticle(), options))
                .compose(v -> webClient.get(port, "127.0.0.1", "/health").send()) // Warmup JVM
                .onComplete(testContext.succeedingThenComplete());
    }

    @AfterAll
    void tearDown(VertxTestContext testContext) {
        if (tcpEchoServer != null) {
            tcpEchoServer.close();
        }
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
    @DisplayName("1. Verify /health endpoint is ultra-fast (< 50ms)")
    void testHealthEndpointAnswersFast(VertxTestContext testContext) {
        long startTime = System.currentTimeMillis();

        webClient.get(port, "127.0.0.1", "/health")
                .send()
                .onSuccess(response -> testContext.verify(() -> {
                    long latency = System.currentTimeMillis() - startTime;
                    assertEquals(200, response.statusCode());
                    JsonObject body = response.bodyAsJsonObject();
                    assertNotNull(body);
                    assertEquals("UP", body.getString("status"));
                    assertTrue(latency < 50, "Health check must answer in under 50ms, was " + latency + "ms");
                    testContext.completeNow();
                }))
                .onFailure(testContext::failNow);
    }

    @Test
    @DisplayName("2. Verify non-blocking HTTP probe success")
    void testHttpProbeSuccess(VertxTestContext testContext) {
        String targetId = "target-http-success";

        resultListeners.put(targetId, result -> testContext.verify(() -> {
            assertEquals("SUCCESS", result.getString("status"));
            assertEquals(200, result.getInteger("statusCode"));
            assertEquals(1, result.getInteger("attempts"));
            assertTrue(result.getLong("latencyMs") >= 0);
            testContext.completeNow();
        }));

        JsonObject targetConfig = new JsonObject()
                .put("id", targetId)
                .put("type", "HTTP")
                .put("url", "http://127.0.0.1:" + port + "/health")
                .put("timeoutMs", 2000L);

        vertx.eventBus().send(EventBusAddresses.CHECK_EXECUTE, targetConfig);
    }

    @Test
    @DisplayName("3. Verify non-blocking TCP raw socket probe success")
    void testTcpProbeSuccess(VertxTestContext testContext) {
        String targetId = "target-tcp-success";

        resultListeners.put(targetId, result -> testContext.verify(() -> {
            assertEquals("SUCCESS", result.getString("status"));
            assertEquals(1, result.getInteger("attempts"));
            assertTrue(result.getLong("latencyMs") >= 0);
            testContext.completeNow();
        }));

        JsonObject targetConfig = new JsonObject()
                .put("id", targetId)
                .put("type", "TCP")
                .put("ip", "127.0.0.1")
                .put("port", testTcpPort)
                .put("timeoutMs", 2000L);

        vertx.eventBus().send(EventBusAddresses.CHECK_EXECUTE, targetConfig);
    }

    @Test
    @DisplayName("4. Verify TCP connection failure with retries")
    void testTcpConnectionFailureWithRetries(VertxTestContext testContext) {
        String targetId = "target-tcp-failure";

        resultListeners.put(targetId, result -> testContext.verify(() -> {
            assertEquals("FAILURE", result.getString("status"));
            assertEquals(2, result.getInteger("attempts"), "Must execute configured 2 retry attempts");
            assertNotNull(result.getString("errorMessage"));
            testContext.completeNow();
        }));

        // Point to an unused closed port (59998)
        JsonObject targetConfig = new JsonObject()
                .put("id", targetId)
                .put("type", "TCP")
                .put("ip", "127.0.0.1")
                .put("port", 59998)
                .put("timeoutMs", 1000L)
                .put("retryPolicy", new JsonObject()
                        .put("maxAttempts", 2)
                        .put("baseDelayMs", 50L)
                        .put("backoffFactor", 2.0));

        vertx.eventBus().send(EventBusAddresses.CHECK_EXECUTE, targetConfig);
    }

    @Test
    @DisplayName("5. Verify HTTP 404 failure with retries")
    void testHttp404FailureWithRetries(VertxTestContext testContext) {
        String targetId = "target-http-404";

        resultListeners.put(targetId, result -> testContext.verify(() -> {
            assertEquals("FAILURE", result.getString("status"));
            assertEquals(3, result.getInteger("attempts"), "Must execute configured 3 retry attempts");
            assertTrue(result.getString("errorMessage").contains("404"));
            testContext.completeNow();
        }));

        JsonObject targetConfig = new JsonObject()
                .put("id", targetId)
                .put("type", "HTTP")
                .put("url", "http://127.0.0.1:" + port + "/non-existent-path-for-404")
                .put("timeoutMs", 1000L)
                .put("retryPolicy", new JsonObject()
                        .put("maxAttempts", 3)
                        .put("baseDelayMs", 50L)
                        .put("backoffFactor", 2.0));

        vertx.eventBus().send(EventBusAddresses.CHECK_EXECUTE, targetConfig);
    }
}
