package com.monitoring;

import com.monitoring.util.EventBusAddresses;
import com.monitoring.verticles.CheckManager;
import com.monitoring.verticles.HttpServer;
import com.monitoring.verticles.PersistenceWorker;
import com.monitoring.verticles.StatsManager;
import com.monitoring.verticles.TargetManager;
import com.monitoring.verticles.TargetScheduler;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Slice5PersistenceTest {

    private Vertx vertx;
    private WebClient webClient;
    private final int port = 18083;
    private final String testTargetsPath = "target/test-data/targets.json";
    private final String testAuditPath = "target/test-data/audit.jsonl";

    @BeforeAll
    void setUp(VertxTestContext testContext) {
        // Clean up any existing test files
        new File(testTargetsPath).delete();
        new File(testAuditPath).delete();

        vertx = Vertx.vertx();
        webClient = WebClient.create(vertx);

        JsonObject config = new JsonObject()
                .put("http", new JsonObject().put("port", port).put("host", "127.0.0.1"))
                .put("monitoring", new JsonObject().put("maxConcurrentChecks", 100).put("tickIntervalMs", 20))
                .put("persistence", new JsonObject()
                        .put("targetsFilePath", testTargetsPath)
                        .put("auditLogFilePath", testAuditPath)
                        .put("flushIntervalMs", 200L)
                        .put("bufferCapacity", 5));

        DeploymentOptions workerOpts = new DeploymentOptions().setConfig(config).setThreadingModel(ThreadingModel.WORKER);
        DeploymentOptions baseOpts = new DeploymentOptions().setConfig(config);

        vertx.deployVerticle(new PersistenceWorker(), workerOpts)
                .compose(v -> vertx.deployVerticle(new TargetManager(), baseOpts))
                .compose(v -> vertx.deployVerticle(new TargetScheduler(), baseOpts))
                .compose(v -> vertx.deployVerticle(new StatsManager(), baseOpts))
                .compose(v -> vertx.deployVerticle(new CheckManager(), baseOpts))
                .compose(v -> vertx.deployVerticle(new HttpServer(), baseOpts))
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
    @DisplayName("1. Verify target registration persists to disk (targets.json)")
    void testTargetPersistenceSave(VertxTestContext testContext) {
        String targetId = "target-persist-1";
        JsonObject target = new JsonObject()
                .put("id", targetId)
                .put("type", "HTTP")
                .put("url", "http://127.0.0.1:" + port + "/health")
                .put("intervalSeconds", 10);

        webClient.post(port, "127.0.0.1", "/api/targets")
                .sendJsonObject(target)
                .onSuccess(resp -> {
                    assertEquals(201, resp.statusCode());

                    // Allow brief moment for disk write
                    vertx.setTimer(200, timerId -> testContext.verify(() -> {
                        File file = new File(testTargetsPath);
                        assertTrue(file.exists(), "targets.json must exist on disk");

                        String content = Files.readString(file.toPath(), StandardCharsets.UTF_8);
                        assertTrue(content.contains(targetId), "targets.json must contain " + targetId);
                        testContext.completeNow();
                    }));
                })
                .onFailure(testContext::failNow);
    }

    @Test
    @DisplayName("2. Verify target deletion removes target from disk (targets.json)")
    void testTargetPersistenceDelete(VertxTestContext testContext) {
        String targetId = "target-to-delete-disk";
        JsonObject target = new JsonObject()
                .put("id", targetId)
                .put("type", "HTTP")
                .put("url", "http://127.0.0.1:" + port + "/health")
                .put("intervalSeconds", 10);

        webClient.post(port, "127.0.0.1", "/api/targets")
                .sendJsonObject(target)
                .compose(resp -> {
                    assertEquals(201, resp.statusCode());
                    return webClient.delete(port, "127.0.0.1", "/api/targets/" + targetId).send();
                })
                .onSuccess(delResp -> {
                    assertEquals(200, delResp.statusCode());

                    vertx.setTimer(200, timerId -> testContext.verify(() -> {
                        File file = new File(testTargetsPath);
                        assertTrue(file.exists());
                        String content = Files.readString(file.toPath(), StandardCharsets.UTF_8);
                        assertFalse(content.contains(targetId), "targets.json must NOT contain deleted " + targetId);
                        testContext.completeNow();
                    }));
                })
                .onFailure(testContext::failNow);
    }

    @Test
    @DisplayName("3. Verify audit log is flushed to disk as JSONL format")
    void testAuditLogFlushing(VertxTestContext testContext) {
        JsonObject auditEvent = new JsonObject()
                .put("action", "ADMIN_ACTION")
                .put("adminUser", "admin")
                .put("timestamp", System.currentTimeMillis())
                .put("description", "Manual bulk check triggered");

        vertx.eventBus().send(EventBusAddresses.AUDIT_LOG, auditEvent);

        // Wait for buffer flush timer (200ms configured in test)
        vertx.setTimer(400, timerId -> testContext.verify(() -> {
            File file = new File(testAuditPath);
            assertTrue(file.exists(), "audit.jsonl must exist on disk");

            String content = Files.readString(file.toPath(), StandardCharsets.UTF_8);
            assertTrue(content.contains("ADMIN_ACTION"), "audit.jsonl must contain the audit action");
            assertTrue(content.contains("adminUser"), "audit.jsonl must contain audit metadata");
            testContext.completeNow();
        }));
    }

    @Test
    @DisplayName("4. Verify crash recovery: targets saved on disk are restored on startup")
    void testStartupRecovery(VertxTestContext testContext) {
        String targetId = "target-recovery-test";
        JsonObject target = new JsonObject()
                .put("id", targetId)
                .put("type", "HTTP")
                .put("url", "http://127.0.0.1:" + port + "/health")
                .put("intervalSeconds", 5);

        // Save target to persistence
        vertx.eventBus().send(EventBusAddresses.PERSIST_TARGET_SAVE, target);

        // Query PersistenceWorker to load targets
        vertx.setTimer(150, timerId -> {
            vertx.eventBus().<JsonObject>request(EventBusAddresses.PERSIST_TARGET_LOAD, new JsonObject())
                    .onSuccess(reply -> testContext.verify(() -> {
                        JsonObject body = reply.body();
                        assertNotNull(body);
                        assertTrue(body.getInteger("count") >= 1, "Must restore at least 1 saved target");
                        testContext.completeNow();
                    }))
                    .onFailure(testContext::failNow);
        });
    }
}
