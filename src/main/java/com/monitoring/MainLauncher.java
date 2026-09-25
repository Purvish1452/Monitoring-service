package com.monitoring;

import com.monitoring.verticles.CheckManagerVerticle;
import com.monitoring.verticles.HttpServerVerticle;
import com.monitoring.verticles.TargetManagerVerticle;
import com.monitoring.verticles.TargetSchedulerVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main application launcher & Composition Root.
 * Configures Vert.x Event Loop Watchdog:
 * maxEventLoopExecuteTime = 100ms, blockedThreadCheckInterval = 500ms.
 * Directly deploys independent Verticles in coordinated sequence with multi-instance scaling.
 * Registers stateful SIGTERM shutdown hook.
 */
public class MainLauncher {

    private static final Logger logger = LoggerFactory.getLogger(MainLauncher.class);

    private static final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public static void main(String[] args) {
        // Enforce strict Event Loop Watchdog rules
        VertxOptions options = new VertxOptions()
                .setMaxEventLoopExecuteTime(100)
                .setMaxEventLoopExecuteTimeUnit(TimeUnit.MILLISECONDS)
                .setBlockedThreadCheckInterval(500)
                .setBlockedThreadCheckIntervalUnit(TimeUnit.MILLISECONDS)
                .setWorkerPoolSize(4);

        Vertx vertx = Vertx.vertx(options);

        JsonObject config = loadConfiguration();
        JsonObject deployConfig = config.getJsonObject("deployment", new JsonObject());

        int httpInstances = deployConfig.getInteger("httpServerInstances", 2);
        int checkInstances = deployConfig.getInteger("checkManagerInstances", 2);

        DeploymentOptions singletonOptions = new DeploymentOptions().setConfig(config).setInstances(1);
        DeploymentOptions httpOptions = new DeploymentOptions().setConfig(config).setInstances(httpInstances);
        DeploymentOptions checkOptions = new DeploymentOptions().setConfig(config).setInstances(checkInstances);

        // Deploy independent peer verticles in safe initialization order with multi-instance scaling
        vertx.deployVerticle(TargetManagerVerticle::new, singletonOptions)
                .compose(v -> vertx.deployVerticle(TargetSchedulerVerticle::new, singletonOptions))
                .compose(v -> vertx.deployVerticle(CheckManagerVerticle::new, checkOptions))
                .compose(v -> vertx.deployVerticle(HttpServerVerticle::new, httpOptions))
                .onSuccess(deploymentId -> {
                    logger.info("All monitoring service verticles deployed successfully (HTTP instances: {}, CheckManager instances: {}).",
                            httpInstances, checkInstances);
                    registerShutdownHook(vertx);
                })
                .onFailure(err -> {
                    logger.error("Failed to bootstrap Monitoring Service", err);
                    System.exit(1);
                });
    }

    private static JsonObject loadConfiguration() {
        try (InputStream is = MainLauncher.class.getClassLoader().getResourceAsStream("application.json")) {
            if (is != null) {
                String jsonStr = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                return new JsonObject(jsonStr);
            }
        } catch (Exception e) {
            logger.warn("Could not load application.json from classpath, using defaults: {}", e.getMessage());
        }
        return new JsonObject();
    }

    private static void registerShutdownHook(Vertx vertx) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (isShuttingDown.compareAndSet(false, true)) {
                logger.info("SIGTERM/SIGINT received. Initiating graceful shutdown (max 10s)...");
                vertx.close().onComplete(ar -> {
                    if (ar.succeeded()) {
                        logger.info("Vert.x instance closed cleanly.");
                    } else {
                        logger.error("Error during Vert.x close", ar.cause());
                    }
                    shutdownLatch.countDown();
                });

                try {
                    boolean drained = shutdownLatch.await(10, TimeUnit.SECONDS);
                    if (drained) {
                        logger.info("Graceful shutdown completed successfully.");
                    } else {
                        logger.warn("Shutdown deadline exceeded (10s), forcing termination.");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "shutdown-hook-thread"));
    }
}
