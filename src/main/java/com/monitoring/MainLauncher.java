package com.monitoring;

import com.monitoring.verticles.CheckManager;
import com.monitoring.verticles.HttpServer;
import com.monitoring.verticles.TargetManager;
import com.monitoring.verticles.TargetScheduler;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class MainLauncher {

    private static final Logger log = LoggerFactory.getLogger(MainLauncher.class);

    public static void main(String[] args) {
        VertxOptions options = new VertxOptions()
                .setMaxEventLoopExecuteTime(100)
                .setMaxEventLoopExecuteTimeUnit(TimeUnit.MILLISECONDS)
                .setBlockedThreadCheckInterval(500)
                .setBlockedThreadCheckIntervalUnit(TimeUnit.MILLISECONDS);

        Vertx vertx = Vertx.vertx(options);
        JsonObject config = loadConfig();

        int httpInstances = config.getJsonObject("deployment", new JsonObject()).getInteger("httpServerInstances", 2);
        int checkInstances = config.getJsonObject("deployment", new JsonObject()).getInteger("checkManagerInstances", 2);

        DeploymentOptions baseOpts = new DeploymentOptions().setConfig(config);
        DeploymentOptions checkOpts = new DeploymentOptions().setConfig(config).setInstances(checkInstances);
        DeploymentOptions httpOpts = new DeploymentOptions().setConfig(config).setInstances(httpInstances);

        // Start verticles in order
        vertx.deployVerticle(TargetManager::new, baseOpts)
                .compose(v -> vertx.deployVerticle(TargetScheduler::new, baseOpts))
                .compose(v -> vertx.deployVerticle(CheckManager::new, checkOpts))
                .compose(v -> vertx.deployVerticle(HttpServer::new, httpOpts))
                .onSuccess(id -> {
                    log.info("Monitoring service started on port 8080 (http instances: {}, check instances: {})",
                            httpInstances, checkInstances);
                    addShutdownHook(vertx);
                })
                .onFailure(err -> {
                    log.error("Failed to start application", err);
                    System.exit(1);
                });
    }

    private static JsonObject loadConfig() {
        try (InputStream in = MainLauncher.class.getClassLoader().getResourceAsStream("application.json")) {
            if (in == null) {
                return new JsonObject();
            }
            return new JsonObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.warn("Failed to load application.json, using defaults: {}", e.getMessage());
            return new JsonObject();
        }
    }

    private static void addShutdownHook(Vertx vertx) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping monitoring service...");
            vertx.close().onComplete(ar -> log.info("Service stopped cleanly."));
        }));
    }
}