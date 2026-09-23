package com.monitoring.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MainVerticle: Coordinates ordered deployment of Slice 1 verticles.
 */
public class MainVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(MainVerticle.class);

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject appConfig = config();
        DeploymentOptions standardOptions = new DeploymentOptions().setConfig(appConfig);

        deployCheckManager(standardOptions)
                .compose(v -> deployHttpServer(standardOptions))
                .onSuccess(v -> {
                    logger.info("MainVerticle deployed Slice 1 verticles successfully");
                    startPromise.complete();
                })
                .onFailure(err -> {
                    logger.error("Failed to deploy verticles", err);
                    startPromise.fail(err);
                });
    }

    private Future<String> deployCheckManager(DeploymentOptions options) {
        return vertx.deployVerticle(CheckManagerVerticle.class.getName(), options);
    }

    private Future<String> deployHttpServer(DeploymentOptions options) {
        return vertx.deployVerticle(HttpServerVerticle.class.getName(), options);
    }
}
