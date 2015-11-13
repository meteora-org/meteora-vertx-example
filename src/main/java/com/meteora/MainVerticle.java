package com.meteora;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

public class MainVerticle extends AbstractVerticle {

    @Override
    public void init(Vertx vertx, Context context) {
        vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(100));
        super.init(vertx, context);
    }

    @Override
    public void start() {
        vertx.deployVerticle(ServerVerticle.class.getName(),new DeploymentOptions().setInstances(16).setWorker(true));

    }

}
