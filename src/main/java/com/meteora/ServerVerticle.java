package com.meteora;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.apex.Router;
import io.vertx.ext.apex.RoutingContext;
import io.vertx.ext.apex.handler.BodyHandler;
import io.vertx.ext.jdbc.JdbcService;
import io.vertx.ext.sql.SqlConnection;

import java.util.List;

public class ServerVerticle extends AbstractVerticle {

    private final static String CONNECTION = "connection";

    @Override
    public void init(Vertx vertx, Context context) {
        vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(40));
        super.init(vertx, context);
    }

    @Override
    public void start() throws Exception {

        JsonObject config = new JsonObject().put("url", "jdbc:mysql://dev-dotmoney-dbm01.amb-stg-295.a4c.jp:3306/machida?user=machida-usr&password=machida-pwd");
        DeploymentOptions options = new DeploymentOptions().setConfig(config);


        vertx.deployVerticle("service:io.vertx.jdbc-service", options, res -> {
            if (res.succeeded()) {
                Router router = Router.router(vertx);

                router.route().handler(BodyHandler.create());
                router.route().handler(this::setConnection);
                router.route().handler(routingContext -> {
                    routingContext.response().putHeader("Content-Type", "application/json");
                    routingContext.next();
                });

                router.get("/account").handler(this::getListProduct);
                router.get("/account/:accountNumber").handler(this::getShowProduct);
                vertx.createHttpServer().requestHandler(router::accept).listen(8081);
            } else {
                throw new RuntimeException(res.cause());
            }
        });
    }

    private void setConnection(RoutingContext routingContext) {
        JdbcService proxy = JdbcService.createEventBusProxy(vertx, "vertx.jdbc");
        proxy.getConnection(res -> {
            if (res.succeeded()) {
                routingContext.put(CONNECTION, res.result());
                routingContext.addHeadersEndHandler(v -> {
                    SqlConnection connection = routingContext.get(CONNECTION);
                    connection.close(c -> {
                        if (!c.succeeded()) {
                            System.out.println(c.cause());
                        }
                    });
                });
                routingContext.next();
            } else {
                routingContext.fail(res.cause());
            }
        });
    }

    private void getListProduct(RoutingContext context) {
        SqlConnection connection = context.get(CONNECTION);

        connection.query("select * from account", res -> {
            if (res.succeeded()) {
                JsonArray arr = new JsonArray();
                List<JsonObject> rows = res.result().getRows();
                rows.forEach(arr::add);
                context.response().end(arr.encode());
            } else {
                context.fail(res.cause());
            }
        });
    }

    private void getShowProduct(RoutingContext context) {
        String productID = context.request().getParam("accountNumber");

        if (productID == null) {
            context.fail(404);
        } else {
            SqlConnection connection = context.get(CONNECTION);
            String sql = "select * from account where account_number = ?";
            JsonArray params = new JsonArray().add(productID);

            connection.queryWithParams(sql, params, res -> {
                if (res.succeeded()) {
                    List<JsonObject> rows = res.result().getRows();
                    if (rows.isEmpty()) {
                        context.fail(404);
                    } else {
                        context.response().end(rows.get(0).encode());
                    }
                } else {
                    context.fail(res.cause());
                }
            });
        }
    }

}
