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
        vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(100));
        super.init(vertx, context);
    }

    @Override
    public void start() throws Exception {

        JsonObject config = new JsonObject().put("url", "jdbc:mysql://52.192.150.26:3306/meteora?characterEncoding=utf8")
                .put("user","meteora-usr")
                .put("max_pool_size", 1)
                .put("initial_pool_size", 1)
                .put("min_pool_size", 1);
        DeploymentOptions options = new DeploymentOptions().setConfig(config).setInstances(2).setMultiThreaded(true).setWorker(true);

        vertx.deployVerticle("service:io.vertx.jdbc-service", options, res -> {
            if (res.succeeded()) {
                Router router = Router.router(vertx);

                router.route().handler(BodyHandler.create());
                router.route().handler(this::setConnection);
                router.route().handler(routingContext -> {
                    routingContext.response().putHeader("Content-Type", "application/json");
                    routingContext.next();
                });

                router.get("/movie").handler(this::getListProduct);
                router.get("/movie/:id").handler(this::getShowProduct);
                router.post("/movie").handler(this::addProduct);
                router.put("/movie/:id").handler(this::updateProduct);
                router.delete("/movie/:id").handler(this::deleteProduct);
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

        connection.query("select * from movie", res -> {
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
        String productID = context.request().getParam("id");

        if (productID == null) {
            context.fail(404);
        } else {
            SqlConnection connection = context.get(CONNECTION);
            String sql = "select * from movie where id = ?";
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

    private void addProduct(RoutingContext context) {
        JsonObject param = context.getBodyAsJson();
        System.out.println(param);

        if (param == null) {
            context.fail(404);
        } else {
            SqlConnection connection = context.get(CONNECTION);
            String sql = "INSERT INTO `meteora`.`movie` ( `title`, `description`) VALUES ( ? ,  ? );";
            JsonArray params = new JsonArray().add(param.getValue("title")).add(param.getValue("description"));

            connection.updateWithParams(sql, params, res -> {
                if (res.succeeded()) {
                    context.response().end();
                } else {
                    context.fail(res.cause());
                }
            });
        }
    }

    private void updateProduct(RoutingContext context) {
        String id = context.request().getParam("id");
        JsonObject param = context.getBodyAsJson();

        if (param == null || id == null) {
            context.fail(404);
        } else {
            SqlConnection connection = context.get(CONNECTION);
            String sql = "UPDATE movie SET title = ? , description = ?  where id = ? ;";
            JsonArray params = new JsonArray().add(param.getValue("title")).add(param.getValue("description")).add(id);

            connection.updateWithParams(sql, params, res -> {
                if (res.succeeded()) {
                    context.response().end();
                } else {
                    context.fail(res.cause());
                }
            });
        }
    }

    private void deleteProduct(RoutingContext context) {
        String id = context.request().getParam("id");

        if ( id == null) {
            context.fail(404);
        } else {
            SqlConnection connection = context.get(CONNECTION);
            String sql = "delete from movie where id = ? ";
            JsonArray params = new JsonArray().add(id);

            connection.updateWithParams(sql, params, res -> {
                if (res.succeeded()) {
                    context.response().end();
                } else {
                    context.fail(res.cause());
                }
            });
        }
    }

}
