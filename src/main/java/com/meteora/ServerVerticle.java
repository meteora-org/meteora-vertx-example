package com.meteora;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.List;

public class ServerVerticle extends AbstractVerticle {

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
    }

    private static final String CONNECTION = "conn";

    private JDBCClient master;

    private JDBCClient slave;

    @Override
    public void start() {

        ServerVerticle that = this;

        master = JDBCClient.createShared(vertx, new JsonObject()
                .put("url", "jdbc:mysql://52.192.150.26:3306/meteora?characterEncoding=utf8")
                .put("user","meteora-usr")
                .put("max_pool_size", 1)
                .put("initial_pool_size", 1)
                .put("min_pool_size", 1));

        Router masterRouter = Router.router(vertx);
        masterRouter.route().handler(BodyHandler.create());

        masterRouter.get("/movie*").handler(routingContext -> master.getConnection(res -> {
            if (res.failed()) {
                routingContext.fail(res.cause());
            } else {
                SQLConnection conn = res.result();
                routingContext.put("conn", conn);
                routingContext.addHeadersEndHandler(done -> conn.close(v -> {
                }));
                routingContext.next();
            }
        })).failureHandler(routingContext -> {
            SQLConnection conn = routingContext.get(CONNECTION);
            if (conn != null) {
                conn.close(v -> {

                });
            }
        });

        masterRouter.get("/movie").handler(this::getListProduct);
        masterRouter.get("/movie/:id").handler(this::getShowProduct);
//        masterRouter.post("/movie").handler(this::addProduct);
//        masterRouter.put("/movie/:id").handler(this::updateProduct);
//        masterRouter.delete("/movie/:id").handler(this::deleteProduct);

//        DeploymentOptions options = new DeploymentOptions();
//        options.setInstances(2);
//        vertx.deployVerticle("com.meteora.ServerVerticle",options);
        vertx.createHttpServer().requestHandler(masterRouter::accept).listen(8081);

    }

    private void getListProduct(RoutingContext context) {
        HttpServerResponse response = context.response();
        SQLConnection connection = context.get("conn");

        connection.query("select * from movie", res -> {
            if (res.succeeded()) {
                JsonArray arr = new JsonArray();
                res.result().getRows().forEach(arr::add);
                context.response().putHeader("content-type", "application/json").end(arr.encode());
            } else {
                sendError(500, response);
            }
        });
    }

    private void getShowProduct(RoutingContext context) {
        String productID = context.request().getParam("id");
        SQLConnection connection = context.get(CONNECTION);

        if (productID == null) {
            context.fail(404);
        } else {

            String sql = "select * from movie where id = ?";
            JsonArray params = new JsonArray().add(productID);

            connection.queryWithParams(sql, params, res -> {
                if (res.succeeded()) {
                    List<JsonObject> rows = res.result().getRows();
                    if (rows.isEmpty()) {
                        context.fail(404);
                    } else {
                        context.response().putHeader("content-type", "application/json").end(rows.get(0).encode());
                    }
                } else {
                    context.fail(res.cause());
                }
            });
        }
    }


    private void addProduct(RoutingContext context) {
        JsonObject param = context.getBodyAsJson();
        String sql = "INSERT INTO `meteora`.`movie` ( `title`, `description`) VALUES ( ? ,  ? );";
        JsonArray params = new JsonArray().add(param.getValue("title")).add(param.getValue("description"));

        if (param == null) {
            context.fail(404);
        } else {
            SQLConnection connection = context.get(CONNECTION);

            connection.updateWithParams(sql, params, res -> {
                if (res.succeeded()) {
                    connection.query("SELECT LAST_INSERT_ID() as id ;", res2 -> {
                        Long insertId = res2.result().getRows().get(0).getLong("id");
                        context.response().putHeader("Location",context.request().path()+ "/" + insertId);
                        context.response().end();
                    });
                } else {
                    context.fail(res.cause());
                }
            });
        }
    }

    private void updateProduct(RoutingContext context) {
        String id = context.request().getParam("id");
        JsonObject param = context.getBodyAsJson();
        String sql = "UPDATE movie SET title = ? , description = ?  where id = ? ;";
        JsonArray params = new JsonArray().add(param.getValue("title")).add(param.getValue("description")).add(id);

        if (param == null || id == null) {
            context.response().setStatusCode(404).end();
        } else {
            SQLConnection connection = context.get(CONNECTION);
            connection.updateWithParams(sql, params, res -> {
                if (res.succeeded()) {
                    context.response().end();
                } else {
                    context.fail(res.cause());
                }
            });
        }
    }

    private void sendError(int statusCode, HttpServerResponse response) {
        response.setStatusCode(statusCode).end();
    }

}
