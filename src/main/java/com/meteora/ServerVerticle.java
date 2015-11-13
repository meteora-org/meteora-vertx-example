package com.meteora;

import com.google.common.base.CaseFormat;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class ServerVerticle extends AbstractVerticle {

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
    }

    private static final String CONNECTION = "conn";

    private JDBCClient master;

    private static final String DEFAULT_LIMIT = " 100 ";

    private static final String GTE = " >= ";
    private static final String LTE = " <= ";
    private static final String EQUAL = " = ";

    private static final String[] paramKeys = {
            "findByUserId"
            ,"findByUserPublicScoreGTE"
            ,"findByUserPublicScoreLTE"
            ,"findByUserFriendsNumberGTE"
            ,"findByUserFriendsNumberLTE"
            ,"findByUserFriendsIncludeUserIds"
            ,"findByUserFriendsNotIncludeUserIds"
            ,"findByUserNotIncludeUserIds"
            ,"findByPostId"
    };

    private static final String[] paramItemKeys = {
            "findByItemId"
            ,"findByItemSupplier"
            ,"findByItemSoldQuantityGTE"
            ,"findByItemSoldQuantityLTE"
            ,"findByItemSalePriceGTE"
            ,"findByItemSalePriceLTE"
            ,"findByItemTagsIncludeAll"
            ,"findByItemTagsIncludeAny"
    };

    private static final String[] LIMIT = {"limit"};

    @Override
    public void start() {

        ServerVerticle that = this;

        master = JDBCClient.createShared(vertx, new JsonObject()
                .put("url", "jdbc:mysql://172.30.2.48:3306/meteora?characterEncoding=utf8") // 本番
//                .put("url", "jdbc:mysql://52.192.150.26:3306/meteora?characterEncoding=utf8") // 検証
                .put("user","meteora-usr")
                .put("initial_pool_size", 30)
                .put("min_pool_size", 30)
                .put("max_pool_size", 30));

        Router masterRouter = Router.router(vertx);
        masterRouter.route().handler(BodyHandler.create());

        masterRouter.get().handler(routingContext -> master.getConnection(res -> {
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

        masterRouter.get("/movie*").handler(this::getListProduct);
        masterRouter.get("/movie/:id").handler(this::getShowProduct);
        masterRouter.post("/movie").handler(this::addProduct);
        masterRouter.put("/movie/:id").handler(this::updateProduct);
        masterRouter.delete("/movie/:id").handler(this::deleteProduct);


        masterRouter.get("/searchUser*").handler(this::getSearchUser);

        masterRouter.get("/searchPost*").handler(this::getListProduct);

        masterRouter.get("/searchItem*").handler(this::getSearchItem);

        vertx.createHttpServer().requestHandler(masterRouter::accept).listen(8080);

    }

    private void getSearchUser(RoutingContext context){

        HttpServerResponse response = context.response();
        StringBuilder sql = new StringBuilder();
        sql.append(" select * from user ");

        StringJoiner where = new StringJoiner(" AND ");
        JsonArray params = new JsonArray();
        String value = null;

        createPhrase(context, sql, where, params,value);
        sql.append( where.toString() );
        createOrderBy(context,sql,params);
        createLimit(context, sql, params);

        System.out.println(sql);

        SQLConnection connection = context.get(CONNECTION);
        connection.queryWithParams(sql.toString(), params, res -> {
            JsonObject object = new JsonObject();
            object.put("result", true);
            JsonArray array = new JsonArray();

            if (res.succeeded()) {

                List<JsonObject> rows = res.result().getRows();
                List<JsonObject> result = new ArrayList<>();

                if(context.request().getParam("findByPostId") != null){
                    JsonArray postIds = new JsonArray();
                    postIds.add(context.request().getParam("findByPostId"));
                    object.put("postIds",postIds);
                }

                for (JsonObject row : rows) {
                    Map<String, Object> map = row.getMap();
                    if (map.get("userFriends") != null) {
                        String str = String.valueOf(row.getValue("userFriends"));
                        String[] split = str.split(",");
                        map.put("userFriends", split);
                    }
                    array.add(new JsonObject(map));
                }

                object.put("data", array);
                context.response().putHeader("content-type", "application/json").end(object.encode());
            } else {
                context.response().setStatusCode(404).end();
            }
        });

    }


    private void getSearchItem(RoutingContext context){

        HttpServerResponse response = context.response();
        StringBuilder sql = new StringBuilder();
        sql.append(" select * from item ");

        StringJoiner where = new StringJoiner(" AND ");
        JsonArray params = new JsonArray();
        createPhraseItem(context, sql, where, params);
        sql.append(where.toString());
        sql.append(" order by itemNo ");
        createLimit(context, sql, params);

        System.out.println(sql);

        SQLConnection connection = context.get(CONNECTION);
        connection.queryWithParams(sql.toString(), params, res -> {
            JsonObject object = new JsonObject();
            object.put("result",true);
            JsonArray array = new JsonArray();

            if (res.succeeded()) {
                List<JsonObject> rows = res.result().getRows();
                List<JsonObject> result = new ArrayList<>();
                for (JsonObject row : rows) {
                    Map<String, Object> map = row.getMap();
                    if(map.get("userFriends") != null){
                        String str = String.valueOf(row.getValue("userFriends"));
                        String[] split = str.split(",");
                        map.put("userFriends",split);
                    }
                    array.add(new JsonObject(map));
                }

                object.put("data",array);
                context.response().putHeader("content-type", "application/json").end(object.encode());
            } else {
                context.response().setStatusCode(404).end();
            }
        });
    }

    private static void createOrderBy(RoutingContext context, StringBuilder sql, JsonArray params){
        sql.append(" order by userNo " );
    }

    private static void createLimit(RoutingContext context, StringBuilder sql, JsonArray params){
        String limit = "10";
        String param = context.request().getParam("limit");
        if(param != null){
            limit = param;
        }
        sql.append(" limit " + limit);
    }

    /**
     * SQLの句を作る
     *
     * @param context
     * @param sql
     * @param where
     * @param params
     */
    private static void createPhrase(RoutingContext context, StringBuilder sql, StringJoiner where , JsonArray params, String value) {

        for (String param : paramKeys) {
            if(context.request().getParam(param) == null){
                continue;
            }

            value = context.request().getParam(param);

            if(param.equals("findByUserFriendsIncludeUserIds")){
                String[] split = value.split(",");
                StringJoiner joiner = new StringJoiner(" AND ");
                for (String s : split) {
                    params.add(s);
                    where.add(" find_in_set ( ? , userFriends ) ");
                }

                continue;
            }

            if(param.equals("findByUserFriendsNotIncludeUserIds")){
                String[] split = value.split(",");
                StringJoiner joiner = new StringJoiner(" OR ");
                for (String s : split) {
                    params.add(s);
                    joiner.add(" find_in_set ( ? , userFriends ) ");
                }

                where.add(" NOT (" + joiner.toString() + ")");

                continue;
            }

            if(param.equals("findByUserFriendsNumberGTE")){

                params.add(value);
                where.add( " userFriendsNumber " + GTE  + "   ? ");
                continue;
            }

            if(param.equals("findByUserFriendsNumberLTE")){

                params.add(context.request().getParam(param));
                where.add( " userFriendsNumber " + LTE  + "   ? ");

                continue;
            }

            if(param.equals("findByPostId")){
                params.add(value);
                where.add( " userId = (select postUserId from post where postId = ? )");
                continue;
            }

            if(param.matches(".*GTE$")){

                params.add(value);
                where.add(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL,param.replaceAll("(findBy|GTE)","")) + GTE  + "   ? ");
                continue;
            }

            if(param.matches(".*LTE$")){

                params.add(value);
                where.add(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL,param.replaceAll("(findBy|LTE)","")) + LTE  + "  ? ");
                continue;
            }

            if(param.matches("findByUserFriendsIncludeUserIds")){
                String[] items = context.request().getParam(param).split(",");

                StringJoiner join = new StringJoiner(",");
                for (String item : items) {
                    params.add(item);
                    join.add("?");
                }

                where.add( param + " in ( " + join.toString() + " ) ");

                continue;

            }

            params.add(context.request().getParam(param));
            where.add(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL,param.replace("findBy","")) + EQUAL + " ? ");
        }

        if(where.length() != 0 ){
            sql.append(" where ");
        }

    }

    private static void createPhraseItem(RoutingContext context, StringBuilder sql, StringJoiner where , JsonArray params) {

        for (String param : paramItemKeys) {
            if(context.request().getParam(param) == null){
                continue;
            }

            String value = context.request().getParam(param);

            if(false){
                continue;
            }

            if(param.equals("findByItemTagsIncludeAll")){
                String[] split = value.split(",");
                StringJoiner joiner = new StringJoiner(" AND ");
                for (String s : split) {
                    params.add(s);
                    where.add(" find_in_set ( ? , itemTags ) ");
                }

                continue;
            }

            if(param.equals("findByItemTagsIncludeAny")){
                String[] split = value.split(",");
                StringJoiner joiner = new StringJoiner(" OR ");
                for (String s : split) {
                    params.add(s);
                    joiner.add(" find_in_set ( ? , itemTags ) ");
                }

                where.add(" NOT (" + joiner.toString() + ")");

                continue;
            }

            if(param.matches(".*GTE$")){

                params.add(context.request().getParam(param));
                where.add(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL,param.replaceAll("(findBy|GTE)","")) + GTE  + "   ? ");
                continue;
            }

            if(param.matches(".*LTE$")){

                params.add(context.request().getParam(param));
                where.add(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL,param.replaceAll("(findBy|LTE)","")) + LTE  + "  ? ");
                continue;
            }

            if(param.matches("findByUserFriendsIncludeUserIds")){
                String[] items = context.request().getParam(param).split(",");

                StringJoiner join = new StringJoiner(",");
                for (String item : items) {
                    params.add(item);
                    join.add("?");
                }

                where.add( param + " in ( " + join.toString() + " ) ");

                continue;

            }

            params.add(context.request().getParam(param));
            where.add(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL,param.replace("findBy","")) + EQUAL + " ? ");
        }

        if(where.length() != 0 ){
            sql.append(" where ");
        }

    }

    private void getListProduct(RoutingContext context) {
        HttpServerResponse response = context.response();
        String productID = context.request().getParam("id");
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
            context.response().setStatusCode(404).end();
        } else {


            String sql = "select * from movie where id = ?";
            JsonArray params = new JsonArray().add(productID);

            connection.queryWithParams(sql, params, res -> {
                if (res.succeeded()) {
                    JsonArray arr = new JsonArray();
                    res.result().getRows().forEach(arr::add);
                    if (arr.isEmpty()) {
                        context.response().setStatusCode(404).end();
                    } else {
                        context.response().putHeader("content-type", "application/json").end(arr.encode());
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
            context.response().setStatusCode(404).end();
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

    private void deleteProduct(RoutingContext context) {
        String id = context.request().getParam("id");

        if ( id == null) {
            context.fail(404);
        } else {
            SQLConnection connection = context.get(CONNECTION);
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

    private void sendError(int statusCode, HttpServerResponse response) {
        response.setStatusCode(statusCode).end();
    }

}
