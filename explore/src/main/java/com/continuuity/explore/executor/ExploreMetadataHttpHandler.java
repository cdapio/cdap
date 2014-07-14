package com.continuuity.explore.executor;

import com.continuuity.common.conf.Constants;
import com.continuuity.explore.client.ExploreClientUtil;
import com.continuuity.explore.service.ExploreService;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.MetaDataInfo;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that implements explore metadata APIs.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class ExploreMetadataHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreMetadataHttpHandler.class);

  private static final Gson GSON = new Gson();

  private static final String PATH = "data/metadata/";

  private final ExploreService exploreService;

  @Inject
  public ExploreMetadataHttpHandler(ExploreService exploreService) {
    this.exploreService = exploreService;
  }

  @POST
  @Path(PATH + "tables")
  public void getTables(HttpRequest request, HttpResponder responder) {
    // document that we need to pass a json. Returns a handle
    // By default asks for everything

    // NOTE: this call is a POST because we need to pass json, and it actually
    // executes a query.
    try {
      ExploreClientUtil.TablesArgs args = decodeArguments(request, ExploreClientUtil.TablesArgs.class,
                                                          new ExploreClientUtil.TablesArgs(null, null, "%", null));
      LOG.trace("Received get tables with params: {}", args.toString());
      Handle handle = exploreService.getTables(args.getCatalog(), args.getSchemaPattern(),
                                               args.getTableNamePattern(), args.getTableTypes());
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST,
                          String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path(PATH + "columns")
  public void getColumns(HttpRequest request, HttpResponder responder) {
    // document that we need to pass a json. Returns a handle
    // By default asks for everything

    // NOTE: this call is a POST because we need to pass json, and it actually
    // executes a query.
    try {
      ExploreClientUtil.ColumnsArgs args = decodeArguments(request, ExploreClientUtil.ColumnsArgs.class,
                                                           new ExploreClientUtil.ColumnsArgs(null, null, "%", "%"));
      LOG.trace("Received get columns with params: {}", args.toString());
      Handle handle = exploreService.getColumns(args.getCatalog(), args.getSchemaPattern(),
                                                args.getTableNamePattern(), args.getColumnNamePattern());
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST,
                          String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path(PATH + "schemas")
  public void getSchemas(HttpRequest request, HttpResponder responder) {
    // document that we need to pass a json. Returns a handle
    // By default asks for everything

    // NOTE: this call is a POST because we need to pass json, and it actually
    // executes a query.
    try {
      ExploreClientUtil.SchemaArgs args = decodeArguments(request, ExploreClientUtil.SchemaArgs.class,
                                                          new ExploreClientUtil.SchemaArgs(null, null));
      LOG.trace("Received get schemas with params: {}", args.toString());
      Handle handle = exploreService.getSchemas(args.getCatalog(), args.getSchemaPattern());
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST,
                          String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path(PATH + "functions")
  public void getFunctions(HttpRequest request, HttpResponder responder) {
    // document that we need to pass a json. Returns a handle
    // By default asks for everything

    // NOTE: this call is a POST because we need to pass json, and it actually
    // executes a query.
    try {
      ExploreClientUtil.FunctionsArgs args = decodeArguments(request, ExploreClientUtil.FunctionsArgs.class,
                                                          new ExploreClientUtil.FunctionsArgs(null, null, "%"));
      LOG.trace("Received get functions with params: {}", args.toString());
      Handle handle = exploreService.getFunctions(args.getCatalog(), args.getSchemaPattern(),
                                                  args.getFunctionNamePattern());
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST,
                          String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path(PATH + "tableTypes")
  public void getTableTypes(HttpRequest request, HttpResponder responder) {
    // document that we need to pass a json. Returns a handle

    // NOTE: this call is a POST because we need to pass json, and it actually
    // executes a query.
    try {
      LOG.trace("Received get table types query.");
      Handle handle = exploreService.getTableTypes();
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST,
                          String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path(PATH + "typeInfo")
  public void getTypeInfo(HttpRequest request, HttpResponder responder) {
    // document that we need to pass a json. Returns a handle

    // NOTE: this call is a POST because we need to pass json, and it actually
    // executes a query.
    try {
      LOG.trace("Received get type info query.");
      Handle handle = exploreService.getTypeInfo();
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST,
                          String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path(PATH + "info/{type}")
  public void getInfo(HttpRequest request, HttpResponder responder, @PathParam("type") final String type) {
    // document that we need to pass a json. Returns a handle

    // NOTE: this call is a POST because we need to pass json, and it actually
    // executes a query.
    try {
      LOG.trace("Received get info for {}", type);
      MetaDataInfo.InfoType infoType = MetaDataInfo.InfoType.fromString(type);
      MetaDataInfo metadataInfo = exploreService.getInfo(infoType);
      responder.sendJson(HttpResponseStatus.OK, metadataInfo);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST,
                          String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private <T> T decodeArguments(HttpRequest request, Class<T> argsType, T defaultValue) throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      return defaultValue;
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      T args = GSON.fromJson(reader, argsType);
      return args == null ? defaultValue : args;
    } catch (JsonSyntaxException e) {
      LOG.info("Failed to parse runtime arguments on {}", request.getUri(), e);
      throw e;
    } finally {
      reader.close();
    }
  }

}
