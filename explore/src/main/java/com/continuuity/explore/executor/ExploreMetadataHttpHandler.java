package com.continuuity.explore.executor;

import com.continuuity.common.conf.Constants;
import com.continuuity.explore.client.ExploreClientUtil;
import com.continuuity.explore.service.ExploreService;
import com.continuuity.explore.service.Handle;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
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
import java.util.List;
import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Handler that implements explore metadata APIs.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class ExploreMetadataHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreMetadataHttpHandler.class);

  private static final Gson GSON = new Gson();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

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

    // NOTE: this call is a POST because we need to pass json, and it actually
    // executes a query.
    try {
      ExploreClientUtil.TablesArgs args = decodeArguments(request, ExploreClientUtil.TablesArgs.class,
                                                          new ExploreClientUtil.TablesArgs(null, null, "%", null));
      LOG.trace("Received get tables with params: {}", args.toString());
      Handle handle = exploreService.getTables(args.getCatalogName(), args.getSchemaNamePattern(),
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
