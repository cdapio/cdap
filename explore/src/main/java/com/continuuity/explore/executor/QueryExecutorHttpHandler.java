/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.explore.executor;

import com.continuuity.common.conf.Constants;
import com.continuuity.explore.service.ExploreService;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.proto.ColumnDesc;
import com.continuuity.proto.QueryHandle;
import com.continuuity.proto.QueryResult;
import com.continuuity.proto.QueryStatus;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
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
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Provides REST endpoints for {@link com.continuuity.explore.service.ExploreService} operations.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class QueryExecutorHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(QueryExecutorHttpHandler.class);

  private static final Gson GSON = new Gson();

  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final ExploreService exploreService;

  @Inject
  public QueryExecutorHttpHandler(ExploreService exploreService) {
    this.exploreService = exploreService;
  }

  @POST
  @Path("/data/queries")
  public void query(HttpRequest request, HttpResponder responder) {
    try {
      Map<String, String> args = decodeArguments(request);
      String query = args.get("query");
      LOG.trace("Received query: {}", query);
      responder.sendJson(HttpResponseStatus.OK, exploreService.execute(query));
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

  @DELETE
  @Path("/data/queries/{id}")
  public void closeQuery(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                         @PathParam("id") final String id) {
    try {
      QueryHandle handle = QueryHandle.fromId(id);
      if (!handle.equals(QueryHandle.NO_OP)) {
        exploreService.close(handle);
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (HandleNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/data/queries/{id}/cancel")
  public void cancelQuery(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                          @PathParam("id") final String id) {
    try {
      QueryHandle handle = QueryHandle.fromId(id);
      if (!handle.equals(QueryHandle.NO_OP)) {
        exploreService.cancel(handle);
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST,
                          String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (HandleNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/data/queries/{id}/status")
  public void getQueryStatus(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                             @PathParam("id") final String id) {
    try {
      QueryHandle handle = QueryHandle.fromId(id);
      QueryStatus status;
      if (!handle.equals(QueryHandle.NO_OP)) {
        status = exploreService.getStatus(handle);
      } else {
        status = QueryStatus.NO_OP;
      }
      responder.sendJson(HttpResponseStatus.OK, status);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST,
                          String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (HandleNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/data/queries/{id}/schema")
  public void getQueryResultsSchema(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                                    @PathParam("id") final String id) {
    try {
      QueryHandle handle = QueryHandle.fromId(id);
      List<ColumnDesc> schema;
      if (!handle.equals(QueryHandle.NO_OP)) {
        schema = exploreService.getResultSchema(handle);
      } else {
        schema = Lists.newArrayList();
      }
      responder.sendJson(HttpResponseStatus.OK, schema);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST,
                          String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (HandleNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/data/queries/{id}/next")
  public void getQueryNextResults(HttpRequest request, HttpResponder responder, @PathParam("id") final String id) {
    // NOTE: this call is a POST because it is not idempotent: cursor of results is moved
    try {
      QueryHandle handle = QueryHandle.fromId(id);
      List<QueryResult> results;
      if (handle.equals(QueryHandle.NO_OP)) {
        results = Lists.newArrayList();
      } else {
        Map<String, String> args = decodeArguments(request);
        int size = args.containsKey("size") ? Integer.valueOf(args.get("size")) : 100;
        results = exploreService.nextResults(handle, size);
      }
      responder.sendJson(HttpResponseStatus.OK, results);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST,
                          String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (HandleNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private Map<String, String> decodeArguments(HttpRequest request) throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      return ImmutableMap.of();
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      Map<String, String> args = GSON.fromJson(reader, STRING_MAP_TYPE);
      return args == null ? ImmutableMap.<String, String>of() : args;
    } catch (JsonSyntaxException e) {
      LOG.info("Failed to parse runtime arguments on {}", request.getUri(), e);
      throw e;
    } finally {
      reader.close();
    }
  }
}
