/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.explore.executor;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.service.ExploreService;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
@Path(Constants.Gateway.API_VERSION_3)
public class QueryExecutorHttpHandler extends AbstractQueryExecutorHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(QueryExecutorHttpHandler.class);
  private final ExploreService exploreService;

  @Inject
  public QueryExecutorHttpHandler(ExploreService exploreService) {
    this.exploreService = exploreService;
  }

  @DELETE
  @Path("data/explore/queries/{id}")
  public void closeQuery(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
    try {
      QueryHandle handle = QueryHandle.fromId(id);
      if (!handle.equals(QueryHandle.NO_OP)) {
        exploreService.close(handle);
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (HandleNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("data/explore/queries/{id}/status")
  public void getQueryStatus(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
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
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (HandleNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("data/explore/queries/{id}/schema")
  public void getQueryResultsSchema(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
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
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (HandleNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("data/explore/queries/{id}/next")
  public void getQueryNextResults(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
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
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (HandleNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("data/explore/queries/{id}/preview")
  public void getQueryResultPreview(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
    // NOTE: this call is a POST because it is not idempotent: cursor of results is moved
    try {
      QueryHandle handle = QueryHandle.fromId(id);
      List<QueryResult> results;
      if (handle.equals(QueryHandle.NO_OP)) {
        results = Lists.newArrayList();
      } else {
        results = exploreService.previewResults(handle);
      }
      responder.sendJson(HttpResponseStatus.OK, results);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (HandleNotFoundException e) {
      if (e.isInactive()) {
        responder.sendString(HttpResponseStatus.CONFLICT, "Preview is unavailable for inactive queries.");
        return;
      }
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("data/explore/queries/{id}/download")
  public void downloadQueryResults(HttpRequest request, HttpResponder responder, @PathParam("id") final String id) {
    // NOTE: this call is a POST because it is not idempotent: cursor of results is moved
    boolean responseStarted = false;
    try {
      QueryHandle handle = QueryHandle.fromId(id);
      if (handle.equals(QueryHandle.NO_OP) ||
        !exploreService.getStatus(handle).getStatus().equals(QueryStatus.OpStatus.FINISHED)) {
        responder.sendStatus(HttpResponseStatus.CONFLICT);
        return;
      }

      StringBuffer sb = new StringBuffer();
      sb.append(getCSVHeaders(exploreService.getResultSchema(handle)));
      sb.append('\n');

      List<QueryResult> results;
      results = exploreService.previewResults(handle);
      if (results.isEmpty()) {
        results = exploreService.nextResults(handle, DOWNLOAD_FETCH_CHUNK_SIZE);
      }

      ChunkResponder chunkResponder = responder.sendChunkStart(HttpResponseStatus.OK, null);
      responseStarted = true;
      while (!results.isEmpty()) {
        for (QueryResult result : results) {
          appendCSVRow(sb, result);
          sb.append('\n');
        }
        // If failed to send to client, just propagate the IOException and let netty-http to handle
        chunkResponder.sendChunk(ChannelBuffers.wrappedBuffer(sb.toString().getBytes("UTF-8")));
        sb.delete(0, sb.length());
        results = exploreService.nextResults(handle, DOWNLOAD_FETCH_CHUNK_SIZE);
      }
      Closeables.closeQuietly(chunkResponder);

    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      // We can't send another response if sendChunkStart has been called
      if (!responseStarted) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
      }
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      if (!responseStarted) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, String.format("[SQLState %s] %s",
                                                                           e.getSQLState(), e.getMessage()));
      }
    } catch (HandleNotFoundException e) {
      if (!responseStarted) {
        if (e.isInactive()) {
          responder.sendString(HttpResponseStatus.CONFLICT, "Query is inactive");
        } else {
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        }
      }
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      if (!responseStarted) {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    }
  }
}
