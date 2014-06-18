package com.continuuity.explore.executor;

import com.continuuity.api.data.batch.RowScannable;
import com.continuuity.api.dataset.Dataset;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.explore.client.DatasetExploreFacade;
import com.continuuity.explore.service.ColumnDesc;
import com.continuuity.explore.service.ExploreService;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Row;
import com.continuuity.explore.service.Status;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.base.Charsets;
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
public class ExploreExecutorHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreExecutorHttpHandler.class);

  private static final Gson GSON = new Gson();

  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final ExploreService exploreService;
  private final DatasetFramework datasetFramework;

  @Inject
  public ExploreExecutorHttpHandler(ExploreService exploreService, DatasetFramework datasetFramework) {
    this.exploreService = exploreService;
    this.datasetFramework = datasetFramework;
  }

  /**
   * This is an internal API to enable ad-hoc exploration of a dataset instance.
   */
  @POST
  @Path("/explore/instances/{instance}/enable")
  public void enableExplore(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                            @PathParam("instance") final String instance) {
    try {
      LOG.debug("Enabling explore for dataset instance {}", instance);
      Dataset dataset = datasetFramework.getDataset(instance, null);
      if (dataset == null) {
        responder.sendError(HttpResponseStatus.NOT_FOUND, "Cannot load dataset " + instance);
        return;
      }

      if (!(dataset instanceof RowScannable)) {
        responder.sendError(HttpResponseStatus.CONFLICT, "Dataset does not implement RowScannable");
        return;
      }

      RowScannable<?> scannable = (RowScannable) dataset;
      String createStatement = DatasetExploreFacade.generateCreateStatement(instance, scannable);
      if (createStatement == null) {
        LOG.error("Empty create statement for dataset {}", instance);
        responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Empty create statement for dataset " + instance);
        return;
      }

      LOG.debug("Running create statement for dataset {} with row scannable {} - {}",
                instance,
                dataset.getClass().getName(),
                createStatement);

      Handle handle = exploreService.execute(createStatement);
      JsonObject json = new JsonObject();
      json.addProperty("id", handle.getId());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * This is an internal API to disable ad-hoc exploration of a dataset instance.
   */
  @POST
  @Path("/explore/instances/{instance}/disable")
  public void disableExplore(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                            @PathParam("instance") final String instance) {
    try {
      LOG.debug("Disabling explore for dataset instance {}", instance);
      String createStatement = DatasetExploreFacade.generateDeleteStatement(instance);
      LOG.debug("Running delete statement for dataset {} - {}",
                instance,
                createStatement);

      Handle handle = exploreService.execute(createStatement);
      JsonObject json = new JsonObject();
      json.addProperty("id", handle.getId());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/data/queries")
  public void query(HttpRequest request, HttpResponder responder) {
    try {
      Map<String, String> args = decodeArguments(request);
      String query = args.get("query");
      LOG.debug("Received query: {}", query);
      Handle handle = exploreService.execute(query);
      JsonObject json = new JsonObject();
      json.addProperty("id", handle.getId());
      responder.sendJson(HttpResponseStatus.OK, json);
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
      Handle handle = Handle.fromId(id);
      exploreService.close(handle);
      responder.sendStatus(HttpResponseStatus.OK);
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
      Handle handle = Handle.fromId(id);
      exploreService.cancel(handle);
      responder.sendStatus(HttpResponseStatus.OK);
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
      Handle handle = Handle.fromId(id);
      Status status = exploreService.getStatus(handle);
      responder.sendJson(HttpResponseStatus.OK, status);
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
      Handle handle = Handle.fromId(id);
      List<ColumnDesc> schema = exploreService.getResultSchema(handle);
      responder.sendJson(HttpResponseStatus.OK, schema);
    } catch (HandleNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/data/queries/{id}/nextResults")
  public void getQueryNextResults(HttpRequest request, HttpResponder responder, @PathParam("id") final String id) {
    // NOTE: this call is a POST because it is not idempotent: cursor of results is moved
    try {
      Map<String, String> args = decodeArguments(request);
      int size = args.containsKey("size") ? Integer.valueOf(args.get("size")) : 100;
      Handle handle = Handle.fromId(id);
      List<Row> rows = exploreService.nextResults(handle, size);
      responder.sendJson(HttpResponseStatus.OK, rows);
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
