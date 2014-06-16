package com.continuuity.explore.executor;

import com.continuuity.api.data.batch.RowScannable;
import com.continuuity.api.dataset.Dataset;
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

  @POST
  @Path("v2/datasets/instances/{instance}/explore/enable")
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

  @POST
  @Path("v2/datasets/instances/{instance}/explore/disable")
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
  @Path("v2/datasets/queries")
  public void sendQuery(HttpRequest request, HttpResponder responder) {
    try {
      Map<String, String> args = decodeArguments(request);
      String query = args.get("query");
      LOG.info("Received query: {}", query);
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
  @Path("v2/datasets/queries/{id}")
  public void closeOperation(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
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
  @Path("v2/datasets/queries/{id}/cancel")
  public void cancelOperation(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
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
  @Path("v2/datasets/queries/{id}/status")
  public void getQueryStatus(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                             @PathParam("id") final String id) {
    try {
      Handle handle = Handle.fromId(id);
      Status status = exploreService.getStatus(handle);
      JsonObject json = new JsonObject();
      json.addProperty("status", GSON.toJson(status));
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (HandleNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("v2/datasets/queries/{id}/schema")
  public void getOperationResultsSchema(@SuppressWarnings("UnusedParameters") HttpRequest request,
                                        HttpResponder responder, @PathParam("id") final String id) {
    try {
      Handle handle = Handle.fromId(id);
      List<ColumnDesc> schema = exploreService.getResultSchema(handle);
      JsonObject json = new JsonObject();
      json.addProperty("schema", GSON.toJson(schema));
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (HandleNotFoundException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("v2/datasets/queries/{id}/nextResults")
  public void getQueryNextResults(HttpRequest request, HttpResponder responder, @PathParam("id") final String id) {
    // NOTE: this call is a POST because it is not idempotent: cursor of results is moved
    try {
      Map<String, String> args = decodeArguments(request);
      int size = args.containsKey("size") ? Integer.valueOf(args.get("size")) : 1;
      Handle handle = Handle.fromId(id);
      List<Row> rows = exploreService.nextResults(handle, size);
      JsonObject json = new JsonObject();
      json.addProperty("results", GSON.toJson(rows));
      responder.sendJson(HttpResponseStatus.OK, json);
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
