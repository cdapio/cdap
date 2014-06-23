package com.continuuity.explore.executor;

import com.continuuity.api.data.batch.RecordScannable;
import com.continuuity.api.dataset.Dataset;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.explore.client.DatasetExploreFacade;
import com.continuuity.explore.service.ExploreService;
import com.continuuity.explore.service.Handle;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that implements internal explore APIs.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class ExploreExecutorHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(QueryExecutorHttpHandler.class);

  private final ExploreService exploreService;
  private final DatasetFramework datasetFramework;

  @Inject
  public ExploreExecutorHttpHandler(ExploreService exploreService, DatasetFramework datasetFramework) {
    this.exploreService = exploreService;
    this.datasetFramework = datasetFramework;
  }

  /**
   * Enable ad-hoc exploration of a dataset instance.
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

      if (!(dataset instanceof RecordScannable)) {
        // It is not an error to get non-RecordScannable datasets, since the type of dataset may not be known where this
        // call originates from.
        LOG.debug("Dataset {} does not implement {}", instance, RecordScannable.class.getName());
        JsonObject json = new JsonObject();
        json.addProperty("handle", Handle.NO_OP.getHandle());
        responder.sendJson(HttpResponseStatus.OK, json);
        return;
      }

      RecordScannable<?> scannable = (RecordScannable) dataset;
      String createStatement;
      try {
        createStatement = DatasetExploreFacade.generateCreateStatement(instance, scannable);
      } catch (UnsupportedTypeException e) {
        LOG.error("Exception while generating create statement for dataset {}", instance, e);
        responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        return;
      }

      LOG.debug("Running create statement for dataset {} with row scannable {} - {}",
                instance,
                dataset.getClass().getName(),
                createStatement);

      Handle handle = exploreService.execute(createStatement);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Disable ad-hoc exploration of a dataset instance.
   */
  @POST
  @Path("/explore/instances/{instance}/disable")
  public void disableExplore(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                             @PathParam("instance") final String instance) {
    try {
      LOG.debug("Disabling explore for dataset instance {}", instance);

      Dataset dataset = datasetFramework.getDataset(instance, null);
      if (dataset == null) {
        responder.sendError(HttpResponseStatus.NOT_FOUND, "Cannot load dataset " + instance);
        return;
      }

      if (!(dataset instanceof RecordScannable)) {
        // It is not an error to get non-RecordScannable datasets, since the type of dataset may not be known where this
        // call originates from.
        LOG.debug("Dataset {} does not implement {}", instance, RecordScannable.class.getName());
        JsonObject json = new JsonObject();
        json.addProperty("handle", Handle.NO_OP.getHandle());
        responder.sendJson(HttpResponseStatus.OK, json);
        return;
      }

      String deleteStatement = DatasetExploreFacade.generateDeleteStatement(instance);
      LOG.debug("Running delete statement for dataset {} - {}",
                instance,
                deleteStatement);

      Handle handle = exploreService.execute(deleteStatement);
      JsonObject json = new JsonObject();
      json.addProperty("handle", handle.getHandle());
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

}
