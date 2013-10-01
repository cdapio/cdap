package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.metadata.MetaDataStore;
import com.continuuity.metadata.types.Dataset;
import com.continuuity.metadata.types.Stream;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.util.List;

/**
 *  {@link MetadataServiceHandler} is REST interface to MDS store.
 */
@Path("/v2")
public class MetadataServiceHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataServiceHandler.class);
  private final MetaDataStore service;

  @Inject
  public MetadataServiceHandler(MetaDataStore service, GatewayAuthenticator authenticator) {
    super(authenticator);
    this.service = service;
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting MetadataServiceHandler.");
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping MetadataServiceHandler.");
  }

  /**
   * Returns a list of streams associated with account.
   */
  @GET
  @Path("/streams")
  public void getStreams(HttpRequest request, HttpResponder responder) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      List<Stream> streams = service.getStreams(accountId);
      JsonArray s = new JsonArray();
      for (Stream stream : streams) {
        JsonObject object = new JsonObject();
        object.addProperty("id", stream.getId());
        object.addProperty("name", stream.getName());
        object.addProperty("description", stream.getDescription());
        s.add(object);
      }
      responder.sendJson(HttpResponseStatus.OK, s);
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Returns a stream associated with account.
   */
  @GET
  @Path("/streams/{streamId}")
  public void getStreamSpecification(HttpRequest request, HttpResponder responder,
                                     @PathParam("streamId") final String streamId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      Stream stream = service.getStream(accountId, streamId);
      if (stream == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
      JsonObject object = new JsonObject();
      object.addProperty("id", stream.getId());
      object.addProperty("name", stream.getName());
      object.addProperty("description", stream.getDescription());
      object.addProperty("capacityInBytes", stream.getCapacityInBytes());
      object.addProperty("expiryInSeconds", stream.getExpiryInSeconds());
      responder.sendJson(HttpResponseStatus.OK, object);
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Returns a list of streams associated with application.
   */
  @GET
  @Path("/apps/{app-id}/streams")
  public void getStreamsByApp(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId) {

    if (appId.isEmpty()) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }

    try {
      String accountId = getAuthenticatedAccountId(request);
      List<Stream> streams = service.getStreamsByApplication(accountId, appId);
      JsonArray s = new JsonArray();
      for (Stream stream : streams) {
        JsonObject object = new JsonObject();
        object.addProperty("id", stream.getId());
        object.addProperty("name", stream.getName());
        object.addProperty("description", stream.getDescription());
        s.add(object);
      }
      responder.sendJson(HttpResponseStatus.OK, s);
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Returns a list of dataset associated with account.
   */
  @GET
  @Path("/datasets")
  public void getDatasets(HttpRequest request, HttpResponder responder) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      List<Dataset> datasets = service.getDatasets(accountId);
      JsonArray s = new JsonArray();
      for (Dataset dataset : datasets) {
        JsonObject object = new JsonObject();
        object.addProperty("id", dataset.getId());
        object.addProperty("name", dataset.getName());
        object.addProperty("description", dataset.getDescription());
        object.addProperty("classname", dataset.getType());

        s.add(object);
      }
      responder.sendJson(HttpResponseStatus.OK, s);
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Returns a dataset associated with account.
   */
  @GET
  @Path("/datasets/{datasetId}")
  public void getDatasetSpecification(HttpRequest request, HttpResponder responder,
                                      @PathParam("datasetId") final String datasetId) {

    if (datasetId.isEmpty()) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }

    try {
      String accountId = getAuthenticatedAccountId(request);

      Dataset dataset = service.getDataset(accountId, datasetId);
      if (dataset == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
      JsonObject object = new JsonObject();
      object.addProperty("id", dataset.getId());
      object.addProperty("name", dataset.getName());
      object.addProperty("description", dataset.getDescription());
      object.addProperty("type", dataset.getType());
      object.addProperty("specification", dataset.getSpecification());
      responder.sendJson(HttpResponseStatus.OK, object);
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Returns a list of dataset associated with application.
   */
  @GET
  @Path("/apps/{app-id}/datasets")
  public void getDatasetsByApp(HttpRequest request, HttpResponder responder,
                               @PathParam("app-id") final String appId) {
    if (appId.isEmpty()) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }

    try {
      String accountId = getAuthenticatedAccountId(request);
      List<Dataset> datasets = service.getDatasetsByApplication(accountId, appId);
      JsonArray s = new JsonArray();
      for (Dataset dataset : datasets) {
        JsonObject object = new JsonObject();
        object.addProperty("id", dataset.getId());
        object.addProperty("name", dataset.getName());
        object.addProperty("description", dataset.getDescription());
        object.addProperty("classname", dataset.getType());
        s.add(object);
      }
      responder.sendJson(HttpResponseStatus.OK, s);
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

}

