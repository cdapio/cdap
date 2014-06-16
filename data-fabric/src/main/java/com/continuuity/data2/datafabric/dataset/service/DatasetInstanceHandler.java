package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.exception.HandlerException;
import com.continuuity.data2.datafabric.dataset.instance.DatasetInstanceManager;
import com.continuuity.data2.datafabric.dataset.service.executor.DatasetAdminOpResponse;
import com.continuuity.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeManager;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.continuuity.explore.client.DatasetExploreFacade;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.Reader;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handles dataset instance management calls.
 */
// todo: do we want to make it authenticated? or do we treat it always as "internal" piece?
@Path(Constants.Gateway.GATEWAY_VERSION)
public class DatasetInstanceHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceHandler.class);
  private static final Gson GSON = new Gson();

  private final DatasetTypeManager implManager;
  private final DatasetInstanceManager instanceManager;
  private final DatasetOpExecutor opExecutorClient;
  private final DatasetExploreFacade datasetExploreFacade;

  @Inject
  public DatasetInstanceHandler(DatasetTypeManager implManager, DatasetInstanceManager instanceManager,
                                DatasetOpExecutor opExecutorClient, DatasetExploreFacade datasetExploreFacade) {
    this.opExecutorClient = opExecutorClient;
    this.implManager = implManager;
    this.instanceManager = instanceManager;
    this.datasetExploreFacade = datasetExploreFacade;
  }

  @GET
  @Path("/data/instances/")
  public void list(HttpRequest request, final HttpResponder responder) {
    responder.sendJson(HttpResponseStatus.OK, instanceManager.getAll());
  }

  @DELETE
  @Path("/data/instances/")
  public void deleteAll(HttpRequest request, final HttpResponder responder) throws Exception {
    for (DatasetSpecification spec : instanceManager.getAll()) {
      // skip if not exists: someone may be deleting it at same time
      if (!instanceManager.delete(spec.getName())) {
        continue;
      }

      try {
        opExecutorClient.drop(spec, implManager.getTypeInfo(spec.getType()));
      } catch (Exception e) {
        String msg = String.format("Cannot delete dataset instance %s: executing delete() failed, reason: %s",
                                   spec.getName(), e.getMessage());
        LOG.warn(msg, e);
        // we continue deleting if something wring happens.
        // todo: Will later be improved by doing all in async: see REACTOR-200
      }
    }

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/data/instances/{instance-name}")
  public void getInfo(HttpRequest request, final HttpResponder responder,
                      @PathParam("instance-name") String name) {
    DatasetSpecification spec = instanceManager.get(name);
    if (spec == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      DatasetInstanceMeta info = new DatasetInstanceMeta(spec, implManager.getTypeInfo(spec.getType()));
      responder.sendJson(HttpResponseStatus.OK, info);
    }
  }

  @POST
  @Path("/data/instances/{instance-name}")
  public void add(HttpRequest request, final HttpResponder responder,
                  @PathParam("instance-name") String name) {
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()));
    DatasetProperties props = GSON.fromJson(reader, DatasetProperties.class);
    String typeName = request.getHeader("type-name");

    LOG.info("Creating dataset instance {}, type name: {}, props: {}", name, typeName, props);

    DatasetSpecification existing = instanceManager.get(name);
    if (existing != null) {
      String message = String.format("Cannot create dataset instance %s: instance with same name already exists %s",
                                     name, existing);
      LOG.warn(message);
      responder.sendString(HttpResponseStatus.CONFLICT, message);
      return;
    }

    DatasetTypeMeta typeMeta = implManager.getTypeInfo(typeName);
    if (typeMeta == null) {
      String message = String.format("Cannot create dataset instance %s: unknown type %s",
                                     name, typeName);
      LOG.warn(message);
      responder.sendString(HttpResponseStatus.NOT_FOUND, message);
      return;
    }

    // Note how we execute configure() via opExecutorClient (outside of ds service) to isolate running user code
    DatasetSpecification spec;
    try {
      spec = opExecutorClient.create(name, typeMeta, props);
    } catch (Exception e) {
      String msg = String.format("Cannot create dataset instance %s of type %s: executing create() failed, reason: %s",
                                 name, typeName, e.getMessage());
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }
    instanceManager.add(spec);

    // Enable ad-hoc exploration of dataset
    try {
      datasetExploreFacade.enableExplore(name);
    } catch (ExploreException e) {
      String msg = String.format("Cannot enable exploration of dataset instance %s of type %s: %s",
                                 name, typeName, e.getMessage());
      LOG.error(msg, e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, msg);
    }

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @DELETE
  @Path("/data/instances/{instance-name}")
  public void drop(HttpRequest request, final HttpResponder responder,
                       @PathParam("instance-name") String name) {
    LOG.info("Deleting dataset instance {}", name);


    // First disable ad-hoc exploration of dataset
    try {
      datasetExploreFacade.disableExplore(name);
    } catch (ExploreException e) {
      String msg = String.format("Cannot disable exploration of dataset instance %s: %s",
                                 name, e.getMessage());
      LOG.error(msg, e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, msg);
    }

    DatasetSpecification spec = instanceManager.get(name);
    if (spec == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    if (!instanceManager.delete(name)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    try {
      opExecutorClient.drop(spec, implManager.getTypeInfo(spec.getType()));
    } catch (Exception e) {
      String msg = String.format("Cannot delete dataset instance %s: executing delete() failed, reason: %s",
                                 name, e.getMessage());
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/data/instances/{instance-id}/admin/{method}")
  public void executeAdmin(HttpRequest request, final HttpResponder responder,
                           @PathParam("instance-id") String instanceName,
                           @PathParam("method") String method) {

    try {
      Object result = null;
      String message = null;

      // NOTE: one cannot directly call create and drop, instead this should be called thru
      //       POST/DELETE @ /data/instances/{instance-id}. Because we must create/drop metadata for these at same time
      if (method.equals("exists")) {
        result = opExecutorClient.exists(instanceName);
      } else if (method.equals("truncate")) {
        opExecutorClient.truncate(instanceName);
      } else if (method.equals("upgrade")) {
        opExecutorClient.upgrade(instanceName);
      } else {
        throw new HandlerException(HttpResponseStatus.NOT_FOUND, "Invalid admin operation: " + method);
      }

      DatasetAdminOpResponse response = new DatasetAdminOpResponse(result, message);
      responder.sendJson(HttpResponseStatus.OK, response);
    } catch (HandlerException e) {
      LOG.debug("Handler error", e);
      responder.sendStatus(e.getFailureStatus());
    } catch (Exception e) {
      LOG.error("Error executing admin operation {} for dataset instance {}", method, instanceName, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @POST
  @Path("/data/instances/{instance-id}/data/{method}")
  public void executeDataOp(HttpRequest request, final HttpResponder responder,
                           @PathParam("instance-id") String instanceName,
                           @PathParam("method") String method) {
    // todo: execute data operation
    responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
  }

}
