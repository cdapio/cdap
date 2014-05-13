package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.common.conf.Constants;
import com.continuuity.data2.datafabric.dataset.instance.DatasetInstanceManager;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeManager;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HandlerContext;
import com.continuuity.http.HttpResponder;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;

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
@Path("/" + Constants.Dataset.Manager.VERSION)
public class DatasetInstanceHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceHandler.class);
  private static final Gson GSON = new Gson();

  private final DatasetTypeManager implManager;
  private final DatasetInstanceManager instanceManager;

  @Inject
  public DatasetInstanceHandler(DatasetTypeManager implManager, DatasetInstanceManager instanceManager) {
    this.implManager = implManager;
    this.instanceManager = instanceManager;
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting DatasetInstanceHandler");
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping DatasetInstanceHandler");
  }

  @GET
  @Path("/datasets/instances/")
  public void list(HttpRequest request, final HttpResponder responder) {
    responder.sendJson(HttpResponseStatus.OK, instanceManager.getAll());
  }

  @GET
  @Path("/datasets/instances/{instance-name}")
  public void getInfo(HttpRequest request, final HttpResponder responder,
                      @PathParam("instance-name") String name) {
    DatasetInstanceSpec spec = instanceManager.get(name);
    if (spec == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      DatasetInstanceMeta info = new DatasetInstanceMeta(spec, implManager.getTypeInfo(spec.getType()));
      responder.sendJson(HttpResponseStatus.OK, info);
    }
  }

  @POST
  @Path("/datasets/instances/{instance-name}")
  public void add(HttpRequest request, final HttpResponder responder,
                  @PathParam("instance-name") String name) {
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()));
    DatasetInstanceProperties props = GSON.fromJson(reader, DatasetInstanceProperties.class);
    String typeName = request.getHeader("type-name");

    LOG.info("Creating dataset instance {}, type name: {}, props: {}", name, typeName, props);

    DatasetInstanceSpec existing = instanceManager.get(name);
    if (existing != null) {
      String message = String.format("Cannot create dataset instance %s: instance with same name already exists %s",
                                     name, existing);
      LOG.warn(message);
      responder.sendString(HttpResponseStatus.CONFLICT, message);
      return;
    }

    DatasetDefinition type = implManager.getType(typeName);

    if (type == null) {
      String message = String.format("Cannot create dataset instance %s: unknown type %s",
                                     name, typeName);
      LOG.warn(message);
      responder.sendString(HttpResponseStatus.NOT_FOUND, message);
      return;
    }

    instanceManager.add(type.configure(name, props));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @DELETE
  @Path("/datasets/instances/{instance-name}")
  public void drop(HttpRequest request, final HttpResponder responder,
                       @PathParam("instance-name") String instanceName) {
    LOG.info("Deleting dataset instance {}", instanceName);

    if (!instanceManager.delete(instanceName)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/datasets/instances/{instance-id}/admin/{method}")
  public void executeAdmin(HttpRequest request, final HttpResponder responder,
                           @PathParam("instance-id") String instanceName,
                           @PathParam("method") String method) {
    // todo: execute admin operation
    responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
  }

  @POST
  @Path("/datasets/instances/{instance-id}/data/{method}")
  public void executeDataOp(HttpRequest request, final HttpResponder responder,
                           @PathParam("instance-id") String instanceName,
                           @PathParam("method") String method) {
    // todo: execute data operation
    responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
  }

}
