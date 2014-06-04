package com.continuuity.data2.datafabric.dataset.service.executor;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.exception.HandlerException;
import com.continuuity.data2.datafabric.dataset.RemoteDatasetFramework;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.continuuity.data2.dataset2.DatasetManagementException;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.AuthenticatedHttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Provides REST endpoints for {@link DatasetAdmin} operations.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class DatasetAdminOpHTTPHandler extends AuthenticatedHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetAdminOpHTTPHandler.class);

  private static final Gson GSON = new Gson();

  private final RemoteDatasetFramework client;

  @Inject
  public DatasetAdminOpHTTPHandler(Authenticator authenticator, RemoteDatasetFramework client) {
    super(authenticator);
    this.client = client;
  }

  @POST
  @Path("/data/instances/{instance}/admin/exists")
  public void exists(HttpRequest request, HttpResponder responder, @PathParam("instance") String instanceName) {
    try {
      DatasetAdmin datasetAdmin = getDatasetAdmin(instanceName);
      responder.sendJson(HttpResponseStatus.OK, new DatasetAdminOpResponse(datasetAdmin.exists(), null));
    } catch (HandlerException e) {
      LOG.debug("Got handler exception", e);
      responder.sendError(e.getFailureStatus(), StringUtils.defaultIfEmpty(e.getMessage(), ""));
    } catch (Exception e) {
      LOG.error(getAdminOpErrorMessage("exists", instanceName), e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, getAdminOpErrorMessage("exists", instanceName));
    }
  }

  @POST
  @Path("/data/instances/{instance}/admin/create")
  public void create(HttpRequest request, HttpResponder responder, @PathParam("instance") String name)
  throws Exception {

    String propsHeader = request.getHeader("instance-props");
    Preconditions.checkArgument(propsHeader != null, "Missing required 'instance-props' header.");
    String typeMetaHeader = request.getHeader("type-meta");
    Preconditions.checkArgument(propsHeader != null, "Missing required 'type-meta' header.");

    LOG.info("Creating dataset instance {}, type meta: {}, props: {}", name, typeMetaHeader, propsHeader);

    DatasetInstanceProperties props = GSON.fromJson(propsHeader, DatasetInstanceProperties.class);
    DatasetTypeMeta typeMeta = GSON.fromJson(typeMetaHeader, DatasetTypeMeta.class);

    DatasetDefinition def = client.getDatasetDefinition(typeMeta, null);

    if (def == null) {
      String msg = String.format("Cannot instantiate dataset type using provided type meta: %s", typeMeta);
      LOG.error(msg);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, msg);
      return;
    }

    DatasetInstanceSpec spec = def.configure(name, props);
    DatasetAdmin admin = def.getAdmin(spec);
    admin.create();
    responder.sendJson(HttpResponseStatus.OK, spec);
  }

  @POST
  @Path("/data/instances/{instance}/admin/drop")
  public void drop(HttpRequest request, HttpResponder responder, @PathParam("instance") String instanceName)
    throws Exception {

    String specHeader = request.getHeader("instance-spec");
    Preconditions.checkArgument(specHeader != null, "Missing required 'instance-spec' header.");
    String typeMetaHeader = request.getHeader("type-meta");
    Preconditions.checkArgument(specHeader != null, "Missing required 'type-meta' header.");

    LOG.info("Dropping dataset with spec: {}, type meta: {}", specHeader, typeMetaHeader);

    DatasetInstanceSpec spec = GSON.fromJson(specHeader, DatasetInstanceSpec.class);
    DatasetTypeMeta typeMeta = GSON.fromJson(typeMetaHeader, DatasetTypeMeta.class);

    DatasetDefinition def = client.getDatasetDefinition(typeMeta, null);

    if (def == null) {
      String msg = String.format("Cannot instantiate dataset type using provided type meta: %s", typeMeta);
      LOG.error(msg);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, msg);
      return;
    }

    DatasetAdmin admin = def.getAdmin(spec);
    admin.drop();
    responder.sendJson(HttpResponseStatus.OK, spec);
  }

  @POST
  @Path("/data/instances/{instance}/admin/truncate")
  public void truncate(HttpRequest request, HttpResponder responder, @PathParam("instance") String instanceName) {
    try {
      DatasetAdmin datasetAdmin = getDatasetAdmin(instanceName);
      datasetAdmin.truncate();
      responder.sendJson(HttpResponseStatus.OK, new DatasetAdminOpResponse(null, null));
    } catch (HandlerException e) {
      LOG.debug("Got handler exception", e);
      responder.sendError(e.getFailureStatus(), StringUtils.defaultIfEmpty(e.getMessage(), ""));
    } catch (Exception e) {
      LOG.error(getAdminOpErrorMessage("truncate", instanceName), e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, getAdminOpErrorMessage("truncate", instanceName));
    }
  }

  @POST
  @Path("/data/instances/{instance}/admin/upgrade")
  public void upgrade(HttpRequest request, HttpResponder responder, @PathParam("instance") String instanceName) {
    try {
      DatasetAdmin datasetAdmin = getDatasetAdmin(instanceName);
      datasetAdmin.upgrade();
      responder.sendJson(HttpResponseStatus.OK, new DatasetAdminOpResponse(null, null));
    } catch (HandlerException e) {
      LOG.debug("Got handler exception", e);
      responder.sendError(e.getFailureStatus(), StringUtils.defaultIfEmpty(e.getMessage(), ""));
    } catch (Exception e) {
      LOG.error(getAdminOpErrorMessage("upgrade", instanceName), e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, getAdminOpErrorMessage("upgrade", instanceName));
    }
  }

  private String getAdminOpErrorMessage(String opName, String instanceName) {
    return String.format("Error executing admin operation %s for dataset instance %s", opName, instanceName);
  }

  private DatasetAdmin getDatasetAdmin(String instanceName) throws IOException, DatasetManagementException {
    DatasetAdmin admin = client.getAdmin(instanceName, getClassLoader(instanceName));
    if (admin == null) {
      throw new HandlerException(HttpResponseStatus.NOT_FOUND,
                                 "Couldn't obtain DatasetAdmin for dataset instance " + instanceName);
    }
    return admin;
  }

  private ClassLoader getClassLoader(String instanceName) {
    // TODO
    return null;
  }

}
