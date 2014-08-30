/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service.executor;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.HandlerException;
import co.cask.cdap.data.runtime.DatasetClassLoaderUtil;
import co.cask.cdap.data.runtime.DatasetClassLoaders;
import co.cask.cdap.data2.datafabric.dataset.DatasetAdminWrapper;
import co.cask.cdap.data2.datafabric.dataset.DatasetType;
import co.cask.cdap.data2.datafabric.dataset.DatasetTypeWrapper;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.http.HttpResponder;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.commons.lang.StringUtils;
import org.apache.twill.filesystem.LocationFactory;
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

  private final RemoteDatasetFramework dsFramework;
  private final LocationFactory locationFactory;

  @Inject
  public DatasetAdminOpHTTPHandler(Authenticator authenticator, RemoteDatasetFramework dsFramework,
                                   LocationFactory locationFactory) {
    super(authenticator);
    this.dsFramework = dsFramework;
    this.locationFactory = locationFactory;
  }

  @POST
  @Path("/data/datasets/{name}/admin/exists")
  public void exists(HttpRequest request, HttpResponder responder, @PathParam("name") String instanceName)
    throws IOException {
    DatasetAdminWrapper datasetAdminWrapper = null;
    try {
      datasetAdminWrapper = getDatasetAdminWrapper(instanceName);
      responder.sendJson(HttpResponseStatus.OK,
                         new DatasetAdminOpResponse(datasetAdminWrapper.getDatasetAdmin().exists(), null));
    } catch (HandlerException e) {
      LOG.debug("Got handler exception", e);
      responder.sendError(e.getFailureStatus(), StringUtils.defaultIfEmpty(e.getMessage(), ""));
    } catch (Exception e) {
      LOG.error(getAdminOpErrorMessage("exists", instanceName), e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, getAdminOpErrorMessage("exists", instanceName));
    } finally {
      if (datasetAdminWrapper != null) {
        datasetAdminWrapper.cleanup();
      }
    }
  }

  @POST
  @Path("/data/datasets/{name}/admin/create")
  public void create(HttpRequest request, HttpResponder responder, @PathParam("name") String name)
  throws Exception {
    DatasetTypeWrapper datasetTypeWrapper = null;
    try {
      String propsHeader = request.getHeader("instance-props");
      Preconditions.checkArgument(propsHeader != null, "Missing required 'instance-props' header.");
      String typeMetaHeader = request.getHeader("type-meta");
      Preconditions.checkArgument(propsHeader != null, "Missing required 'type-meta' header.");

      LOG.info("Creating dataset instance {}, type meta: {}, props: {}", name, typeMetaHeader, propsHeader);

      DatasetProperties props = GSON.fromJson(propsHeader, DatasetProperties.class);
      DatasetTypeMeta typeMeta = GSON.fromJson(typeMetaHeader, DatasetTypeMeta.class);

      datasetTypeWrapper = getDatasetTypeWrapper(typeMeta);
      DatasetType type = datasetTypeWrapper.getDatasetType();

      if (type == null) {
        String msg = String.format("Cannot instantiate dataset type using provided type meta: %s", typeMeta);
        LOG.error(msg);
        responder.sendError(HttpResponseStatus.BAD_REQUEST, msg);
        return;
      }

      DatasetSpecification spec = type.configure(name, props);
      DatasetAdmin admin = type.getAdmin(spec);
      admin.create();
      responder.sendJson(HttpResponseStatus.OK, spec);
    } finally {
      if (datasetTypeWrapper != null) {
        datasetTypeWrapper.cleanup();
      }
    }
  }

  @POST
  @Path("/data/datasets/{name}/admin/drop")
  public void drop(HttpRequest request, HttpResponder responder, @PathParam("name") String instanceName)
    throws Exception {
    DatasetTypeWrapper datasetTypeWrapper = null;
    try {
      String specHeader = request.getHeader("instance-spec");
      Preconditions.checkArgument(specHeader != null, "Missing required 'instance-spec' header.");
      String typeMetaHeader = request.getHeader("type-meta");
      Preconditions.checkArgument(specHeader != null, "Missing required 'type-meta' header.");

      LOG.info("Dropping dataset with spec: {}, type meta: {}", specHeader, typeMetaHeader);

      DatasetSpecification spec = GSON.fromJson(specHeader, DatasetSpecification.class);
      DatasetTypeMeta typeMeta = GSON.fromJson(typeMetaHeader, DatasetTypeMeta.class);
      datasetTypeWrapper = getDatasetTypeWrapper(typeMeta);
      DatasetType type = datasetTypeWrapper.getDatasetType();

      if (type == null) {
        String msg = String.format("Cannot instantiate dataset type using provided type meta: %s", typeMeta);
        LOG.error(msg);
        responder.sendError(HttpResponseStatus.BAD_REQUEST, msg);
        return;
      }
      DatasetAdmin admin = type.getAdmin(spec);
      admin.drop();
      responder.sendJson(HttpResponseStatus.OK, spec);
    } finally {
      if (datasetTypeWrapper != null) {
        datasetTypeWrapper.cleanup();
      }
    }
  }

  @POST
  @Path("/data/datasets/{name}/admin/truncate")
  public void truncate(HttpRequest request, HttpResponder responder, @PathParam("name") String instanceName)
    throws IOException {
    DatasetAdminWrapper datasetAdminWrapper = null;
    try {
      datasetAdminWrapper = getDatasetAdminWrapper(instanceName);
      datasetAdminWrapper.getDatasetAdmin().truncate();
      responder.sendJson(HttpResponseStatus.OK, new DatasetAdminOpResponse(null, null));
    } catch (HandlerException e) {
      LOG.debug("Got handler exception", e);
      responder.sendError(e.getFailureStatus(), StringUtils.defaultIfEmpty(e.getMessage(), ""));
    } catch (Exception e) {
      LOG.error(getAdminOpErrorMessage("truncate", instanceName), e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, getAdminOpErrorMessage("truncate", instanceName));
    } finally {
      if (datasetAdminWrapper != null) {
        datasetAdminWrapper.cleanup();
      }
    }
  }

  @POST
  @Path("/data/datasets/{name}/admin/upgrade")
  public void upgrade(HttpRequest request, HttpResponder responder, @PathParam("name") String instanceName)
    throws IOException {
    DatasetAdminWrapper datasetAdminWrapper = null;
    try {
      datasetAdminWrapper = getDatasetAdminWrapper(instanceName);
      datasetAdminWrapper.getDatasetAdmin().upgrade();
      responder.sendJson(HttpResponseStatus.OK, new DatasetAdminOpResponse(null, null));
    } catch (HandlerException e) {
      LOG.debug("Got handler exception", e);
      responder.sendError(e.getFailureStatus(), StringUtils.defaultIfEmpty(e.getMessage(), ""));
    } catch (Exception e) {
      LOG.error(getAdminOpErrorMessage("upgrade", instanceName), e);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, getAdminOpErrorMessage("upgrade", instanceName));
    } finally {
      if (datasetAdminWrapper != null) {
        datasetAdminWrapper.cleanup();
      }
    }
  }

  private String getAdminOpErrorMessage(String opName, String instanceName) {
    return String.format("Error executing admin operation %s for dataset instance %s", opName, instanceName);
  }

  private DatasetAdminWrapper getDatasetAdminWrapper(String instanceName)
    throws IOException, DatasetManagementException {
    ClassLoader parentClassLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                   getClass().getClassLoader());
    if (dsFramework.getDatasetSpec(instanceName) == null) {
      throw new HandlerException(HttpResponseStatus.NOT_FOUND,
                                 String.format("Dataset instance %s does not exist", instanceName));
    }
    DatasetClassLoaderUtil dsUtil = DatasetClassLoaders.createDatasetClassLoaderFromType
      (parentClassLoader, dsFramework.getType(dsFramework.getDatasetSpec(instanceName).getType()), locationFactory);
    DatasetAdmin admin = dsFramework.getAdmin(instanceName, dsUtil.getClassLoader());

    if (admin == null) {
      throw new HandlerException(HttpResponseStatus.NOT_FOUND,
                                 "Couldn't obtain DatasetAdmin for dataset instance " + instanceName);
    }

    return new DatasetAdminWrapper(dsUtil, admin);
  }

  private DatasetTypeWrapper getDatasetTypeWrapper(DatasetTypeMeta typeMeta)
    throws IOException, DatasetManagementException {
    ClassLoader parentClassLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                         getClass().getClassLoader());
    DatasetClassLoaderUtil dsUtil = DatasetClassLoaders.createDatasetClassLoaderFromType
      (parentClassLoader, typeMeta, locationFactory);
    DatasetType datasetType = dsFramework.getDatasetType(typeMeta, dsUtil.getClassLoader());

    if (datasetType == null) {
      throw new HandlerException(HttpResponseStatus.NOT_FOUND,
                                 "Couldn't obtain DatasetType type: " + typeMeta.getName());
    }
    return new DatasetTypeWrapper(dsUtil, datasetType);
  }

}
