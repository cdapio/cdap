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

package co.cask.cdap.data2.datafabric.dataset.service.executor;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Provides REST endpoints for {@link DatasetAdmin} operations.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class DatasetAdminOpHTTPHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetAdminOpHTTPHandler.class);
  private static final Gson GSON = new Gson();

  private final DatasetAdminService datasetAdminService;

  @Inject
  public DatasetAdminOpHTTPHandler(DatasetAdminService datasetAdminService) {
    this.datasetAdminService = datasetAdminService;
  }

  @POST
  @Path("/data/datasets/{name}/admin/exists")
  public void exists(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId,
                     @PathParam("name") String instanceName) {
    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    try {
      Id.DatasetInstance instanceId = Id.DatasetInstance.from(namespace, instanceName);
      responder.sendJson(HttpResponseStatus.OK,
                         new DatasetAdminOpResponse(datasetAdminService.exists(instanceId), null));
    } catch (NotFoundException e) {
      LOG.debug("Got handler exception", e);
      responder.sendString(HttpResponseStatus.NOT_FOUND, StringUtils.defaultIfEmpty(e.getMessage(), ""));
    } catch (Exception e) {
      LOG.error(getAdminOpErrorMessage("exists", instanceName), e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, getAdminOpErrorMessage("exists", instanceName));
    }
  }

  @POST
  @Path("/data/datasets/{name}/admin/create")
  public void create(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId,
                     @PathParam("name") String name) {
    InternalDatasetCreationParams params = GSON.fromJson(request.getContent().toString(Charsets.UTF_8),
                                                         InternalDatasetCreationParams.class);
    Preconditions.checkArgument(params.getProperties() != null, "Missing required 'instanceProps' parameter.");
    Preconditions.checkArgument(params.getTypeMeta() != null, "Missing required 'typeMeta' parameter.");
    Preconditions.checkArgument(params.isExisting() != null, "Missing required 'existing' parameter.");

    DatasetProperties props = params.getProperties();
    DatasetTypeMeta typeMeta = params.getTypeMeta();

    try {
      Id.DatasetInstance instanceId = Id.DatasetInstance.from(namespaceId, name);
      DatasetSpecification spec = datasetAdminService.create(instanceId, typeMeta, props, params.isExisting());
      responder.sendJson(HttpResponseStatus.OK, spec);
    } catch (BadRequestException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @POST
  @Path("/data/datasets/{name}/admin/drop")
  public void drop(HttpRequest request, HttpResponder responder,
                   @PathParam("namespace-id") String namespaceId,
                   @PathParam("name") String instanceName) throws Exception {

    InternalDatasetDropParams params = GSON.fromJson(request.getContent().toString(Charsets.UTF_8),
                                                     InternalDatasetDropParams.class);
    Preconditions.checkArgument(params.getInstanceSpec() != null, "Missing required 'instanceSpec' parameter.");
    Preconditions.checkArgument(params.getTypeMeta() != null, "Missing required 'typeMeta' parameter.");

    DatasetSpecification spec = params.getInstanceSpec();
    DatasetTypeMeta typeMeta = params.getTypeMeta();

    try {
      datasetAdminService.drop(Id.DatasetInstance.from(namespaceId, instanceName), typeMeta, spec);
      responder.sendJson(HttpResponseStatus.OK, spec);
    } catch (BadRequestException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }
  }

  @POST
  @Path("/data/datasets/{name}/admin/truncate")
  public void truncate(HttpRequest request, HttpResponder responder,
                       @PathParam("namespace-id") String namespaceId,
                       @PathParam("name") String instanceName) {
    try {
      Id.DatasetInstance instanceId = Id.DatasetInstance.from(namespaceId, instanceName);
      datasetAdminService.truncate(instanceId);
      responder.sendJson(HttpResponseStatus.OK, new DatasetAdminOpResponse(null, null));
    } catch (NotFoundException e) {
      LOG.debug("Got handler exception", e);
      responder.sendString(HttpResponseStatus.NOT_FOUND, StringUtils.defaultIfEmpty(e.getMessage(), ""));
    } catch (Exception e) {
      LOG.error(getAdminOpErrorMessage("truncate", instanceName), e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, getAdminOpErrorMessage("truncate", instanceName));
    }
  }

  @POST
  @Path("/data/datasets/{name}/admin/upgrade")
  public void upgrade(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @PathParam("name") String instanceName) {
    try {
      Id.DatasetInstance instanceId = Id.DatasetInstance.from(namespaceId, instanceName);
      datasetAdminService.upgrade(instanceId);
      responder.sendJson(HttpResponseStatus.OK, new DatasetAdminOpResponse(null, null));
    } catch (NotFoundException e) {
      LOG.debug("Got handler exception", e);
      responder.sendString(HttpResponseStatus.NOT_FOUND, StringUtils.defaultIfEmpty(e.getMessage(), ""));
    } catch (Exception e) {
      LOG.error(getAdminOpErrorMessage("upgrade", instanceName), e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, getAdminOpErrorMessage("upgrade", instanceName));
    }
  }

  private String getAdminOpErrorMessage(String opName, String instanceName) {
    return String.format("Error executing admin operation %s for dataset instance %s", opName, instanceName);
  }
}
