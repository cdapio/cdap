/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.service.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.IncompatibleUpdateException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Provides REST endpoints for {@link DatasetAdmin} operations.
 * The corresponding client is {@link RemoteDatasetOpExecutor}.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class DatasetAdminOpHTTPHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetAdminOpHTTPHandler.class);
  private static final Gson GSON = new Gson();

  private final DatasetAdminService datasetAdminService;

  @Inject
  @VisibleForTesting
  public DatasetAdminOpHTTPHandler(DatasetAdminService datasetAdminService) {
    this.datasetAdminService = datasetAdminService;
  }

  @POST
  @Path("/data/datasets/{name}/admin/exists")
  public void exists(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId,
                     @PathParam("name") String instanceName) throws Exception {
    propagateUserId(request);
    NamespaceId namespace = new NamespaceId(namespaceId);
    DatasetId instanceId = namespace.dataset(instanceName);
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(new DatasetAdminOpResponse(datasetAdminService.exists(instanceId), null)));
  }

  @POST
  @Path("/data/datasets/{name}/admin/create")
  public void create(FullHttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId,
                     @PathParam("name") String name) throws Exception {
    propagateUserId(request);
    InternalDatasetCreationParams params = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
                                                         InternalDatasetCreationParams.class);
    Preconditions.checkArgument(params.getProperties() != null, "Missing required 'instanceProps' parameter.");
    Preconditions.checkArgument(params.getTypeMeta() != null, "Missing required 'typeMeta' parameter.");

    DatasetProperties props = params.getProperties();
    DatasetTypeMeta typeMeta = params.getTypeMeta();

    try {
      DatasetId instanceId = new DatasetId(namespaceId, name);
      DatasetCreationResponse response = datasetAdminService.createOrUpdate(instanceId, typeMeta, props, null);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(response));
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  @POST
  @Path("/data/datasets/{name}/admin/update")
  public void update(FullHttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId,
                     @PathParam("name") String name) throws Exception {
    propagateUserId(request);
    InternalDatasetUpdateParams params = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
                                                       InternalDatasetUpdateParams.class);
    Preconditions.checkArgument(params.getProperties() != null, "Missing required 'instanceProps' parameter.");
    Preconditions.checkArgument(params.getTypeMeta() != null, "Missing required 'typeMeta' parameter.");
    Preconditions.checkArgument(params.getExistingSpec() != null, "Missing required 'existingSpec' parameter.");

    DatasetProperties props = params.getProperties();
    DatasetSpecification existing = params.getExistingSpec();
    DatasetTypeMeta typeMeta = params.getTypeMeta();

    try {
      DatasetId instanceId = new DatasetId(namespaceId, name);
      DatasetCreationResponse response = datasetAdminService.createOrUpdate(instanceId, typeMeta, props, existing);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(response));
    } catch (IncompatibleUpdateException e) {
      throw new ConflictException(e.getMessage());
    }
  }

  @POST
  @Path("/data/datasets/{name}/admin/drop")
  public void drop(FullHttpRequest request, HttpResponder responder,
                   @PathParam("namespace-id") String namespaceId,
                   @PathParam("name") String instanceName) throws Exception {
    propagateUserId(request);
    InternalDatasetDropParams params = GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
                                                     InternalDatasetDropParams.class);
    Preconditions.checkArgument(params.getInstanceSpec() != null, "Missing required 'instanceSpec' parameter.");
    Preconditions.checkArgument(params.getTypeMeta() != null, "Missing required 'typeMeta' parameter.");

    DatasetSpecification spec = params.getInstanceSpec();
    DatasetTypeMeta typeMeta = params.getTypeMeta();

    datasetAdminService.drop(new DatasetId(namespaceId, instanceName), typeMeta, spec);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(spec));
  }

  @POST
  @Path("/data/datasets/{name}/admin/truncate")
  public void truncate(HttpRequest request, HttpResponder responder,
                       @PathParam("namespace-id") String namespaceId,
                       @PathParam("name") String instanceName) throws Exception {
    propagateUserId(request);
    DatasetId instanceId = new DatasetId(namespaceId, instanceName);
    datasetAdminService.truncate(instanceId);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(new DatasetAdminOpResponse(null, null)));
  }

  @POST
  @Path("/data/datasets/{name}/admin/upgrade")
  public void upgrade(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @PathParam("name") String instanceName) throws Exception {
    propagateUserId(request);
    DatasetId instanceId = new DatasetId(namespaceId, instanceName);
    datasetAdminService.upgrade(instanceId);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(new DatasetAdminOpResponse(null, null)));
  }

  // propagate user id from the HTTP Request in the current thread
  private void propagateUserId(HttpRequest request) {
    String userId = request.headers().get(Constants.Security.Headers.USER_ID);
    if (userId != null) {
      LOG.debug("Propagating userId as {}", userId);
      SecurityRequestContext.setUserId(userId);
    }
  }
}
