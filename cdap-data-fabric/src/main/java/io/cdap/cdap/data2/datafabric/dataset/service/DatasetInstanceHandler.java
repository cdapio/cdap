/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.service;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.DatasetAlreadyExistsException;
import io.cdap.cdap.common.DatasetTypeNotFoundException;
import io.cdap.cdap.common.HandlerException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.data2.datafabric.dataset.DatasetServiceClient;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.proto.DatasetInstanceConfiguration;
import io.cdap.cdap.proto.DatasetMeta;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handles dataset instance management calls.
 */
// todo: do we want to make it authenticated? or do we treat it always as "internal" piece?
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class DatasetInstanceHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceHandler.class);

  private final DatasetInstanceService instanceService;
  private static final Gson GSON = new Gson();

  @Inject
  public DatasetInstanceHandler(DatasetInstanceService instanceService) {
    this.instanceService = instanceService;
  }

  @GET
  @Path("/data/datasets/")
  public void list(HttpRequest request, HttpResponder responder,
                   @PathParam("namespace-id") String namespaceId) throws Exception {
    logCallReceived(request);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(ConversionHelpers.spec2Summary(
      instanceService.list(ConversionHelpers.toNamespaceId(namespaceId)))));
    logCallResponded(request);
  }

  @POST
  @Path("/data/datasets/")
  public void listWithSpecifiedProperties(FullHttpRequest request, HttpResponder responder,
                                          @PathParam("namespace-id") String namespaceId) throws Exception {
    logCallReceived(request);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(ConversionHelpers.spec2Summary(
      instanceService.list(ConversionHelpers.toNamespaceId(namespaceId), getDatasetProperties(request)))));
    logCallResponded(request);
  }

  /**
   * Gets the {@link DatasetMeta} for a dataset instance.
   *
   * @param namespaceId namespace of the dataset instance
   * @param name name of the dataset instance
   * @throws NotFoundException if the dataset instance was not found
   */
  @GET
  @Path("/data/datasets/{name}")
  public void get(HttpRequest request, HttpResponder responder,
                  @PathParam("namespace-id") String namespaceId,
                  @PathParam("name") String name) throws Exception {
    logCallReceived(request);
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(instanceService.get(ConversionHelpers.toDatasetInstanceId(namespaceId, name)),
                                   DatasetMeta.class));
    logCallResponded(request);
  }

  /**
   * Creates a new dataset instance.
   *
   * @param namespaceId namespace of the new dataset instance
   * @param name name of the new dataset instance
   */
  @PUT
  @Path("/data/datasets/{name}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void create(FullHttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                     @PathParam("name") String name) throws Exception {
    logCallReceived(request);
    DatasetInstanceConfiguration creationProperties = ConversionHelpers.getInstanceConfiguration(request);
    try {
      instanceService.create(namespaceId, name, creationProperties);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (DatasetAlreadyExistsException e) {
      responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (DatasetTypeNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, e.getMessage());
    } catch (HandlerException e) {
      responder.sendString(e.getFailureStatus(), e.getMessage());
    }
    logCallResponded(request);
  }

  @GET
  @Path("/data/datasets/{name}/properties")
  public void getProperties(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("name") String name) throws Exception {
    logCallReceived(request);
    DatasetId instance = ConversionHelpers.toDatasetInstanceId(namespaceId, name);
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(instanceService.getOriginalProperties(instance),
                                   new TypeToken<Map<String, String>>() { }.getType()));
    logCallResponded(request);
  }

  /**
   * Updates an existing dataset specification properties.
   *
   * @param namespaceId namespace of the dataset instance
   * @param name name of the dataset instance
   * @throws Exception
   */
  @PUT
  @Path("/data/datasets/{name}/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void update(FullHttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId,
                     @PathParam("name") String name) throws Exception {
    logCallReceived(request);
    DatasetId instance = ConversionHelpers.toDatasetInstanceId(namespaceId, name);
    Map<String, String> properties = ConversionHelpers.getProperties(request);
    instanceService.update(instance, properties);
    responder.sendStatus(HttpResponseStatus.OK);
    logCallResponded(request);
  }

  /**
   * Deletes a dataset instance, which also deletes the data owned by it.
   *
   * @param namespaceId namespace of the dataset instance
   * @param name name of the dataset instance
   * @throws Exception
   */
  @DELETE
  @Path("/data/datasets/{name}")
  public void drop(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("name") String name) throws Exception {
    logCallReceived(request);
    DatasetId instance = ConversionHelpers.toDatasetInstanceId(namespaceId, name);
    instanceService.drop(instance);
    responder.sendStatus(HttpResponseStatus.OK);
    logCallResponded(request);
  }

  @DELETE
  @Path("/data/datasets")
  public void dropAll(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId) throws Exception {
    logCallReceived(request);
    instanceService.dropAll(ConversionHelpers.toNamespaceId(namespaceId));
    responder.sendStatus(HttpResponseStatus.OK);
    logCallResponded(request);
  }

  /**
   * Executes an admin operation on a dataset instance.
   *
   * @param namespaceId namespace of the dataset instance
   * @param name name of the dataset instance
   * @param method the admin operation to execute (e.g. "exists", "truncate", "upgrade")
   * @throws Exception
   */
  @POST
  @Path("/data/datasets/{name}/admin/{method}")
  public void executeAdmin(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                           @PathParam("name") String name,
                           @PathParam("method") String method) throws Exception {
    logCallReceived(request);
    DatasetId instance = ConversionHelpers.toDatasetInstanceId(namespaceId, name);
    try {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(instanceService.executeAdmin(instance, method)));
    } catch (HandlerException e) {
      responder.sendStatus(e.getFailureStatus());
    }
    logCallResponded(request);
  }

  /**
   * Executes a data operation on a dataset instance. Not yet implemented.
   *
   * @param namespaceId namespace of the dataset instance
   * @param name name of the dataset instance
   * @param method the data operation to execute
   */
  @POST
  @Path("/data/datasets/{name}/data/{method}")
  public void executeDataOp(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                            @PathParam("name") String name, @PathParam("method") String method) {
    // todo: execute data operation
    responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
  }

  private Map<String, String> getDatasetProperties(FullHttpRequest request) {
    Map<String, String> properties = new HashMap<>();
    ByteBuf content = request.content();
    if (!content.isReadable()) {
      return properties;
    }

    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
      properties = GSON.fromJson(reader, new TypeToken<Map<String, String>>() { }.getType());
    } catch (IOException e) {
      // no-op since is happens during closing of the reader
    }
    return properties;
  }

  private void logCallReceived(HttpRequest request) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Received {} for {} from {}",
                request.method(), request.uri(), DatasetServiceClient.getCallerId(request));
    }
  }

  private void logCallResponded(HttpRequest request) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Responded to {} for {} from {}",
                request.method(), request.uri(), DatasetServiceClient.getCallerId(request));
    }
  }
}
