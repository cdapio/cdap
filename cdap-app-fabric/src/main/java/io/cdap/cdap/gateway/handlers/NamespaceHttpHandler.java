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

package io.cdap.cdap.gateway.handlers;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.stream.Collectors;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link HttpHandler} for handling REST calls to namespace endpoints.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class NamespaceHttpHandler extends AbstractAppFabricHttpHandler {

  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final NamespaceAdmin namespaceAdmin;

  @Inject
  NamespaceHttpHandler(CConfiguration cConf, NamespaceAdmin namespaceAdmin) {
    this.cConf = cConf;
    this.namespaceAdmin = namespaceAdmin;
  }

  @GET
  @Path("/namespaces")
  public void getAllNamespaces(HttpRequest request, HttpResponder responder) throws Exception {
    // return keytab URI without version
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(namespaceAdmin.list().stream()
                                     .map(meta -> new NamespaceMeta.Builder(meta).buildWithoutKeytabURIVersion())
                                     .collect(Collectors.toList())));
  }

  @GET
  @Path("/namespaces/{namespace-id}")
  public void getNamespace(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) throws Exception {
    // return keytab URI without version
    NamespaceMeta ns =
      new NamespaceMeta.Builder(namespaceAdmin.get(new NamespaceId(namespaceId))).buildWithoutKeytabURIVersion();
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(ns));
  }


  @PUT
  @Path("/namespaces/{namespace-id}/properties")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void updateNamespaceProperties(FullHttpRequest request, HttpResponder responder,
                                        @PathParam("namespace-id") String namespaceId) throws Exception {
    NamespaceMeta meta = getNamespaceMeta(request);
    namespaceAdmin.updateProperties(new NamespaceId(namespaceId), meta);
    responder.sendString(HttpResponseStatus.OK, String.format("Updated properties for namespace '%s'.", namespaceId));
  }

  @PUT
  @Path("/namespaces/{namespace-id}")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void create(FullHttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId) throws Exception {
    NamespaceId namespace;
    try {
      namespace = new NamespaceId(namespaceId);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("Namespace id can contain only alphanumeric characters or '_'.");
    }

    NamespaceMeta metadata = getNamespaceMeta(request);

    if (NamespaceId.isReserved(namespaceId)) {
      throw new BadRequestException(String.format("Cannot create the namespace '%s'. '%s' is a reserved namespace.",
                                                  namespaceId, namespaceId));
    }

    NamespaceMeta.Builder builder = metadata == null ? new NamespaceMeta.Builder() :
      new NamespaceMeta.Builder(metadata);
    builder.setName(namespace);
    builder.setGeneration(System.currentTimeMillis());

    NamespaceMeta finalMetadata = builder.build();

    try {
      namespaceAdmin.create(finalMetadata);
      responder.sendString(HttpResponseStatus.OK,
                           String.format("Namespace '%s' created successfully.", namespaceId));
    } catch (AlreadyExistsException e) {
      responder.sendString(HttpResponseStatus.OK, String.format("Namespace '%s' already exists.", namespaceId));
    }
  }

  @DELETE
  @Path("/unrecoverable/namespaces/{namespace-id}")
  public void delete(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespace)
    throws Exception {
    if (!cConf.getBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, Constants.Dangerous.DEFAULT_UNRECOVERABLE_RESET)) {
      responder.sendString(HttpResponseStatus.FORBIDDEN,
                           String.format("Namespace '%s' cannot be deleted because '%s' is not enabled. " +
                                           "Please enable it and restart CDAP Master.",
                                         namespace, Constants.Dangerous.UNRECOVERABLE_RESET));
      return;
    }
    NamespaceId namespaceId = new NamespaceId(namespace);
    namespaceAdmin.delete(namespaceId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @DELETE
  @Path("/unrecoverable/namespaces/{namespace-id}/datasets")
  public void deleteDatasets(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespace) throws Exception {
    if (!cConf.getBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, Constants.Dangerous.DEFAULT_UNRECOVERABLE_RESET)) {
      responder.sendString(HttpResponseStatus.FORBIDDEN,
                           String.format("All datasets in namespace %s cannot be deleted because '%s' is not enabled." +
                                           " Please enable it and restart CDAP Master.",
                                         namespace, Constants.Dangerous.UNRECOVERABLE_RESET));
      return;
    }
    NamespaceId namespaceId = new NamespaceId(namespace);
    namespaceAdmin.deleteDatasets(namespaceId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private NamespaceMeta getNamespaceMeta(FullHttpRequest request) throws BadRequestException {
    try {
      return parseBody(request, NamespaceMeta.class);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid json object provided in request body.");
    }
  }
}
