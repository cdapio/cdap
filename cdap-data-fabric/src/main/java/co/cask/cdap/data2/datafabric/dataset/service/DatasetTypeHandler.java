/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.security.AuditDetail;
import co.cask.cdap.common.security.AuditPolicy;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.BodyConsumer;
import co.cask.http.HandlerContext;
import co.cask.http.HttpResponder;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Handles dataset type management calls.
 */
// todo: do we want to make it authenticated? or do we treat it always as "internal" piece?
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class DatasetTypeHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetTypeHandler.class);
  private static final Gson GSON = new Gson();

  private static final String HEADER_CLASS_NAME = "X-Class-Name";

  private final DatasetTypeService typeService;

  @Inject
  DatasetTypeHandler(DatasetTypeService typeService) {
    this.typeService = typeService;
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting DatasetTypeHandler");
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping DatasetTypeHandler");
  }

  @GET
  @Path("/data/modules")
  public void listModules(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(typeService.listModules(new NamespaceId(namespaceId))));
  }

  @DELETE
  @Path("/data/modules")
  public void deleteModules(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) throws Exception {
    typeService.deleteAll(new NamespaceId(namespaceId));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @PUT
  @Path("/data/modules/{name}")
  @AuditPolicy(AuditDetail.HEADERS)
  public BodyConsumer addModule(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId, @PathParam("name") final String name,
                                @QueryParam("force") final boolean forceUpdate,
                                @HeaderParam(HEADER_CLASS_NAME) final String className) throws Exception {
    return typeService.addModule(new NamespaceId(namespaceId).datasetModule(name), className, forceUpdate);
  }

  @DELETE
  @Path("/data/modules/{name}")
  public void deleteModule(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId,
                           @PathParam("name") String name) throws Exception {
    typeService.delete(new NamespaceId(namespaceId).datasetModule(name));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/data/modules/{name}")
  public void getModuleInfo(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("name") String name) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(typeService.getModule(new NamespaceId(namespaceId).datasetModule(name))));
  }

  @GET
  @Path("/data/types")
  public void listTypes(HttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") String namespaceId) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(typeService.listTypes(new NamespaceId(namespaceId))));
  }

  @GET
  @Path("/data/types/{name}")
  public void getTypeInfo(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId,
                          @PathParam("name") String name) throws Exception {
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(typeService.getType(new NamespaceId(namespaceId).datasetType(name))));
  }
}
