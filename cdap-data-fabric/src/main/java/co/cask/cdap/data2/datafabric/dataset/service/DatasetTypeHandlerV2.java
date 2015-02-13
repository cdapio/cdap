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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.RESTMigrationUtils;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.BodyConsumer;
import co.cask.http.HandlerContext;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handles dataset type management v2 API calls.
 */
// todo: do we want to make it authenticated? or do we treat it always as "internal" piece?
@Path(Constants.Gateway.API_VERSION_2)
public class DatasetTypeHandlerV2 extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetTypeHandlerV2.class);
  private static final String HEADER_CLASS_NAME = "X-Class-Name";

  private final DatasetTypeHandler datasetTypeHandler;

  @Inject
  public DatasetTypeHandlerV2(DatasetTypeHandler datasetTypeHandler) {
    this.datasetTypeHandler = datasetTypeHandler;
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
  public void listModules(HttpRequest request, HttpResponder responder) {
    datasetTypeHandler.listModules(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                   Constants.DEFAULT_NAMESPACE);
  }

  @DELETE
  @Path("/data/modules")
  public void deleteModules(HttpRequest request, HttpResponder responder) {
    datasetTypeHandler.deleteModules(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                     Constants.DEFAULT_NAMESPACE);
  }

  @PUT
  @Path("/data/modules/{name}")
  public BodyConsumer addModule(HttpRequest request, HttpResponder responder,
                                @PathParam("name") String name,
                                @HeaderParam(HEADER_CLASS_NAME) String className) throws IOException {
    return datasetTypeHandler.addModule(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                        Constants.DEFAULT_NAMESPACE, name, className);
  }

  @DELETE
  @Path("/data/modules/{name}")
  public void deleteModule(HttpRequest request, HttpResponder responder, @PathParam("name") String name) {
    datasetTypeHandler.deleteModule(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                    Constants.DEFAULT_NAMESPACE, name);
  }

  @GET
  @Path("/data/modules/{name}")
  public void getModuleInfo(HttpRequest request, HttpResponder responder, @PathParam("name") String name) {
    datasetTypeHandler.getModuleInfo(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                     Constants.DEFAULT_NAMESPACE, name);
  }

  @GET
  @Path("/data/types")
  public void listTypes(HttpRequest request, HttpResponder responder) {
    datasetTypeHandler.listTypes(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                 Constants.DEFAULT_NAMESPACE);
  }

  @GET
  @Path("/data/types/{name}")
  public void getTypeInfo(HttpRequest request, HttpResponder responder, @PathParam("name") String name) {
    datasetTypeHandler.getTypeInfo(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                   Constants.DEFAULT_NAMESPACE, name);
  }
}
