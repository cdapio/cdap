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
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;

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
@Path(Constants.Gateway.API_VERSION_2)
public class DatasetInstanceHandlerV2 extends AbstractHttpHandler {
  private final DatasetInstanceHandler datasetInstanceHandler;

  @Inject
  public DatasetInstanceHandlerV2(DatasetInstanceHandler datasetInstanceHandler) {
    this.datasetInstanceHandler = datasetInstanceHandler;
  }

  @GET
  @Path("/data/datasets/")
  public void list(HttpRequest request, HttpResponder responder) {
    datasetInstanceHandler.list(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                Constants.DEFAULT_NAMESPACE);
  }

  @GET
  @Path("/data/datasets/{name}")
  public void getInfo(HttpRequest request, HttpResponder responder,
                      @PathParam("name") String name) {
    datasetInstanceHandler.getInfo(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                   Constants.DEFAULT_NAMESPACE, name);
  }

  /**
   * Creates a new Dataset instance.
   */
  @PUT
  @Path("/data/datasets/{name}")
  public void create(HttpRequest request, HttpResponder responder,
                     @PathParam("name") String name) {
    datasetInstanceHandler.create(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                  Constants.DEFAULT_NAMESPACE, name);
  }

  /**
   * Updates an existing Dataset specification properties  {@link DatasetInstanceConfiguration}
   * is constructed based on request and the Dataset instance is updated.
   */
  @PUT
  @Path("/data/datasets/{name}/properties")
  public void update(HttpRequest request, HttpResponder responder,
                     @PathParam("name") String name) {
    datasetInstanceHandler.update(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                  Constants.DEFAULT_NAMESPACE, name);
  }

  @DELETE
  @Path("/data/datasets/{name}")
  public void drop(HttpRequest request, HttpResponder responder,
                   @PathParam("name") String name) {
    datasetInstanceHandler.drop(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                Constants.DEFAULT_NAMESPACE, name);
  }

  @POST
  @Path("/data/datasets/{name}/admin/{method}")
  public void executeAdmin(HttpRequest request, HttpResponder responder,
                           @PathParam("name") String instanceName,
                           @PathParam("method") String method) {
    datasetInstanceHandler.executeAdmin(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                        Constants.DEFAULT_NAMESPACE, instanceName, method);
  }

  @POST
  @Path("/data/datasets/{name}/data/{method}")
  public void executeDataOp(HttpRequest request, HttpResponder responder,
                            @PathParam("name") String instanceName,
                            @PathParam("method") String method) {
    datasetInstanceHandler.executeDataOp(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                         Constants.DEFAULT_NAMESPACE, instanceName, method);
  }
}
