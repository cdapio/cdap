/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.RESTMigrationUtils;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * v2 REST endpoints for {@link DatasetAdmin} operations.
 */
@Path(Constants.Gateway.API_VERSION_2)
public class DatasetAdminOpHTTPHandlerV2 extends AuthenticatedHttpHandler {

  private final DatasetAdminOpHTTPHandler datasetAdminOpHTTPHandler;

  @Inject
  public DatasetAdminOpHTTPHandlerV2(Authenticator authenticator, DatasetAdminOpHTTPHandler datasetAdminOpHTTPHandler) {
    super(authenticator);
    this.datasetAdminOpHTTPHandler = datasetAdminOpHTTPHandler;
  }

  @POST
  @Path("/data/datasets/{name}/admin/exists")
  public void exists(HttpRequest request, HttpResponder responder,
                     @PathParam("name") String instanceName) {
    datasetAdminOpHTTPHandler.exists(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                     Constants.DEFAULT_NAMESPACE, instanceName);
  }

  @POST
  @Path("/data/datasets/{name}/admin/create")
  public void create(HttpRequest request, HttpResponder responder,
                     @PathParam("name") String name) throws Exception {
    datasetAdminOpHTTPHandler.create(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                     Constants.DEFAULT_NAMESPACE, name);
  }

  @POST
  @Path("/data/datasets/{name}/admin/drop")
  public void drop(HttpRequest request, HttpResponder responder,
                   @PathParam("name") String instanceName) throws Exception {
    datasetAdminOpHTTPHandler.drop(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                   Constants.DEFAULT_NAMESPACE, instanceName);
  }

  @POST
  @Path("/data/datasets/{name}/admin/truncate")
  public void truncate(HttpRequest request, HttpResponder responder,
                       @PathParam("name") String instanceName) {
    datasetAdminOpHTTPHandler.truncate(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                       Constants.DEFAULT_NAMESPACE, instanceName);
  }

  @POST
  @Path("/data/datasets/{name}/admin/upgrade")
  public void upgrade(HttpRequest request, HttpResponder responder,
                      @PathParam("name") String instanceName) {
    datasetAdminOpHTTPHandler.upgrade(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                      Constants.DEFAULT_NAMESPACE, instanceName);
  }
}
