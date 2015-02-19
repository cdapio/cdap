/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.app.services.Data;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.ProgramType;
import co.cask.common.authorization.client.AuthorizationClient;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *  HttpHandler class for stream requests in app-fabric.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class AppFabricStreamHttpHandler extends AbstractAppFabricHttpHandler {

  /**
   * Access Dataset Service
   */
  private final DatasetFramework dsFramework;

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;


  /**
   * Constructs an new instance. Parameters are binded by Guice.
   */
  @Inject
  public AppFabricStreamHttpHandler(Authenticator authenticator, CConfiguration configuration,
                                    StoreFactory storeFactory, DatasetFramework dsFramework,
                                    AuthorizationClient authorizationClient) {
    super(authenticator, authorizationClient);
    this.store = storeFactory.create();
    this.dsFramework =
      new NamespacedDatasetFramework(dsFramework, new DefaultDatasetNamespace(configuration, Namespace.USER));
  }

  /**
   * Returns a list of streams associated with account.
   */
  @GET
  @Path("/streams")
  public void getStreams(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId) {
    dataList(request, responder, store, dsFramework, Data.STREAM, namespaceId, null, null);
  }

  /**
   * Returns a list of streams associated with application.
   */
  @GET
  @Path("/apps/{app-id}/streams")
  public void getStreamsByApp(HttpRequest request, HttpResponder responder,
                              @PathParam("namespace-id") String namespaceId,
                              @PathParam("app-id") final String appId) {
    dataList(request, responder, store, dsFramework, Data.STREAM, namespaceId, null, appId);
  }

  /**
   * Returns all flows associated with a stream.
   */
  @GET
  @Path("/streams/{stream-id}/flows")
  public void getFlowsByStream(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("stream-id") final String streamId) {
    programListByDataAccess(request, responder, store, dsFramework, ProgramType.FLOW, Data.STREAM,
                            namespaceId, streamId);
  }
}
