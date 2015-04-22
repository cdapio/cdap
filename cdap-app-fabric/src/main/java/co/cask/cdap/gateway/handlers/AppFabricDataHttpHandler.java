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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetInstanceHandler;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *  HttpHandler class for stream and dataset requests in app-fabric.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class AppFabricDataHttpHandler extends AbstractAppFabricHttpHandler {

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
  public AppFabricDataHttpHandler(Authenticator authenticator,
                                  Store store, DatasetFramework dsFramework) {
    super(authenticator);
    this.store = store;
    this.dsFramework = dsFramework;
  }

  /**
   * Returns a list of streams in a namespace.
   */
  @GET
  @Path("/streams")
  public void getStreams(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId) {
    dataList(responder, store, dsFramework, Data.STREAM, namespaceId, null, null);
  }

  /**
   * Returns a list of dataset associated with namespace. This is here for the v2 API to use,
   * but was removed in v3 in favor of APIs in {@link DatasetInstanceHandler}.
   */
  void getDatasets(HttpRequest request, HttpResponder responder,
                   @PathParam("namespace-id") String namespaceId) {
    dataList(responder, store, dsFramework, Data.DATASET, namespaceId, null, null);
  }

  /**
   * Returns a dataset associated with namespace. This is here for the v2 API to use,
   * but was removed in v3 in favor of APIs in {@link DatasetInstanceHandler}.
   */
  void getDatasetSpecification(HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("dataset-id") String datasetId) {
    dataList(responder, store, dsFramework, Data.DATASET, namespaceId, datasetId, null);
  }
}
