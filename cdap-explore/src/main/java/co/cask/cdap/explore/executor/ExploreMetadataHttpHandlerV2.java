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

package co.cask.cdap.explore.executor;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.RESTMigrationUtils;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that implements V2 explore metadata APIs
 */
@Path(Constants.Gateway.API_VERSION_2)
public class ExploreMetadataHttpHandlerV2 extends AbstractExploreMetadataHttpHandler {
  private final NamespacedExploreMetadataHttpHandler namespacedExploreMetadataHttpHandler;
  private final ExploreMetadataHttpHandler exploreMetadataHttpHandler;

  @Inject
  public ExploreMetadataHttpHandlerV2(NamespacedExploreMetadataHttpHandler namespacedExploreMetadataHttpHandler,
                                      ExploreMetadataHttpHandler exploreMetadataHttpHandler) {
    this.namespacedExploreMetadataHttpHandler = namespacedExploreMetadataHttpHandler;
    this.exploreMetadataHttpHandler = exploreMetadataHttpHandler;
  }

  @GET
  @Path("data/explore/tables")
  public void getTables(HttpRequest request, HttpResponder responder) {
    namespacedExploreMetadataHttpHandler.getTables(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                         Constants.DEFAULT_NAMESPACE);
  }

  @GET
  @Path("data/explore/tables/{table}/info")
  public void getTableSchema(HttpRequest request, HttpResponder responder, @PathParam("table") String table) {
    namespacedExploreMetadataHttpHandler.getTableSchema(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                              Constants.DEFAULT_NAMESPACE, table);
  }

  @POST
  @Path("data/explore/jdbc/tables")
  public void getJDBCTables(HttpRequest request, HttpResponder responder) {
    namespacedExploreMetadataHttpHandler.getJDBCTables(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                             Constants.DEFAULT_NAMESPACE);
  }

  @POST
  @Path("data/explore/jdbc/columns")
  public void getJDBCColumns(HttpRequest request, HttpResponder responder) {
    namespacedExploreMetadataHttpHandler.getJDBCColumns(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                              Constants.DEFAULT_NAMESPACE);
  }

  @POST
  @Path("data/explore/jdbc/catalogs")
  public void getJDBCCatalogs(HttpRequest request, HttpResponder responder) {
    exploreMetadataHttpHandler.getJDBCCatalogs(RESTMigrationUtils.rewriteV2RequestToV3(request), responder);
  }

  @POST
  @Path("data/explore/jdbc/schemas")
  public void getJDBCSchemas(HttpRequest request, HttpResponder responder) {
    namespacedExploreMetadataHttpHandler.getJDBCSchemas(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                              Constants.DEFAULT_NAMESPACE);
  }

  @POST
  @Path("data/explore/jdbc/functions")
  public void getJDBCFunctions(HttpRequest request, HttpResponder responder) {
    namespacedExploreMetadataHttpHandler.getJDBCFunctions(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                                Constants.DEFAULT_NAMESPACE);
  }

  @POST
  @Path("data/explore/jdbc/tableTypes")
  public void getJDBCTableTypes(HttpRequest request, HttpResponder responder) {
    exploreMetadataHttpHandler.getJDBCTableTypes(RESTMigrationUtils.rewriteV2RequestToV3(request), responder);
  }

  @POST
  @Path("data/explore/jdbc/types")
  public void getJDBCTypes(HttpRequest request, HttpResponder responder) {
    exploreMetadataHttpHandler.getJDBCTypes(RESTMigrationUtils.rewriteV2RequestToV3(request), responder);
  }

  @GET
  @Path("data/explore/jdbc/info/{type}")
  public void getJDBCInfo(HttpRequest request, HttpResponder responder, @PathParam("type") String type) {
    exploreMetadataHttpHandler.getJDBCInfo(RESTMigrationUtils.rewriteV2RequestToV3(request), responder, type);
  }
}
