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
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 *
 */
@Path(Constants.Gateway.API_VERSION_2)
public class QueryExecutorHttpHandlerV2 extends AbstractHttpHandler {
  private final QueryExecutorHttpHandler queryExecutorHttpHandler;
  private final NamespacedQueryExecutorHttpHandler namespacedQueryExecutorHttpHandler;

  @Inject
  public QueryExecutorHttpHandlerV2(NamespacedQueryExecutorHttpHandler namespacedQueryExecutorHttpHandler,
                                    QueryExecutorHttpHandler queryExecutorHttpHandler) {
    this.namespacedQueryExecutorHttpHandler = namespacedQueryExecutorHttpHandler;
    this.queryExecutorHttpHandler = queryExecutorHttpHandler;
  }

  @DELETE
  @Path("data/explore/queries/{id}")
  public void closeQuery(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
    queryExecutorHttpHandler.closeQuery(RESTMigrationUtils.rewriteV2RequestToV3(request), responder, id);
  }

  @GET
  @Path("data/explore/queries/{id}/status")
  public void getQueryStatus(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
    queryExecutorHttpHandler.getQueryStatus(RESTMigrationUtils.rewriteV2RequestToV3(request), responder, id);
  }

  @GET
  @Path("data/explore/queries/{id}/schema")
  public void getQueryResultsSchema(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
    queryExecutorHttpHandler.getQueryResultsSchema(RESTMigrationUtils.rewriteV2RequestToV3(request), responder, id);
  }

  @POST
  @Path("data/explore/queries/{id}/next")
  public void getQueryNextResults(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
    queryExecutorHttpHandler.getQueryNextResults(RESTMigrationUtils.rewriteV2RequestToV3(request), responder, id);
  }


  @POST
  @Path("/data/explore/queries/{id}/preview")
  public void getQueryResultPreview(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
    queryExecutorHttpHandler.getQueryResultPreview(RESTMigrationUtils.rewriteV2RequestToV3(request), responder, id);
  }

  @POST
  @Path("/data/explore/queries/{id}/download")
  public void downloadQueryResults(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
    queryExecutorHttpHandler.downloadQueryResults(RESTMigrationUtils.rewriteV2RequestToV3(request), responder, id);
  }

  @POST
  @Path("data/explore/queries")
  public void query(HttpRequest request, HttpResponder responder) {
    namespacedQueryExecutorHttpHandler.query(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                             Constants.DEFAULT_NAMESPACE);
  }

  @GET
  @Path("/data/explore/queries")
  public void getQueryLiveHandles(HttpRequest request, HttpResponder responder,
                                  @QueryParam("offset") @DefaultValue("9223372036854775807") long offset,
                                  @QueryParam("cursor") @DefaultValue("next") String cursor,
                                  @QueryParam("limit") @DefaultValue("50") int limit) {
    namespacedQueryExecutorHttpHandler.getQueryLiveHandles(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                                           Constants.DEFAULT_NAMESPACE, offset, cursor, limit);
  }
}
