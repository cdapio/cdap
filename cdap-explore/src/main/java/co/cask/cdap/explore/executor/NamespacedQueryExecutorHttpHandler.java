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

package co.cask.cdap.explore.executor;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.service.ExploreService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryInfo;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Provides REST endpoints for {@link ExploreService} operations.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class NamespacedQueryExecutorHttpHandler extends AbstractQueryExecutorHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(NamespacedQueryExecutorHttpHandler.class);
  private final ExploreService exploreService;

  @Inject
  public NamespacedQueryExecutorHttpHandler(ExploreService exploreService) {
    this.exploreService = exploreService;
  }

  @POST
  @Path("data/explore/queries")
  public void query(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId) {
    try {
      Map<String, String> args = decodeArguments(request);
      String query = args.get("query");
      LOG.trace("Received query: {}", query);
      responder.sendJson(HttpResponseStatus.OK, exploreService.execute(Id.Namespace.from(namespaceId), query));
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, String.format("[SQLState %s] %s",
                                                                         e.getSQLState(), e.getMessage()));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("data/explore/queries")
  public void getQueryLiveHandles(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @QueryParam("offset") @DefaultValue("9223372036854775807") long offset,
                                  @QueryParam("cursor") @DefaultValue("next") String cursor,
                                  @QueryParam("limit") @DefaultValue("50") int limit) {
    try {
      boolean isForward = "next".equals(cursor);

      List<QueryInfo> queries = exploreService.getQueries(Id.Namespace.from(namespaceId));
      // return the queries by after filtering (> offset) and limiting number of queries
      responder.sendJson(HttpResponseStatus.OK, filterQueries(queries, offset, isForward, limit));
    } catch (Exception e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error");
    }
  }
}
