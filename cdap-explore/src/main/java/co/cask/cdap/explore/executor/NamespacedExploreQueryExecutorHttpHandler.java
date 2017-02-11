/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.common.kerberos.ImpersonatedOpType;
import co.cask.cdap.common.security.AuditDetail;
import co.cask.cdap.common.security.AuditPolicy;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.ExploreService;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryInfo;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
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
public class NamespacedExploreQueryExecutorHttpHandler extends AbstractExploreQueryExecutorHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(NamespacedExploreQueryExecutorHttpHandler.class);

  private final ExploreService exploreService;
  private final Impersonator impersonator;

  @Inject
  public NamespacedExploreQueryExecutorHttpHandler(ExploreService exploreService, Impersonator impersonator) {
    this.exploreService = exploreService;
    this.impersonator = impersonator;
  }

  @POST
  @Path("data/explore/queries")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void query(HttpRequest request, HttpResponder responder,
                    @PathParam("namespace-id") final String namespaceId) throws Exception {
    try {
      Map<String, String> args = decodeArguments(request);
      final String query = args.get("query");
      final Map<String, String> additionalSessionConf = new HashMap<>(args);
      additionalSessionConf.remove("query");
      LOG.trace("Received query: {}", query);
      QueryHandle queryHandle = impersonator.doAs(new NamespaceId(namespaceId), new Callable<QueryHandle>() {
        @Override
        public QueryHandle call() throws Exception {
          return exploreService.execute(new NamespaceId(namespaceId), query, additionalSessionConf);
        }
      }, ImpersonatedOpType.EXPLORE);
      responder.sendJson(HttpResponseStatus.OK, queryHandle);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, String.format("[SQLState %s] %s",
                                                                         e.getSQLState(), e.getMessage()));
    }
  }

  @GET
  @Path("data/explore/queries")
  public void getQueryLiveHandles(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId,
                                  @QueryParam("offset") @DefaultValue("9223372036854775807") long offset,
                                  @QueryParam("cursor") @DefaultValue("next") String cursor,
                                  @QueryParam("limit") @DefaultValue("50") int limit)
    throws ExploreException, SQLException {
    boolean isForward = "next".equals(cursor);

    // this operation doesn't interact with hive, so doesn't require impersonation
    List<QueryInfo> queries = exploreService.getQueries(new NamespaceId(namespaceId));
    // return the queries by after filtering (> offset) and limiting number of queries
    responder.sendJson(HttpResponseStatus.OK, filterQueries(queries, offset, isForward, limit));
  }

  @GET
  @Path("data/explore/queries/count")
  public void getActiveQueryCount(HttpRequest request, HttpResponder responder,
                                  @PathParam("namespace-id") String namespaceId) throws ExploreException {
    // this operation doesn't interact with hive, so doesn't require impersonation
    int count = exploreService.getActiveQueryCount(new NamespaceId(namespaceId));
    responder.sendJson(HttpResponseStatus.OK, ImmutableMap.of("count", count));
  }
}
