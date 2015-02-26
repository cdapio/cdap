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
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.ExploreService;
import co.cask.cdap.explore.service.TableNotFoundException;
import co.cask.cdap.explore.utils.ColumnsArgs;
import co.cask.cdap.explore.utils.FunctionsArgs;
import co.cask.cdap.explore.utils.SchemasArgs;
import co.cask.cdap.explore.utils.TablesArgs;
import co.cask.cdap.proto.QueryHandle;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that implements namespaced explore metadata APIs.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/data/explore")
public class NamespacedExploreMetadataHttpHandler extends AbstractExploreMetadataHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(NamespacedExploreMetadataHttpHandler.class);

  private final ExploreService exploreService;

  @Inject
  public NamespacedExploreMetadataHttpHandler(ExploreService exploreService) {
    this.exploreService = exploreService;
  }

  @GET
  @Path("tables")
  public void getTables(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId) {
    LOG.trace("Received get tables for current user");
    try {
      responder.sendJson(HttpResponseStatus.OK, exploreService.getTables(namespaceId));
    } catch (Throwable t) {
      LOG.error("Got exception:", t);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage());
    }
  }

  @GET
  @Path("tables/{table}/info")
  public void getTableSchema(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId, @PathParam("table") String table) {
    LOG.trace("Received get table info for table {}", table);
    try {
      responder.sendJson(HttpResponseStatus.OK, exploreService.getTableInfo(namespaceId, table));
    } catch (TableNotFoundException e) {
      LOG.error("Could not find table {}", table, e);
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Throwable t) {
      LOG.error("Got exception:", t);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage());
    }
  }

  @POST
  @Path("jdbc/tables")
  public void getJDBCTables(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") final String namespaceId) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        TablesArgs args = decodeArguments(request, TablesArgs.class, new TablesArgs(null, namespaceId, "%", null));
        LOG.trace("Received get tables with params: {}", args.toString());
        return exploreService.getTables(args.getCatalog(), args.getSchemaPattern(),
                                        args.getTableNamePattern(), args.getTableTypes());
      }
    });
  }

  @POST
  @Path("jdbc/columns")
  public void getJDBCColumns(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") final String namespaceId) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        ColumnsArgs args = decodeArguments(request, ColumnsArgs.class, new ColumnsArgs(null, namespaceId, "%", "%"));
        LOG.trace("Received get columns with params: {}", args.toString());
        return exploreService.getColumns(args.getCatalog(), args.getSchemaPattern(),
                                         args.getTableNamePattern(), args.getColumnNamePattern());
      }
    });
  }

  @POST
  @Path("jdbc/schemas")
  public void getJDBCSchemas(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") final String namespaceId) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        SchemasArgs args = decodeArguments(request, SchemasArgs.class, new SchemasArgs(null, namespaceId));
        LOG.trace("Received get schemas with params: {}", args.toString());
        return exploreService.getSchemas(args.getCatalog(), args.getSchemaPattern());
      }
    });
  }

  @POST
  @Path("jdbc/functions")
  public void getJDBCFunctions(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") final String namespaceId) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        FunctionsArgs args = decodeArguments(request, FunctionsArgs.class, new FunctionsArgs(null, namespaceId, "%"));
        LOG.trace("Received get functions with params: {}", args.toString());
        return exploreService.getFunctions(args.getCatalog(), args.getSchemaPattern(),
                                           args.getFunctionNamePattern());
      }
    });
  }
}
