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

package co.cask.cdap.explore.executor;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.ExploreService;
import co.cask.cdap.explore.service.MetaDataInfo;
import co.cask.cdap.explore.service.TableNotFoundException;
import co.cask.cdap.explore.utils.ColumnsArgs;
import co.cask.cdap.explore.utils.FunctionsArgs;
import co.cask.cdap.explore.utils.SchemasArgs;
import co.cask.cdap.explore.utils.TablesArgs;
import co.cask.cdap.proto.QueryHandle;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.SQLException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Handler that implements explore metadata APIs.
 */
@Path(Constants.Gateway.API_VERSION_2)
public class ExploreMetadataHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreMetadataHttpHandler.class);
  private static final Gson GSON = new Gson();

  private final ExploreService exploreService;

  @Inject
  public ExploreMetadataHttpHandler(ExploreService exploreService) {
    this.exploreService = exploreService;
  }

  @GET
  @Path("data/explore/tables")
  public void getTables(HttpRequest request, HttpResponder responder, @QueryParam("db") String databaseName) {
    LOG.trace("Received get tables for current user");
    try {
      responder.sendJson(HttpResponseStatus.OK, exploreService.getTables(databaseName));
    } catch (ExploreException e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @GET
  @Path("data/explore/tables/{table}/info")
  public void getTableSchema(HttpRequest request, HttpResponder responder, @PathParam("table") final String table) {
    LOG.trace("Received get table info for table {}", table);
    try {
      int dbSepIdx = table.indexOf('.');
      String dbName = null;
      String tableName = table;
      if (dbSepIdx >= 0) {
        dbName = table.substring(0, dbSepIdx);
        tableName = table.substring(dbSepIdx + 1);
      }
      responder.sendJson(HttpResponseStatus.OK, exploreService.getTableInfo(dbName, tableName));
    } catch (TableNotFoundException e) {
      LOG.error("Could not find table {}", table, e);
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (ExploreException e) {
      LOG.error("Got exception:", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }


  @POST
  @Path("data/explore/jdbc/tables")
  public void getJDBCTables(HttpRequest request, HttpResponder responder) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        TablesArgs args = decodeArguments(request, TablesArgs.class, new TablesArgs(null, null, "%", null));
        LOG.trace("Received get tables with params: {}", args.toString());
        return exploreService.getTables(args.getCatalog(), args.getSchemaPattern(),
                                        args.getTableNamePattern(), args.getTableTypes());
      }
    });
  }

  @POST
  @Path("data/explore/jdbc/columns")
  public void getJDBCColumns(HttpRequest request, HttpResponder responder) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        ColumnsArgs args = decodeArguments(request, ColumnsArgs.class, new ColumnsArgs(null, null, "%", "%"));
        LOG.trace("Received get columns with params: {}", args.toString());
        return exploreService.getColumns(args.getCatalog(), args.getSchemaPattern(),
                                         args.getTableNamePattern(), args.getColumnNamePattern());
      }
    });
  }

  @POST
  @Path("data/explore/jdbc/catalogs")
  public void getJDBCCatalogs(HttpRequest request, HttpResponder responder) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        LOG.trace("Received get catalogs query.");
        return exploreService.getCatalogs();
      }
    });
  }

  @POST
  @Path("data/explore/jdbc/schemas")
  public void getJDBCSchemas(HttpRequest request, HttpResponder responder) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        SchemasArgs args = decodeArguments(request, SchemasArgs.class, new SchemasArgs(null, null));
        LOG.trace("Received get schemas with params: {}", args.toString());
        return exploreService.getSchemas(args.getCatalog(), args.getSchemaPattern());
      }
    });
  }

  @POST
  @Path("data/explore/jdbc/functions")
  public void getJDBCFunctions(HttpRequest request, HttpResponder responder) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        FunctionsArgs args = decodeArguments(request, FunctionsArgs.class, new FunctionsArgs(null, null, "%"));
        LOG.trace("Received get functions with params: {}", args.toString());
        return exploreService.getFunctions(args.getCatalog(), args.getSchemaPattern(),
                                           args.getFunctionNamePattern());
      }
    });
  }

  @POST
  @Path("data/explore/jdbc/tableTypes")
  public void getJDBCTableTypes(HttpRequest request, HttpResponder responder) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        LOG.trace("Received get table types query.");
        return exploreService.getTableTypes();
      }
    });
  }

  @POST
  @Path("data/explore/jdbc/types")
  public void getJDBCTypes(HttpRequest request, HttpResponder responder) {
    handleResponseEndpointExecution(request, responder, new EndpointCoreExecution<QueryHandle>() {
      @Override
      public QueryHandle execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        LOG.trace("Received get type info query.");
        return exploreService.getTypeInfo();
      }
    });
  }

  @GET
  @Path("data/explore/jdbc/info/{type}")
  public void getJDBCInfo(HttpRequest request, HttpResponder responder, @PathParam("type") final String type) {
    genericEndpointExecution(request, responder, new EndpointCoreExecution<Void>() {
      @Override
      public Void execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        LOG.trace("Received get info for {}", type);
        MetaDataInfo.InfoType infoType = MetaDataInfo.InfoType.fromString(type);
        MetaDataInfo metadataInfo = exploreService.getInfo(infoType);
        responder.sendJson(HttpResponseStatus.OK, metadataInfo);
        return null;
      }
    });
  }

  private void handleResponseEndpointExecution(HttpRequest request, HttpResponder responder,
                                               final EndpointCoreExecution<QueryHandle> execution) {
    genericEndpointExecution(request, responder, new EndpointCoreExecution<Void>() {
      @Override
      public Void execute(HttpRequest request, HttpResponder responder)
        throws IllegalArgumentException, SQLException, ExploreException, IOException {
        QueryHandle handle = execution.execute(request, responder);
        JsonObject json = new JsonObject();
        json.addProperty("handle", handle.getHandle());
        responder.sendJson(HttpResponseStatus.OK, json);
        return null;
      }
    });
  }

  private void genericEndpointExecution(HttpRequest request, HttpResponder responder,
                                        EndpointCoreExecution<Void> execution) {
    try {
      execution.execute(request, responder);
    } catch (IllegalArgumentException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (SQLException e) {
      LOG.debug("Got exception:", e);
      responder.sendError(HttpResponseStatus.BAD_REQUEST,
                          String.format("[SQLState %s] %s", e.getSQLState(), e.getMessage()));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private <T> T decodeArguments(HttpRequest request, Class<T> argsType, T defaultValue) throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      return defaultValue;
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      T args = GSON.fromJson(reader, argsType);
      return (args == null) ? defaultValue : args;
    } catch (JsonSyntaxException e) {
      LOG.info("Failed to parse runtime arguments on {}", request.getUri(), e);
      throw e;
    } finally {
      reader.close();
    }
  }

  /**
   * Represents the core execution of an endpoint.
   */
  private static interface EndpointCoreExecution<T> {
    T execute(HttpRequest request, HttpResponder responder)
      throws IllegalArgumentException, SQLException, ExploreException, IOException;
  }
}
