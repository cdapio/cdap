/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.datapipeline.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.datapipeline.connection.ConnectionStore;
import io.cdap.cdap.datapipeline.connection.InvalidConnectorException;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.etl.proto.connection.ConnectionCreationRequest;
import io.cdap.cdap.etl.proto.connection.ConnectionId;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.proto.id.NamespaceId;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Handler for all the connection operations
 */
public class ConnectionHandler extends AbstractDataPipelineHandler {
  private static final String API_VERSION = "v1";
  private static final Gson GSON = new GsonBuilder()
    .setPrettyPrinting()
    .create();
  private ConnectionStore store;

  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    store = new ConnectionStore(context);
  }

  /**
   * Returns the list of connections in the given namespace
   */
  @GET
  @Path(API_VERSION + "/contexts/{context}/connections")
  public void listConnections(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("context") String namespace) {
    respond(namespace, responder, namespaceSummary -> {
      if (namespaceSummary.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                            "Listing connections in system namespace is currently not supported");
        return;
      }
      responder.sendJson(store.listConnections(namespaceSummary));
    });
  }

  /**
   * Returns the specific connection information in the given namespace
   */
  @GET
  @Path(API_VERSION + "/contexts/{context}/connections/{connection}")
  public void getConnection(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace,
                            @PathParam("connection") String connection) {
    respond(namespace, responder, namespaceSummary -> {
      if (namespaceSummary.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                            "Getting connection in system namespace is currently not supported");
        return;
      }
      responder.sendJson(store.getConnection(new ConnectionId(namespaceSummary, connection)));
    });
  }

  /**
   * Creates a connection in the given namespace
   */
  @PUT
  @Path(API_VERSION + "/contexts/{context}/connections/{connection}")
  public void createConnection(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("context") String namespace,
                               @PathParam("connection") String connection) {
    respond(namespace, responder, namespaceSummary -> {
      if (namespaceSummary.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                            "Creating connection in system namespace is currently not supported");
        return;
      }

      ConnectionCreationRequest creationRequest =
        GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), ConnectionCreationRequest.class);

      // validate connector exists and can get instantiated
      getConnector(getContext().createPluginConfigurer(namespaceSummary.getName()), creationRequest.getPlugin());

      long now = System.currentTimeMillis();
      Connection connectionInfo = new Connection(connection, creationRequest.getPlugin().getName(),
                                                 creationRequest.getDescription(), false,
                                                 now, now, creationRequest.getPlugin());
      store.saveConnection(new ConnectionId(namespaceSummary, connection), connectionInfo);
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  /**
   * Delete a connection in the given namespace
   */
  @DELETE
  @Path(API_VERSION + "/contexts/{context}/connections/{connection}")
  public void deleteConnection(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("context") String namespace,
                               @PathParam("connection") String connection) {
    respond(namespace, responder, namespaceSummary -> {
      if (namespaceSummary.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                            "Deleting connection in system namespace is currently not supported");
        return;
      }

      store.deleteConnection(new ConnectionId(namespaceSummary, connection));
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  /**
   * Explore the connection on a given path
   */
  @GET
  @Path(API_VERSION + "/contexts/{context}/connections/{connection}/browse")
  public void browse(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace,
                     @PathParam("connection") String connection,
                     @QueryParam("path") String path,
                     @QueryParam("limit") @DefaultValue("1000") int limit,
                     @QueryParam("findAll") @DefaultValue("false") boolean findAll) {
    respond(namespace, responder, namespaceSummary -> {
      if (namespaceSummary.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                            "Browsing connection in system namespace is currently not supported");
        return;
      }

      if (path == null) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Path is not provided in the browse request");
      }

      PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceSummary.getName());
      Connection conn = store.getConnection(new ConnectionId(namespaceSummary, connection));

      try (Connector connector = getConnector(pluginConfigurer, conn.getPlugin())) {
        responder.sendJson(connector.browse(BrowseRequest.builder(path).setLimit(findAll ? null : limit).build()));
      }
    });
  }

  private Connector getConnector(PluginConfigurer configurer, PluginInfo pluginInfo) {
    Connector connector;

    try {
      connector = configurer.usePlugin(pluginInfo.getType(), pluginInfo.getName(), UUID.randomUUID().toString(),
                                       PluginProperties.builder().addAll(pluginInfo.getProperties()).build());
    } catch (InvalidPluginConfigException e) {
      throw new InvalidConnectorException(String.format("Unable to instantiate source plugin: %s", e.getMessage()), e);
    }

    if (connector == null) {
      throw new InvalidConnectorException(String.format("Unable to find connector '%s'", pluginInfo.getName()));
    }
    return connector;
  }
}
