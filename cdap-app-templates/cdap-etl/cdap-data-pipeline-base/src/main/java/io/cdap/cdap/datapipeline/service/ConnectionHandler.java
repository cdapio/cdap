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
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.datapipeline.connection.ConnectionStore;
import io.cdap.cdap.datapipeline.connection.LimitingConnector;
import io.cdap.cdap.etl.api.batch.BatchConnector;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.etl.proto.connection.ConnectionBadRequestException;
import io.cdap.cdap.etl.proto.connection.ConnectionCreationRequest;
import io.cdap.cdap.etl.proto.connection.ConnectionId;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.etl.proto.connection.SampleResponse;
import io.cdap.cdap.etl.proto.connection.SampleResponseCodec;
import io.cdap.cdap.etl.validation.SimpleFailureCollector;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.NamespaceId;

import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler for all the connection operations
 */
public class ConnectionHandler extends AbstractDataPipelineHandler {
  private static final String API_VERSION = "v1";
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(SampleResponse.class, new SampleResponseCodec()).setPrettyPrinting().create();
  private static final Type MAP_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
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
      ConnectionId connectionId = new ConnectionId(namespaceSummary, connection);

      long now = System.currentTimeMillis();
      Connection connectionInfo = new Connection(connection, connectionId.getConnectionId(),
                                                 creationRequest.getPlugin().getName(),
                                                 creationRequest.getDescription(), false, false,
                                                 now, now, creationRequest.getPlugin());
      store.saveConnection(connectionId, connectionInfo, creationRequest.overWrite());
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

  @POST
  @Path(API_VERSION + "/contexts/{context}/connections/test")
  public void testConnection(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("context") String namespace) {
    respond(namespace, responder, namespaceSummary -> {
      if (namespaceSummary.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                            "Creating connection in system namespace is currently not supported");
        return;
      }

      ConnectionCreationRequest creationRequest =
        GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), ConnectionCreationRequest.class);

      PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceSummary.getName());
      try (Connector connector = getConnector(pluginConfigurer, creationRequest.getPlugin())) {
        connector.configure(pluginConfigurer);
        SimpleFailureCollector failureCollector = new SimpleFailureCollector();
        try {
          connector.test(failureCollector);
          failureCollector.getOrThrowException();
        } catch (ValidationException e) {
          responder.sendJson(e.getFailures());
          return;
        }
      }

      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  /**
   * Browse the connection on a given path.
   */
  @POST
  @Path(API_VERSION + "/contexts/{context}/connections/{connection}/browse")
  public void browse(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace,
                     @PathParam("connection") String connection) {
    respond(namespace, responder, namespaceSummary -> {
      if (namespaceSummary.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                            "Browsing connection in system namespace is currently not supported");
        return;
      }

      BrowseRequest browseRequest =
        GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), BrowseRequest.class);

      if (browseRequest == null) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "The request body is empty");
        return;
      }

      if (browseRequest.getPath() == null) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Path is not provided in the browse request");
        return;
      }

      PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceSummary.getName());
      Connection conn = store.getConnection(new ConnectionId(namespaceSummary, connection));

      try (Connector connector = getConnector(pluginConfigurer, conn.getPlugin())) {
        connector.configure(pluginConfigurer);
        responder.sendJson(connector.browse(browseRequest));
      }
    });
  }

  /**
   * Retrive sample result for the connection
   */
  @POST
  @Path(API_VERSION + "/contexts/{context}/connections/{connection}/sample")
  public void sample(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace,
                     @PathParam("connection") String connection) {
    respond(namespace, responder, namespaceSummary -> {
      if (namespaceSummary.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                            "Sampling connection in system namespace is currently not supported");
        return;
      }

      SampleRequest sampleRequest =
        GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), SampleRequest.class);

      if (sampleRequest == null) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "The request body is empty");
        return;
      }

      if (sampleRequest.getPath() == null) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Path is not provided in the sample request");
        return;
      }

      if (sampleRequest.getLimit() <= 0) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Limit should be greater than 0");
        return;
      }

      PluginConfigurer pluginConfigurer = getContext().createPluginConfigurer(namespaceSummary.getName());
      Connection conn = store.getConnection(new ConnectionId(namespaceSummary, connection));

      try (Connector connector = getConnector(pluginConfigurer, conn.getPlugin())) {
        connector.configure(pluginConfigurer);
        ConnectorSpec spec = connector.generateSpec(sampleRequest.getPath());
        ConnectorSpec connectorSpec = ConnectorSpec.builder().setProperties(sampleRequest.getProperties())
                                        .addProperties(spec.getProperties()).build();
        if (connector instanceof DirectConnector) {
          DirectConnector directConnector = (DirectConnector) connector;
          List<StructuredRecord> sample = directConnector.sample(sampleRequest);
          responder.sendString(GSON.toJson(
            new SampleResponse(connectorSpec, sample.isEmpty() ? null : sample.get(0).getSchema(), sample)));
          return;
        }
        if (connector instanceof BatchConnector) {
          LimitingConnector limitingConnector = new LimitingConnector((BatchConnector) connector);
          List<StructuredRecord> sample = limitingConnector.sample(sampleRequest);
          responder.sendString(GSON.toJson(
            new SampleResponse(connectorSpec, sample.isEmpty() ? null : sample.get(0).getSchema(), sample)));
          return;
        }
        // should not happen
        responder.sendError(
          HttpURLConnection.HTTP_BAD_REQUEST,
          "Connector is not supported. The supported connector should be DirectConnector or BatchConnector.");
      }
    });
  }

  private Connector getConnector(PluginConfigurer configurer, PluginInfo pluginInfo) {
    Connector connector;

    try {
      connector = configurer.usePlugin(pluginInfo.getType(), pluginInfo.getName(), UUID.randomUUID().toString(),
                                       PluginProperties.builder().addAll(pluginInfo.getProperties()).build());
    } catch (InvalidPluginConfigException e) {
      throw new ConnectionBadRequestException(
        String.format("Unable to instantiate connector plugin: %s", e.getMessage()), e);
    }

    if (connector == null) {
      throw new ConnectionBadRequestException(String.format("Unable to find connector '%s'", pluginInfo.getName()));
    }
    return connector;
  }
}
