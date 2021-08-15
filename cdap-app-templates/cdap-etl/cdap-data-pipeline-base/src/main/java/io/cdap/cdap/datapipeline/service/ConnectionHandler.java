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

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.ServicePluginConfigurer;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.datapipeline.connection.ConnectionStore;
import io.cdap.cdap.datapipeline.connection.DefaultConnectorConfigurer;
import io.cdap.cdap.datapipeline.connection.DefaultConnectorContext;
import io.cdap.cdap.datapipeline.connection.LimitingConnector;
import io.cdap.cdap.etl.api.batch.BatchConnector;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorConfigurer;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.common.ArtifactSelectorProvider;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.OAuthMacroEvaluator;
import io.cdap.cdap.etl.common.SecureStoreMacroEvaluator;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.etl.proto.connection.ConnectionBadRequestException;
import io.cdap.cdap.etl.proto.connection.ConnectionCreationRequest;
import io.cdap.cdap.etl.proto.connection.ConnectionId;
import io.cdap.cdap.etl.proto.connection.ConnectorDetail;
import io.cdap.cdap.etl.proto.connection.PluginDetail;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.etl.proto.connection.SampleResponse;
import io.cdap.cdap.etl.proto.connection.SampleResponseCodec;
import io.cdap.cdap.etl.proto.connection.SpecGenerationRequest;
import io.cdap.cdap.etl.proto.v2.ConnectionConfig;
import io.cdap.cdap.etl.proto.validation.SimpleFailureCollector;
import io.cdap.cdap.etl.spec.TrackedPluginSelector;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.NamespaceId;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
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
  private static final Type SET_STRING_TYPE = new TypeToken<Set<String>>() { }.getType();

  private static final String DISABLED_TYPES = "disabledTypes";
  private ConnectionStore store;
  private ConnectionConfig connectionConfig;
  private Set<String> disabledTypes;

  public ConnectionHandler(@Nullable ConnectionConfig connectionConfig) {
    this.connectionConfig = connectionConfig;
  }

  @Override
  protected void configure() {
    Set<String> disabledTypes = connectionConfig == null ? Collections.emptySet() : connectionConfig.getDisabledTypes();
    setProperties(Collections.singletonMap(DISABLED_TYPES, GSON.toJson(disabledTypes)));
  }

  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    store = new ConnectionStore(context);
    String disabledTypesStr = context.getSpecification().getProperty(DISABLED_TYPES);
    this.disabledTypes = GSON.fromJson(disabledTypesStr, SET_STRING_TYPE);
  }

  /**
   * Returns the list of connections in the given namespace
   */
  @GET
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
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
      String connType = creationRequest.getPlugin().getName();
      if (disabledTypes.contains(connType)) {
        throw new ConnectionBadRequestException(
          String.format("Connection type %s is disabled, connection cannot be created", connType));
      }
      ConnectionId connectionId = new ConnectionId(namespaceSummary, connection);

      long now = System.currentTimeMillis();
      Connection connectionInfo = new Connection(connection, connectionId.getConnectionId(),
                                                 connType,
                                                 creationRequest.getDescription(), false, false,
                                                 now, now, creationRequest.getPlugin());
      store.saveConnection(connectionId, connectionInfo, false);
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  /**
   * Delete a connection in the given namespace
   */
  @DELETE
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
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

      ServicePluginConfigurer pluginConfigurer =
        getContext().createServicePluginConfigurer(namespaceSummary.getName());
      ConnectorConfigurer connectorConfigurer = new DefaultConnectorConfigurer(pluginConfigurer);
      SimpleFailureCollector failureCollector = new SimpleFailureCollector();
      ConnectorContext connectorContext = new DefaultConnectorContext(failureCollector, pluginConfigurer);
      TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
        new ArtifactSelectorProvider().getPluginSelector(creationRequest.getPlugin().getArtifact()));
      try (Connector connector = getConnector(pluginConfigurer, creationRequest.getPlugin(),
                                              namespaceSummary.getName(), pluginSelector)) {
        connector.configure(connectorConfigurer);
        try {
          connector.test(connectorContext);
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
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

      ServicePluginConfigurer pluginConfigurer =
        getContext().createServicePluginConfigurer(namespaceSummary.getName());
      ConnectorConfigurer connectorConfigurer = new DefaultConnectorConfigurer(pluginConfigurer);
      ConnectorContext connectorContext = new DefaultConnectorContext(new SimpleFailureCollector(), pluginConfigurer);
      Connection conn = store.getConnection(new ConnectionId(namespaceSummary, connection));

      TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
        new ArtifactSelectorProvider().getPluginSelector(conn.getPlugin().getArtifact()));
      try (Connector connector = getConnector(pluginConfigurer, conn.getPlugin(), namespaceSummary.getName(),
                                              pluginSelector)) {
        connector.configure(connectorConfigurer);
        responder.sendJson(connector.browse(connectorContext, browseRequest));
      }
    });
  }

  /**
   * Retrive sample result for the connection
   */
  @POST
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
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

      ServicePluginConfigurer pluginConfigurer =
        getContext().createServicePluginConfigurer(namespaceSummary.getName());
      ConnectorConfigurer connectorConfigurer = new DefaultConnectorConfigurer(pluginConfigurer);
      ConnectorContext connectorContext = new DefaultConnectorContext(new SimpleFailureCollector(), pluginConfigurer);
      Connection conn = store.getConnection(new ConnectionId(namespaceSummary, connection));

      PluginInfo plugin = conn.getPlugin();
      // use tracked selector to get exact plugin version that gets selected since the passed version can be null
      TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
        new ArtifactSelectorProvider().getPluginSelector(plugin.getArtifact()));
      try (Connector connector = getConnector(pluginConfigurer, plugin, namespaceSummary.getName(), pluginSelector)) {
        connector.configure(connectorConfigurer);
        ConnectorSpecRequest specRequest = ConnectorSpecRequest.builder().setPath(sampleRequest.getPath())
                                             .setConnection(connection)
                                             .setProperties(sampleRequest.getProperties()).build();
        ConnectorSpec spec = connector.generateSpec(connectorContext, specRequest);
        ConnectorDetail detail = getConnectorDetail(pluginSelector.getSelectedArtifact(), spec);

        if (connector instanceof DirectConnector) {
          DirectConnector directConnector = (DirectConnector) connector;
          List<StructuredRecord> sample = directConnector.sample(connectorContext, sampleRequest);
          responder.sendString(GSON.toJson(
            new SampleResponse(detail, sample.isEmpty() ? null : sample.get(0).getSchema(), sample)));
          return;
        }
        if (connector instanceof BatchConnector) {
          LimitingConnector limitingConnector = new LimitingConnector((BatchConnector) connector, pluginConfigurer);
          List<StructuredRecord> sample = limitingConnector.sample(connectorContext, sampleRequest);
          responder.sendString(GSON.toJson(
            new SampleResponse(detail, sample.isEmpty() ? null : sample.get(0).getSchema(), sample)));
          return;
        }
        // should not happen
        responder.sendError(
          HttpURLConnection.HTTP_BAD_REQUEST,
          "Connector is not supported. The supported connector should be DirectConnector or BatchConnector.");
      }
    });
  }

  /**
   * Retrieve the spec for the connector, which can be used in a source/sink
   */
  @POST
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path(API_VERSION + "/contexts/{context}/connections/{connection}/specification")
  public void spec(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("context") String namespace,
                   @PathParam("connection") String connection) {
    respond(namespace, responder, namespaceSummary -> {
      if (namespaceSummary.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST,
                            "Generating connector spec in system namespace is currently not supported");
        return;
      }

      SpecGenerationRequest specRequest =
        GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), SpecGenerationRequest.class);

      if (specRequest == null) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "The request body is empty");
        return;
      }

      if (specRequest.getPath() == null) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Path is not provided in the sample request");
        return;
      }

      ServicePluginConfigurer pluginConfigurer =
        getContext().createServicePluginConfigurer(namespaceSummary.getName());
      ConnectorConfigurer connectorConfigurer = new DefaultConnectorConfigurer(pluginConfigurer);
      ConnectorContext connectorContext = new DefaultConnectorContext(new SimpleFailureCollector(), pluginConfigurer);
      Connection conn = store.getConnection(new ConnectionId(namespaceSummary, connection));

      // use tracked selector to get exact plugin version that gets selected since the passed version can be null
      TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
        new ArtifactSelectorProvider().getPluginSelector(conn.getPlugin().getArtifact()));
      try (Connector connector = getConnector(pluginConfigurer, conn.getPlugin(), namespaceSummary.getName(),
                                              pluginSelector)) {
        connector.configure(connectorConfigurer);
        ConnectorSpecRequest connectorSpecRequest = ConnectorSpecRequest.builder().setPath(specRequest.getPath())
                                                      .setConnection(connection)
                                                      .setProperties(specRequest.getProperties()).build();
        ConnectorSpec spec = connector.generateSpec(connectorContext, connectorSpecRequest);
        responder.sendString(GSON.toJson(getConnectorDetail(pluginSelector.getSelectedArtifact(), spec)));
      }
    });
  }

  private ConnectorDetail getConnectorDetail(ArtifactId artifactId, ConnectorSpec spec) {
    ArtifactSelectorConfig artifact = new ArtifactSelectorConfig(artifactId.getScope().name(),
                                                                 artifactId.getName(),
                                                                 artifactId.getVersion().getVersion());
    Set<PluginDetail> relatedPlugins = new HashSet<>();
    spec.getRelatedPlugins().forEach(pluginSpec -> relatedPlugins.add(
      new PluginDetail(pluginSpec.getName(), pluginSpec.getType(), pluginSpec.getProperties(), artifact,
                       spec.getSchema())));
    return new ConnectorDetail(relatedPlugins);
  }

  private Connector getConnector(ServicePluginConfigurer configurer, PluginInfo pluginInfo,
                                 String namespace, TrackedPluginSelector pluginSelector) throws IOException {

    Map<String, String> arguments = getContext().getPreferencesForNamespace(namespace, true);
    Map<String, MacroEvaluator> evaluators = ImmutableMap.of(
      SecureStoreMacroEvaluator.FUNCTION_NAME, new SecureStoreMacroEvaluator(namespace, getContext()),
      OAuthMacroEvaluator.FUNCTION_NAME, new OAuthMacroEvaluator(getContext())
    );
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(new BasicArguments(arguments), evaluators,
                                                              Collections.singleton(OAuthMacroEvaluator.FUNCTION_NAME));
    MacroParserOptions options = MacroParserOptions.builder()
                                 .skipInvalidMacros()
                                 .setEscaping(false)
                                 .setFunctionWhitelist(evaluators.keySet())
                                 .build();
    Connector connector;
    try {
      connector = configurer.usePlugin(pluginInfo.getType(), pluginInfo.getName(), UUID.randomUUID().toString(),
                                       PluginProperties.builder().addAll(pluginInfo.getProperties()).build(),
                                       pluginSelector, macroEvaluator, options);
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
