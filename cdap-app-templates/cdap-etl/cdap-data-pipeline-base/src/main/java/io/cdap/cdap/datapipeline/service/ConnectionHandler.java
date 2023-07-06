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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.ServicePluginConfigurer;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.api.service.worker.RemoteTaskException;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.datapipeline.connection.ConnectionStore;
import io.cdap.cdap.datapipeline.connection.DefaultConnectorConfigurer;
import io.cdap.cdap.datapipeline.connection.DefaultConnectorContext;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorConfigurer;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.common.ArtifactSelectorProvider;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.OAuthAccessTokenMacroEvaluator;
import io.cdap.cdap.etl.common.OAuthMacroEvaluator;
import io.cdap.cdap.etl.common.SecureStoreMacroEvaluator;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.etl.proto.connection.ConnectionBadRequestException;
import io.cdap.cdap.etl.proto.connection.ConnectionCreationRequest;
import io.cdap.cdap.etl.proto.connection.ConnectionId;
import io.cdap.cdap.etl.proto.connection.ConnectionNotFoundException;
import io.cdap.cdap.etl.proto.connection.ConnectorDetail;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.etl.proto.connection.SampleResponse;
import io.cdap.cdap.etl.proto.connection.SampleResponseCodec;
import io.cdap.cdap.etl.proto.connection.SpecGenerationRequest;
import io.cdap.cdap.etl.proto.v2.ConnectionConfig;
import io.cdap.cdap.etl.proto.validation.SimpleFailureCollector;
import io.cdap.cdap.etl.spec.TrackedPluginSelector;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ConnectionEntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
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
  private ContextAccessEnforcer contextAccessEnforcer;

  // Injected by CDAP
  @SuppressWarnings("unused")
  private Metrics metrics;

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
    contextAccessEnforcer = context.getContextAccessEnforcer();
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

      contextAccessEnforcer.enforceOnParent(EntityType.SYSTEM_APP_ENTITY, new NamespaceId(namespace),
                                            StandardPermission.LIST);
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

      contextAccessEnforcer.enforce(new ConnectionEntityId(namespace, ConnectionId.getConnectionId(connection)),
                                    StandardPermission.GET);
      Connection conn = store.getConnection(new ConnectionId(namespaceSummary, connection));
      Metrics child = metrics.child(ImmutableMap.of(Constants.Metrics.Tag.APP_ENTITY_TYPE,
                                                    Constants.CONNECTION_SERVICE_NAME,
                                                    Constants.Metrics.Tag.APP_ENTITY_TYPE_NAME,
                                                    conn.getConnectionType()));
      child.count(Constants.Metrics.Connection.CONNECTION_GET_COUNT, 1);
      responder.sendJson(conn);
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

      ConnectionId connectionId = new ConnectionId(namespaceSummary, connection);
      checkPutConnectionPermissions(connectionId);

      ConnectionCreationRequest creationRequest =
        GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), ConnectionCreationRequest.class);
      String connType = creationRequest.getPlugin().getName();
      if (disabledTypes.contains(connType)) {
        throw new ConnectionBadRequestException(
          String.format("Connection type %s is disabled, connection cannot be created", connType));
      }

      long now = System.currentTimeMillis();
      Connection connectionInfo = new Connection(connection, connectionId.getConnectionId(),
                                                 connType,
                                                 creationRequest.getDescription(), false, false,
                                                 now, now, creationRequest.getPlugin());
      store.saveConnection(connectionId, connectionInfo, creationRequest.overWrite());
      Metrics child = metrics.child(ImmutableMap.of(Constants.Metrics.Tag.APP_ENTITY_TYPE,
                                                    Constants.CONNECTION_SERVICE_NAME,
                                                    Constants.Metrics.Tag.APP_ENTITY_TYPE_NAME,
                                                    connectionInfo.getConnectionType()));
      child.count(Constants.Metrics.Connection.CONNECTION_COUNT, 1);
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
      ConnectionId connectionId = new ConnectionId(namespaceSummary, connection);
      contextAccessEnforcer.enforce(new ConnectionEntityId(namespace, connectionId.getConnectionId()),
                                    StandardPermission.DELETE);
      Connection oldConnection = store.getConnection(connectionId);
      store.deleteConnection(connectionId);
      Metrics child = metrics.child(ImmutableMap.of(Constants.Metrics.Tag.APP_ENTITY_TYPE,
                                                    Constants.CONNECTION_SERVICE_NAME,
                                                    Constants.Metrics.Tag.APP_ENTITY_TYPE_NAME,
                                                    oldConnection.getConnectionType()));
      child.count(Constants.Metrics.Connection.CONNECTION_DELETED_COUNT, 1);
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

      String testRequestString = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      ConnectionCreationRequest testRequest = GSON.fromJson(testRequestString, ConnectionCreationRequest.class);

      if (getContext().isRemoteTaskEnabled()) {
        executeRemotely(namespace, testRequestString, null, RemoteConnectionTestTask.class, responder);
      } else {
        testLocally(namespaceSummary.getName(), testRequest, responder);
      }
    });
  }

  private void testLocally(String namespace, ConnectionCreationRequest creationRequest,
                           HttpServiceResponder responder) throws IOException {
    ServicePluginConfigurer pluginConfigurer =
      getContext().createServicePluginConfigurer(namespace);
    ConnectorConfigurer connectorConfigurer = new DefaultConnectorConfigurer(pluginConfigurer);
    SimpleFailureCollector failureCollector = new SimpleFailureCollector();
    ConnectorContext connectorContext = new DefaultConnectorContext(failureCollector, pluginConfigurer);
    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider().getPluginSelector(creationRequest.getPlugin().getArtifact()));
    try (Connector connector = getConnector(pluginConfigurer, creationRequest.getPlugin(),
                                            namespace, pluginSelector)) {
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

      contextAccessEnforcer.enforce(new ConnectionEntityId(namespace, ConnectionId.getConnectionId(connection)),
                                    StandardPermission.USE);
      String browseRequestString = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      BrowseRequest browseRequest = GSON.fromJson(browseRequestString, BrowseRequest.class);

      if (browseRequest == null) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "The request body is empty");
        return;
      }

      if (browseRequest.getPath() == null) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Path is not provided in the browse request");
        return;
      }

      Connection conn = store.getConnection(new ConnectionId(namespaceSummary, connection));

      if (getContext().isRemoteTaskEnabled()) {
        executeRemotely(namespace, browseRequestString, conn, RemoteConnectionBrowseTask.class, responder);
      } else {
        browseLocally(namespaceSummary.getName(), browseRequest, conn, responder);
      }
      Metrics child = metrics.child(ImmutableMap.of(Constants.Metrics.Tag.APP_ENTITY_TYPE,
                                                    Constants.CONNECTION_SERVICE_NAME,
                                                    Constants.Metrics.Tag.APP_ENTITY_TYPE_NAME,
                                                    conn.getConnectionType()));
      child.count(Constants.Metrics.Connection.CONNECTION_BROWSE_COUNT, 1);
    });
  }

  private void browseLocally(String namespace, BrowseRequest browseRequest,
                             Connection conn, HttpServiceResponder responder) throws IOException {
    ServicePluginConfigurer pluginConfigurer =
      getContext().createServicePluginConfigurer(namespace);
    ConnectorConfigurer connectorConfigurer = new DefaultConnectorConfigurer(pluginConfigurer);
    ConnectorContext connectorContext = new DefaultConnectorContext(new SimpleFailureCollector(), pluginConfigurer);

    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider().getPluginSelector(conn.getPlugin().getArtifact()));
    try (Connector connector = getConnector(pluginConfigurer, conn.getPlugin(), namespace, pluginSelector)) {
      connector.configure(connectorConfigurer);
      responder.sendJson(connector.browse(connectorContext, browseRequest));
    }
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

      contextAccessEnforcer.enforce(new ConnectionEntityId(namespace, ConnectionId.getConnectionId(connection)),
                                    StandardPermission.USE);
      String sampleRequestString = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      SampleRequest sampleRequest = GSON.fromJson(sampleRequestString, SampleRequest.class);

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

      Connection conn = store.getConnection(new ConnectionId(namespaceSummary, connection));

      if (getContext().isRemoteTaskEnabled()) {
        executeRemotely(namespace, sampleRequestString, conn, RemoteConnectionSampleTask.class, responder);
      } else {
        sampleLocally(namespaceSummary.getName(), sampleRequestString, conn, responder);
      }
      Metrics child = metrics.child(ImmutableMap.of(Constants.Metrics.Tag.APP_ENTITY_TYPE,
                                                    Constants.CONNECTION_SERVICE_NAME,
                                                    Constants.Metrics.Tag.APP_ENTITY_TYPE_NAME,
                                                    conn.getConnectionType()));
      child.count(Constants.Metrics.Connection.CONNECTION_SAMPLE_COUNT, 1);
      // sample will also generate the spec, so add the metric for it
      child.count(Constants.Metrics.Connection.CONNECTION_SPEC_COUNT, 1);
    });
  }

  private void sampleLocally(String namespace, String sampleRequestString,
                             Connection conn, HttpServiceResponder responder) throws IOException {
    SampleRequest sampleRequest = GSON.fromJson(sampleRequestString, SampleRequest.class);

    ServicePluginConfigurer pluginConfigurer =
      getContext().createServicePluginConfigurer(namespace);
    ConnectorConfigurer connectorConfigurer = new DefaultConnectorConfigurer(pluginConfigurer);
    ConnectorContext connectorContext = new DefaultConnectorContext(new SimpleFailureCollector(), pluginConfigurer);

    PluginInfo plugin = conn.getPlugin();
    // use tracked selector to get exact plugin version that gets selected since the passed version can be null
    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider().getPluginSelector(plugin.getArtifact()));
    try (Connector connector = getConnector(pluginConfigurer, plugin, namespace, pluginSelector)) {
      connector.configure(connectorConfigurer);
      ConnectorSpecRequest specRequest = ConnectorSpecRequest.builder().setPath(sampleRequest.getPath())
        .setConnection(conn.getName())
        .setProperties(sampleRequest.getProperties()).build();
      ConnectorSpec spec = connector.generateSpec(connectorContext, specRequest);
      ConnectorDetail detail = ConnectionUtils.getConnectorDetail(pluginSelector.getSelectedArtifact(), spec);

      try {
        SampleResponse sampleResponse = ConnectionUtils.getSampleResponse(connector, connectorContext, sampleRequest,
                                                                          detail, pluginConfigurer);
        responder.sendString(GSON.toJson(sampleResponse));
      } catch (ConnectionBadRequestException e) {
        // should not happen
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
      }
    }
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

      contextAccessEnforcer.enforce(new ConnectionEntityId(namespace, ConnectionId.getConnectionId(connection)),
                                    StandardPermission.USE);
      String specGenerationRequestString = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      SpecGenerationRequest specRequest = GSON.fromJson(specGenerationRequestString, SpecGenerationRequest.class);

      if (specRequest == null) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "The request body is empty");
        return;
      }

      if (specRequest.getPath() == null) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Path is not provided in the sample request");
        return;
      }
      Connection conn = store.getConnection(new ConnectionId(namespaceSummary, connection));

      if (getContext().isRemoteTaskEnabled()) {
        executeRemotely(namespace, specGenerationRequestString, conn, RemoteConnectionSpecTask.class, responder);
      } else {
        specGenerationLocally(namespaceSummary.getName(), specRequest, conn, responder);
      }

      Metrics child = metrics.child(ImmutableMap.of(Constants.Metrics.Tag.APP_ENTITY_TYPE,
                                                    Constants.CONNECTION_SERVICE_NAME,
                                                    Constants.Metrics.Tag.APP_ENTITY_TYPE_NAME,
                                                    conn.getConnectionType()));
      child.count(Constants.Metrics.Connection.CONNECTION_SPEC_COUNT, 1);
    });
  }

  private void specGenerationLocally(String namespace, SpecGenerationRequest specRequest,
                                     Connection conn, HttpServiceResponder responder) throws IOException {
    ServicePluginConfigurer pluginConfigurer =
      getContext().createServicePluginConfigurer(namespace);
    ConnectorConfigurer connectorConfigurer = new DefaultConnectorConfigurer(pluginConfigurer);
    ConnectorContext connectorContext = new DefaultConnectorContext(new SimpleFailureCollector(), pluginConfigurer);

    // use tracked selector to get exact plugin version that gets selected since the passed version can be null
    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider().getPluginSelector(conn.getPlugin().getArtifact()));
    try (Connector connector = getConnector(pluginConfigurer, conn.getPlugin(), namespace,
                                            pluginSelector)) {
      connector.configure(connectorConfigurer);
      ConnectorSpecRequest connectorSpecRequest = ConnectorSpecRequest.builder().setPath(specRequest.getPath())
        .setConnection(conn.getName())
        .setProperties(specRequest.getProperties()).build();
      ConnectorSpec spec = connector.generateSpec(connectorContext, connectorSpecRequest);
      ConnectorSpec newSpec = ConnectionUtils.filterSpecWithPluginNameAndType(spec, specRequest.getPluginName(),
                                                                              specRequest.getPluginType());
      responder.sendString(GSON.toJson(ConnectionUtils.getConnectorDetail(pluginSelector.getSelectedArtifact(),
                                                                          newSpec)));
    }
  }

  private Connector getConnector(ServicePluginConfigurer configurer, PluginInfo pluginInfo,
                                 String namespace, TrackedPluginSelector pluginSelector) throws IOException {

    Map<String, String> arguments = getContext().getPreferencesForNamespace(namespace, true);
    Map<String, MacroEvaluator> evaluators = ImmutableMap.of(
      SecureStoreMacroEvaluator.FUNCTION_NAME, new SecureStoreMacroEvaluator(namespace, getContext()),
      OAuthMacroEvaluator.FUNCTION_NAME, new OAuthMacroEvaluator(getContext()),
      OAuthAccessTokenMacroEvaluator.FUNCTION_NAME, new OAuthAccessTokenMacroEvaluator(getContext())
    );
    MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(new BasicArguments(arguments), evaluators,
                                                              Collections.singleton(OAuthMacroEvaluator.FUNCTION_NAME));
    MacroParserOptions options = MacroParserOptions.builder()
                                 .setEscaping(false)
                                 .setFunctionWhitelist(evaluators.keySet())
                                 .build();
    return ConnectionUtils.getConnector(configurer, pluginInfo, pluginSelector, macroEvaluator, options);
  }

  private void checkPutConnectionPermissions(ConnectionId connectionId) {
    boolean connectionExists;
    try {
      store.getConnection(connectionId);
      connectionExists = true;
    } catch (ConnectionNotFoundException e) {
      connectionExists = false;
    }
    if (connectionExists) {
      contextAccessEnforcer.enforce(new ConnectionEntityId(connectionId.getNamespace().getName(),
                                                           connectionId.getConnectionId()), StandardPermission.UPDATE);
    } else {
      contextAccessEnforcer.enforce(new ConnectionEntityId(connectionId.getNamespace().getName(),
                                                           connectionId.getConnectionId()), StandardPermission.CREATE);
    }
  }

  /**
   * Common method for all remote executions.
   * Remote request is created, executed and response is added to {@link HttpServiceResponder}
   * @param namespace namespace string
   * @param request Serialized request string
   * @param connection {@link Connection} details if present
   * @param remoteExecutionTaskClass Remote execution task class
   * @param responder {@link HttpServiceResponder} for the http request.
   */
  private void executeRemotely(String namespace, String request, @Nullable Connection connection,
                               Class<? extends RemoteConnectionTaskBase> remoteExecutionTaskClass,
                               HttpServiceResponder responder)  {
    RemoteConnectionRequest remoteRequest = new RemoteConnectionRequest(namespace, request, connection);
    RunnableTaskRequest runnableTaskRequest =
      RunnableTaskRequest.getBuilder(remoteExecutionTaskClass.getName()).
        withParam(GSON.toJson(remoteRequest)).withNamespace(namespace).build();
    try {
      byte[] bytes = getContext().runTask(runnableTaskRequest);
      responder.sendString(new String(bytes, StandardCharsets.UTF_8));
    } catch (RemoteExecutionException e) {
      //TODO CDAP-18787 - Handle other exceptions
      RemoteTaskException remoteTaskException = e.getCause();
      responder.sendError(
        getExceptionCode(remoteTaskException.getRemoteExceptionClassName(), remoteTaskException.getMessage(),
                         namespace), remoteTaskException.getMessage());
    } catch (Exception e) {
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
    }
  }

  private int getExceptionCode(String exceptionClass, String exceptionMessage, String namespace) {
    if (IllegalArgumentException.class.getName().equals(exceptionClass)
      || ConnectionBadRequestException.class.getName().equals(exceptionClass)) {
      return HttpURLConnection.HTTP_BAD_REQUEST;
    }
    return HttpURLConnection.HTTP_INTERNAL_ERROR;
  }
}
