/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline.service;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.service.http.HttpServiceHandlerSpecification;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.datapipeline.connection.ConnectionStore;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.etl.proto.connection.ConnectionId;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.proto.id.ConnectionEntityId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ConnectionHandler} credential redaction and RBAC checks.
 */
public class ConnectionHandlerTest {

  private static final String NAMESPACE = "default";
  private static final NamespaceSummary NS_SUMMARY = new NamespaceSummary(NAMESPACE, "", 1L);

  @Test
  public void testGetConnectionRedactsPropertiesWithoutUsePermission() throws Exception {
    ContextAccessEnforcer enforcer = Mockito.mock(ContextAccessEnforcer.class);
    ConnectionEntityId entityId = new ConnectionEntityId(NAMESPACE, "My_DB_1");
    Mockito.doThrow(new UnauthorizedException("no use")).when(enforcer).enforce(entityId, StandardPermission.USE);

    Connection stored = createConnection("My DB-1", "My_DB_1", Collections.singletonMap("password", "secret"));
    Connection returned = invokeGetConnection(enforcer, stored, false);
    Assert.assertEquals(createConnection("My DB-1", "My_DB_1", Collections.emptyMap()), returned);
  }

  @Test
  public void testGetConnectionReturnsPropertiesWithUsePermission() throws Exception {
    Map<String, String> props = Collections.singletonMap("password", "secret");
    Connection stored = createConnection("My DB-1", "My_DB_1", props);
    Connection returned = invokeGetConnection(Mockito.mock(ContextAccessEnforcer.class), stored, false);
    Assert.assertEquals(stored, returned);
  }

  @Test
  public void testGetConnectionRequireUseReturnsForbiddenWithoutUsePermission() throws Exception {
    ContextAccessEnforcer enforcer = Mockito.mock(ContextAccessEnforcer.class);
    ConnectionEntityId entityId = new ConnectionEntityId(NAMESPACE, "My_DB_1");
    Mockito.doThrow(new UnauthorizedException("no use")).when(enforcer).enforce(entityId, StandardPermission.USE);

    ConnectionStore store = Mockito.mock(ConnectionStore.class);
    ConnectionHandler handler = createHandler(enforcer, store);
    HttpServiceResponder responder = Mockito.mock(HttpServiceResponder.class);
    handler.getConnection(Mockito.mock(HttpServiceRequest.class), responder, NAMESPACE, "My DB-1", true);

    Mockito.verify(responder).sendError(HttpURLConnection.HTTP_FORBIDDEN, "no use");
    Mockito.verify(store, Mockito.never()).getConnection(Mockito.any());
    Mockito.verify(responder, Mockito.never()).sendJson(Mockito.any());
  }

  @Test
  public void testGetConnectionRequireUseReturnsFullConnectionWithUsePermission() throws Exception {
    Map<String, String> props = Collections.singletonMap("password", "secret");
    Connection stored = createConnection("My DB-1", "My_DB_1", props);
    Connection returned = invokeGetConnection(Mockito.mock(ContextAccessEnforcer.class), stored, true);
    Assert.assertEquals(stored, returned);
  }

  @Test
  public void testGetConnectionReturnsForbiddenWithoutGetPermission() throws Exception {
    ContextAccessEnforcer enforcer = Mockito.mock(ContextAccessEnforcer.class);
    ConnectionEntityId entityId = new ConnectionEntityId(NAMESPACE, "My_DB_1");
    Mockito.doThrow(new UnauthorizedException("no get")).when(enforcer).enforce(entityId, StandardPermission.GET);

    ConnectionStore store = Mockito.mock(ConnectionStore.class);
    ConnectionHandler handler = createHandler(enforcer, store);
    HttpServiceResponder responder = Mockito.mock(HttpServiceResponder.class);
    handler.getConnection(Mockito.mock(HttpServiceRequest.class), responder, NAMESPACE, "My DB-1", false);

    Mockito.verify(responder).sendError(HttpURLConnection.HTTP_FORBIDDEN, "no get");
    Mockito.verify(store, Mockito.never()).getConnection(Mockito.any());
    Mockito.verify(responder, Mockito.never()).sendJson(Mockito.any());
  }

  @Test
  public void testListConnectionsRedactsOnlyUnauthorizedConnections() throws Exception {
    ContextAccessEnforcer enforcer = Mockito.mock(ContextAccessEnforcer.class);
    Mockito.doThrow(new UnauthorizedException("no use"))
      .when(enforcer).enforce(new ConnectionEntityId(NAMESPACE, "Conn_Denied"), StandardPermission.USE);

    Connection allowedConn = createConnection("Conn Allowed", "Conn_Allowed",
                                              Collections.singletonMap("password", "allowed-secret"));
    Connection deniedConn = createConnection("Conn Denied", "Conn_Denied",
                                             Collections.singletonMap("password", "denied-secret"));
    ConnectionStore store = Mockito.mock(ConnectionStore.class);
    Mockito.when(store.listConnections(NS_SUMMARY)).thenReturn(Arrays.asList(allowedConn, deniedConn));

    ConnectionHandler handler = createHandler(enforcer, store);
    HttpServiceResponder responder = Mockito.mock(HttpServiceResponder.class);
    handler.listConnections(Mockito.mock(HttpServiceRequest.class), responder, NAMESPACE);

    @SuppressWarnings("unchecked") // listConnections passes List<Connection> to sendJson
    ArgumentCaptor<List<Connection>> captor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(responder).sendJson(captor.capture());
    Assert.assertEquals(
      Arrays.asList(allowedConn, createConnection("Conn Denied", "Conn_Denied", Collections.emptyMap())),
      captor.getValue());
  }

  @Test
  public void testGetAndListFailClosedOnNonUnauthorizedAccessException() throws Exception {
    ContextAccessEnforcer enforcer = Mockito.mock(ContextAccessEnforcer.class);
    ConnectionEntityId entityId = new ConnectionEntityId(NAMESPACE, "My_DB_1");
    Mockito.doThrow(new AccessException("auth backend unavailable"))
      .when(enforcer).enforce(entityId, StandardPermission.USE);

    Connection stored = createConnection("My DB-1", "My_DB_1", Collections.singletonMap("password", "secret"));
    ConnectionStore store = Mockito.mock(ConnectionStore.class);
    Mockito.when(store.getConnection(new ConnectionId(NS_SUMMARY, "My DB-1"))).thenReturn(stored);
    Mockito.when(store.listConnections(NS_SUMMARY)).thenReturn(Collections.singletonList(stored));

    ConnectionHandler handler = createHandler(enforcer, store);

    HttpServiceResponder getResponder = Mockito.mock(HttpServiceResponder.class);
    handler.getConnection(Mockito.mock(HttpServiceRequest.class), getResponder, NAMESPACE, "My DB-1", false);
    Mockito.verify(getResponder).sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, "auth backend unavailable");
    Mockito.verify(getResponder, Mockito.never()).sendJson(Mockito.any());

    HttpServiceResponder listResponder = Mockito.mock(HttpServiceResponder.class);
    handler.listConnections(Mockito.mock(HttpServiceRequest.class), listResponder, NAMESPACE);
    Mockito.verify(listResponder).sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, "auth backend unavailable");
    Mockito.verify(listResponder, Mockito.never()).sendJson(Mockito.any());
  }

  @Test
  public void testRedactionHandlesNullEmptyPropertiesAndLegacyNullConnectionId() throws Exception {
    ContextAccessEnforcer enforcer = Mockito.mock(ContextAccessEnforcer.class);
    Mockito.doThrow(new UnauthorizedException("no use"))
      .when(enforcer).enforce(new ConnectionEntityId(NAMESPACE, "Legacy_Conn"), StandardPermission.USE);

    Connection nullPluginConn = new Connection("null_plugin", "null_plugin", "MySQL", "desc",
                                               true, false, 100L, 200L, null);
    Connection nullPropsConn = createConnection("null_props", "null_props", null);
    Connection emptyPropsConn = createConnection("empty_props", "empty_props", Collections.emptyMap());
    Connection legacyNullIdConn = createConnection("Legacy Conn", null,
                                                   Collections.singletonMap("password", "secret"));

    ConnectionStore store = Mockito.mock(ConnectionStore.class);
    Mockito.when(store.listConnections(NS_SUMMARY))
      .thenReturn(Arrays.asList(nullPluginConn, nullPropsConn, emptyPropsConn, legacyNullIdConn));

    ConnectionHandler handler = createHandler(enforcer, store);
    HttpServiceResponder responder = Mockito.mock(HttpServiceResponder.class);
    handler.listConnections(Mockito.mock(HttpServiceRequest.class), responder, NAMESPACE);

    @SuppressWarnings("unchecked") // listConnections passes List<Connection> to sendJson
    ArgumentCaptor<List<Connection>> captor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(responder).sendJson(captor.capture());
    Assert.assertEquals(
      Arrays.asList(nullPluginConn, nullPropsConn, emptyPropsConn,
                    createConnection("Legacy Conn", null, Collections.emptyMap())),
      captor.getValue());
    // Property-less connections must not invoke USE enforcement
    Mockito.verify(enforcer, Mockito.times(1)).enforce(Mockito.any(ConnectionEntityId.class),
                                                       Mockito.eq(StandardPermission.USE));
  }

  private static Connection invokeGetConnection(ContextAccessEnforcer enforcer, Connection stored,
                                                boolean requireUse) throws Exception {
    ConnectionStore store = Mockito.mock(ConnectionStore.class);
    Mockito.when(store.getConnection(new ConnectionId(NS_SUMMARY, stored.getName()))).thenReturn(stored);

    ConnectionHandler handler = createHandler(enforcer, store);
    HttpServiceResponder responder = Mockito.mock(HttpServiceResponder.class);
    handler.getConnection(Mockito.mock(HttpServiceRequest.class), responder, NAMESPACE, stored.getName(), requireUse);

    ArgumentCaptor<Connection> captor = ArgumentCaptor.forClass(Connection.class);
    Mockito.verify(responder).sendJson(captor.capture());
    return captor.getValue();
  }

  private static Connection createConnection(String name, String connectionId, Map<String, String> props) {
    PluginInfo plugin = new PluginInfo("MySQL", "connector", "Database", props,
                                       new ArtifactSelectorConfig("SYSTEM", "mysql", "1.0.0"));
    return new Connection(name, connectionId, "MySQL", "desc", true, false, 100L, 200L, plugin);
  }

  private static ConnectionHandler createHandler(ContextAccessEnforcer enforcer,
                                                 ConnectionStore store) throws Exception {
    SystemHttpServiceContext context = Mockito.mock(SystemHttpServiceContext.class);
    Admin admin = Mockito.mock(Admin.class);
    Mockito.when(admin.getNamespaceSummary(NAMESPACE)).thenReturn(NS_SUMMARY);
    Mockito.when(context.getAdmin()).thenReturn(admin);
    Mockito.when(context.getContextAccessEnforcer()).thenReturn(enforcer);

    HttpServiceHandlerSpecification spec = new HttpServiceHandlerSpecification(
      ConnectionHandler.class.getName(), "connection", "",
      Collections.singletonMap("disabledTypes", "[]"),
      Collections.emptySet(), Collections.emptyList());
    Mockito.when(context.getSpecification()).thenReturn(spec);

    Metrics metrics = Mockito.mock(Metrics.class);
    Mockito.when(metrics.child(Mockito.anyMap())).thenReturn(metrics);

    ConnectionHandler handler = new ConnectionHandler(null);
    handler.initialize(context, store, metrics);
    return handler;
  }
}
