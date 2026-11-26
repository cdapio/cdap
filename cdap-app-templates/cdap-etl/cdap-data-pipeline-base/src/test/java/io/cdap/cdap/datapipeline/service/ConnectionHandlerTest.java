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
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ConnectionHandler} credential redaction.
 */
public class ConnectionHandlerTest {

  private static final String NAMESPACE = "default";
  private static final NamespaceSummary NS_SUMMARY = new NamespaceSummary(NAMESPACE, "", 1L);

  @Test
  public void testGetConnectionRedactsPropertiesWithoutUsePermission() throws Exception {
    ContextAccessEnforcer enforcer = Mockito.mock(ContextAccessEnforcer.class);
    ConnectionEntityId entityId = new ConnectionEntityId(NAMESPACE, "db_conn");
    Mockito.doThrow(new UnauthorizedException("no use")).when(enforcer).enforce(entityId, StandardPermission.USE);

    Connection stored = createConnection("db_conn", Collections.singletonMap("password", "secret"));
    ConnectionStore store = Mockito.mock(ConnectionStore.class);
    Mockito.when(store.getConnection(new ConnectionId(NS_SUMMARY, "db_conn"))).thenReturn(stored);

    ConnectionHandler handler = createHandler(enforcer, store);
    HttpServiceResponder responder = Mockito.mock(HttpServiceResponder.class);
    handler.getConnection(Mockito.mock(HttpServiceRequest.class), responder, NAMESPACE, "db_conn");

    ArgumentCaptor<Connection> captor = ArgumentCaptor.forClass(Connection.class);
    Mockito.verify(responder).sendJson(captor.capture());
    Connection returned = captor.getValue();
    Assert.assertEquals("db_conn", returned.getName());
    Assert.assertEquals(Collections.emptyMap(), returned.getPlugin().getProperties());
  }

  @Test
  public void testGetConnectionReturnsPropertiesWithUsePermission() throws Exception {
    ContextAccessEnforcer enforcer = Mockito.mock(ContextAccessEnforcer.class);
    Map<String, String> props = Collections.singletonMap("password", "secret");
    Connection stored = createConnection("db_conn", props);
    ConnectionStore store = Mockito.mock(ConnectionStore.class);
    Mockito.when(store.getConnection(new ConnectionId(NS_SUMMARY, "db_conn"))).thenReturn(stored);

    ConnectionHandler handler = createHandler(enforcer, store);
    HttpServiceResponder responder = Mockito.mock(HttpServiceResponder.class);
    handler.getConnection(Mockito.mock(HttpServiceRequest.class), responder, NAMESPACE, "db_conn");

    ArgumentCaptor<Connection> captor = ArgumentCaptor.forClass(Connection.class);
    Mockito.verify(responder).sendJson(captor.capture());
    Assert.assertEquals(props, captor.getValue().getPlugin().getProperties());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListConnectionsRedactsOnlyUnauthorizedConnections() throws Exception {
    ContextAccessEnforcer enforcer = Mockito.mock(ContextAccessEnforcer.class);
    Mockito.doThrow(new UnauthorizedException("no use"))
      .when(enforcer).enforce(new ConnectionEntityId(NAMESPACE, "conn_denied"), StandardPermission.USE);

    Map<String, String> allowedProps = Collections.singletonMap("password", "allowed-secret");
    Map<String, String> deniedProps = Collections.singletonMap("password", "denied-secret");
    Connection allowedConn = createConnection("conn_allowed", allowedProps);
    Connection deniedConn = createConnection("conn_denied", deniedProps);
    ConnectionStore store = Mockito.mock(ConnectionStore.class);
    Mockito.when(store.listConnections(NS_SUMMARY)).thenReturn(Arrays.asList(allowedConn, deniedConn));

    ConnectionHandler handler = createHandler(enforcer, store);
    HttpServiceResponder responder = Mockito.mock(HttpServiceResponder.class);
    handler.listConnections(Mockito.mock(HttpServiceRequest.class), responder, NAMESPACE);

    ArgumentCaptor<List<Connection>> captor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(responder).sendJson(captor.capture());
    List<Connection> returned = captor.getValue();
    Assert.assertEquals(2, returned.size());
    Assert.assertEquals(allowedProps, returned.get(0).getPlugin().getProperties());
    Assert.assertEquals(Collections.emptyMap(), returned.get(1).getPlugin().getProperties());
  }

  private static Connection createConnection(String name, Map<String, String> props) {
    PluginInfo plugin = new PluginInfo("MySQL", "connector", "Database", props,
                                       new ArtifactSelectorConfig("SYSTEM", "mysql", "1.0.0"));
    return new Connection(name, name, "MySQL", "desc", false, false, 1L, 1L, plugin);
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

    ConnectionHandler handler = new ConnectionHandler(null);
    handler.initialize(context);

    Field storeField = ConnectionHandler.class.getDeclaredField("store");
    storeField.setAccessible(true);
    storeField.set(handler, store);

    Metrics metrics = Mockito.mock(Metrics.class);
    Mockito.when(metrics.child(Mockito.anyMap())).thenReturn(metrics);
    Field metricsField = ConnectionHandler.class.getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metricsField.set(handler, metrics);

    return handler;
  }
}
