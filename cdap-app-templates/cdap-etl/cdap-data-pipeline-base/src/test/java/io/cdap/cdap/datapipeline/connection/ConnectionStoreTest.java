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

package io.cdap.cdap.datapipeline.connection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.etl.proto.connection.ConnectionId;
import io.cdap.cdap.etl.proto.connection.ConnectionNotFoundException;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.test.SystemAppTestBase;
import io.cdap.cdap.test.TestConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;

/**
 * Unit test for connection store
 */
public class ConnectionStoreTest extends SystemAppTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);
  private ConnectionStore connectionStore;

  @Before
  public void setupTest() throws Exception {
    getStructuredTableAdmin().create(ConnectionStore.CONNECTION_TABLE_SPEC);
    connectionStore = new ConnectionStore(getTransactionRunner());
  }

  @After
  public void cleanupTest() throws Exception {
    getStructuredTableAdmin().drop(ConnectionStore.CONNECTION_TABLE_SPEC.getTableId());
  }

  @Test
  public void testBasicOperations() throws Exception {
    // put a connection in the store
    NamespaceSummary namespace = new NamespaceSummary("default", "", 0L);
    ConnectionId connectionId = new ConnectionId(namespace, "myconn");
    Connection expected = new Connection(
      "myconn", "GCS", "GCS connection", false, 0L, 0L,
      new PluginInfo("GCS", "connector", "Google Cloud Platform", ImmutableMap.of("project", "abc"),
                     new ArtifactSelectorConfig("SYSTEM", "google-cloud", "1.0.0")));
    connectionStore.saveConnection(connectionId, expected);
    // assert the connections
    Connection existing = connectionStore.getConnection(connectionId);
    Assert.assertEquals(expected, existing);

    // update the connection, the connection should be updated
    Connection updated = new Connection(
      "myconn", "GCS", "GCS connection updated", false, 1000L, 2000L,
      new PluginInfo("GCS", "connector", "Google Cloud Platform",
                     ImmutableMap.of("project", "abc", "serviceAccount", "secrect.json"),
                     new ArtifactSelectorConfig("SYSTEM", "google-cloud", "1.0.0")));
    connectionStore.saveConnection(connectionId, updated);
    Connection actual = connectionStore.getConnection(connectionId);
    Assert.assertEquals(updated, actual);

    // the creation time should remain the same
    Assert.assertEquals(0L, actual.getCreatedTimeMillis());
    // update time should get updated
    Assert.assertEquals(2000L, actual.getUpdatedTimeMillis());
    // identifier should be same
    Assert.assertEquals(existing.getIdentifier(), actual.getIdentifier());

    // add a new connection
    ConnectionId newConnectioId = new ConnectionId(namespace, "mynewconn");
    Connection newConnection = new Connection(
      "mynewconn", "BigQuery", "BigQuery connection", false, 0L, 0L,
      new PluginInfo("BigQuery", "connector", "Google Cloud Platform", ImmutableMap.of("project", "myproject"),
                     new ArtifactSelectorConfig("SYSTEM", "google-cloud", "1.0.0")));
    connectionStore.saveConnection(newConnectioId, newConnection);
    Assert.assertEquals(newConnection, connectionStore.getConnection(newConnectioId));

    // list connections should have two connections
    Assert.assertEquals(ImmutableList.of(updated, newConnection), connectionStore.listConnections(namespace));

    // delete the first one
    connectionStore.deleteConnection(connectionId);
    Assert.assertEquals(Collections.singletonList(newConnection), connectionStore.listConnections(namespace));

    // delete the second one
    connectionStore.deleteConnection(newConnectioId);
    Assert.assertEquals(Collections.emptyList(), connectionStore.listConnections(namespace));
  }

  @Test
  public void testNotFound() throws Exception {
    ConnectionId connectionId = new ConnectionId(new NamespaceSummary("default", "", 0L), "nonexisting");

    // get non-existing connection
    try {
      connectionStore.getConnection(connectionId);
      Assert.fail();
    } catch (ConnectionNotFoundException e) {
      // expected
    }

    try {
      connectionStore.deleteConnection(connectionId);
      Assert.fail();
    } catch (ConnectionNotFoundException e) {
      // expected
    }

    // put a connection in the store
    NamespaceSummary oldNamespace = new NamespaceSummary("default", "", 0L);
    ConnectionId id = new ConnectionId(oldNamespace, "myconn");
    Connection connection = new Connection(
      "myconn", "GCS", "GCS connection", false, 0L, 0L,
      new PluginInfo("GCS", "connector", "Google Cloud Platform", ImmutableMap.of("project", "abc"),
                     new ArtifactSelectorConfig("SYSTEM", "google-cloud", "1.0.0")));
    connectionStore.saveConnection(id, connection);

    // get the connection with a namespace with a higher generation id should fail
    try {
      connectionStore.getConnection(new ConnectionId(new NamespaceSummary("default", "", 1L), "myconn"));
      Assert.fail();
    } catch (ConnectionNotFoundException e) {
      // expected
    }
  }
}
