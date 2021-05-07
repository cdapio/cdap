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
import io.cdap.cdap.etl.proto.connection.ConnectionConflictException;
import io.cdap.cdap.etl.proto.connection.ConnectionId;
import io.cdap.cdap.etl.proto.connection.ConnectionNotFoundException;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.test.SystemAppTestBase;
import io.cdap.cdap.test.TestConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Unit test for connection store
 */
public class ConnectionStoreTest extends SystemAppTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);
  private static ConnectionStore connectionStore;

  @BeforeClass
  public static void setupTest() throws Exception {
    getStructuredTableAdmin().create(ConnectionStore.CONNECTION_TABLE_SPEC);
    connectionStore = new ConnectionStore(getTransactionRunner());
  }

  @After
  public void cleanupTest() throws Exception {
    // structuredadmin.drop won't actually delete the table since all entity tables use a single entity nosql table
    // it will just remove the registry, so next test will still see old connections
    connectionStore.clear();
  }

  @Test
  public void testBasicOperations() throws Exception {
    // put a connection in the store
    NamespaceSummary namespace = new NamespaceSummary("default", "", 0L);
    ConnectionId connectionId = new ConnectionId(namespace, "my_conn");
    Connection expected = new Connection(
      "my_conn", "GCS", "GCS connection", false, 0L, 0L,
      new PluginInfo("GCS", "connector", "Google Cloud Platform", ImmutableMap.of("project", "abc"),
                     new ArtifactSelectorConfig("SYSTEM", "google-cloud", "1.0.0")));
    connectionStore.saveConnection(connectionId, expected, false);
    // assert the connections
    Assert.assertEquals(expected, connectionStore.getConnection(connectionId));

    // update the connection, the connection should be updated
    Connection updated = new Connection(
      "my_conn", "GCS", "GCS connection updated", false, 1000L, 2000L,
      new PluginInfo("GCS", "connector", "Google Cloud Platform",
                     ImmutableMap.of("project", "abc", "serviceAccount", "secrect.json"),
                     new ArtifactSelectorConfig("SYSTEM", "google-cloud", "1.0.0")));
    connectionStore.saveConnection(connectionId, updated, false);
    Connection actual = connectionStore.getConnection(connectionId);
    Assert.assertEquals(updated, actual);

    // the creation time should remain the same
    Assert.assertEquals(0L, actual.getCreatedTimeMillis());
    // update time should get updated
    Assert.assertEquals(2000L, actual.getUpdatedTimeMillis());

    // add a new connection
    ConnectionId newConnectioId = new ConnectionId(namespace, "mynewconn");
    Connection newConnection = new Connection(
      "mynewconn", "BigQuery", "BigQuery connection", false, 0L, 0L,
      new PluginInfo("BigQuery", "connector", "Google Cloud Platform", ImmutableMap.of("project", "myproject"),
                     new ArtifactSelectorConfig("SYSTEM", "google-cloud", "1.0.0")));
    connectionStore.saveConnection(newConnectioId, newConnection, false);
    Assert.assertEquals(newConnection, connectionStore.getConnection(newConnectioId));

    // list connections should have two connections
    Assert.assertEquals(ImmutableList.of(updated, newConnection), connectionStore.listConnections(namespace));

    // update first connection to a different name "my conn" which translates to same id "my_conn", with overwrite
    // set to true, it should overwrite the connection and listing should still show two connections
    connectionId = new ConnectionId(namespace, "my conn");
    updated = new Connection(
      "my conn", "GCS", "GCS connection updated", false, 1000L, 2000L,
      new PluginInfo("GCS", "connector", "Google Cloud Platform",
                     ImmutableMap.of("project", "abc", "serviceAccount", "secrect.json"),
                     new ArtifactSelectorConfig("SYSTEM", "google-cloud", "1.0.0")));
    connectionStore.saveConnection(connectionId, updated, true);
    actual = connectionStore.getConnection(connectionId);
    Assert.assertEquals(updated, actual);

    // list should still get 2 connections
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
    connectionStore.saveConnection(id, connection, false);

    // get the connection with a namespace with a higher generation id should fail
    try {
      connectionStore.getConnection(new ConnectionId(new NamespaceSummary("default", "", 1L), "myconn"));
      Assert.fail();
    } catch (ConnectionNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testConflict() throws Exception {
    // put a connection in the store
    NamespaceSummary namespace = new NamespaceSummary("default", "", 0L);
    ConnectionId connectionId = new ConnectionId(namespace, "my_conn");
    Connection connection = new Connection(
      "my_conn", "GCS", "GCS connection", false, 0L, 0L,
      new PluginInfo("GCS", "connector", "Google Cloud Platform", ImmutableMap.of("project", "abc"),
                     new ArtifactSelectorConfig("SYSTEM", "google-cloud", "1.0.0")));
    connectionStore.saveConnection(connectionId, connection, false);

    // use a different name but evaluate to same id, with overwrite to false, it should fail to update
    try {
      connectionId = new ConnectionId(namespace, "my conn");
      connection = new Connection(
        "my conn", "GCS", "GCS connection", false, 0L, 0L,
        new PluginInfo("GCS", "connector", "Google Cloud Platform", ImmutableMap.of("project", "abc"),
                       new ArtifactSelectorConfig("SYSTEM", "google-cloud", "1.0.0")));
      connectionStore.saveConnection(connectionId, connection, false);
      Assert.fail();
    } catch (ConnectionConflictException e) {
      // expected
    }

    // check pre configured connection cannot get updated
    connectionId = new ConnectionId(namespace, "default conn");
    connection = new Connection(
      "default conn", "GCS", "GCS connection", true, 0L, 0L,
      new PluginInfo("GCS", "connector", "Google Cloud Platform", ImmutableMap.of("project", "abc"),
                     new ArtifactSelectorConfig("SYSTEM", "google-cloud", "1.0.0")));

    connectionStore.saveConnection(connectionId, connection, false);

    try {
      connection = new Connection("default conn", "BigQuery", "", false, 0L, 0L,
                                  new PluginInfo("BigQuery", "connector", "", Collections.emptyMap(),
                                                 new ArtifactSelectorConfig()));
      connectionStore.saveConnection(connectionId, connection, true);
      Assert.fail();
    } catch (ConnectionConflictException e) {
      // expected
    }

    // and pre-configured cannot be deleted
    try {
      connectionStore.deleteConnection(connectionId);
      Assert.fail();
    } catch (ConnectionConflictException e) {
      // expected
    }
  }
}
