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
 */


package io.cdap.cdap.internal.tether;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.proto.NamespaceConfig;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TetherClientHandlerTest {
  private static final Gson GSON = new Gson();
  public static final String CLIENT_INSTANCE = "tether-client";
  public static final String NAMESPACE_1 = "ns1";
  public static final String NAMESPACE_2 = "ns2";
  public static final String NAMESPACE_3 = "ns3";
  public static final List<NamespaceAllocation> NAMESPACES = ImmutableList.of(
    new NamespaceAllocation(NAMESPACE_1, "1", "1Gi"),
    new NamespaceAllocation(NAMESPACE_2, "2", "2Gi"),
    new NamespaceAllocation(NAMESPACE_3, null, null));
  public static final String SERVER_INSTANCE = "my-instance";
  public static final String PROJECT = "my-project";
  public static final String  LOCATION = "us-west1";
  private static CConfiguration cConf;
  private static TetherStore tetherStore;
  private static NamespaceAdmin namespaceAdmin;
  private static Injector injector;
  private static TransactionManager txManager;

  private NettyHttpService serverService;
  private ClientConfig serverConfig;
  private NettyHttpService clientService;
  private ClientConfig clientConfig;
  private MockTetherServerHandler serverHandler;
  private RemoteAgentService remoteAgentService;

  @BeforeClass
  public static void setup() throws Exception {
    cConf = CConfiguration.create();
    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new TransactionModules().getInMemoryModules(),
      new TransactionExecutorModule(),
      new NamespaceAdminTestModule(),
      new StorageModule(),
      new PrivateModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
          expose(MetricsCollectionService.class);
        }
      });
    tetherStore = new TetherStore(injector.getInstance(TransactionRunner.class));
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    NamespaceConfig config1 = new NamespaceConfig(ImmutableMap.of(
      TetherClientHandler.NAMESPACE_CPU_LIMIT_PROPERTY, NAMESPACES.get(0).getCpuLimit(),
      TetherClientHandler.NAMESPACE_MEMORY_LIMIT_PROPERTY, NAMESPACES.get(0).getMemoryLimit()));
    NamespaceMeta nsMeta1 =  new NamespaceMeta(NAMESPACE_1, "my ns1", System.currentTimeMillis(), config1);
    NamespaceConfig config2 = new NamespaceConfig(ImmutableMap.of(
      TetherClientHandler.NAMESPACE_CPU_LIMIT_PROPERTY, NAMESPACES.get(1).getCpuLimit(),
      TetherClientHandler.NAMESPACE_MEMORY_LIMIT_PROPERTY, NAMESPACES.get(1).getMemoryLimit()));
    NamespaceMeta nsMeta2 =  new NamespaceMeta(NAMESPACE_2, "my ns2", System.currentTimeMillis(), config2);

    Map<String, String> config = new HashMap<>();
    // Not using an ImmutableMap here because it doesn't allow null values.
    config.put(TetherClientHandler.NAMESPACE_CPU_LIMIT_PROPERTY, NAMESPACES.get(2).getCpuLimit());
    config.put(TetherClientHandler.NAMESPACE_MEMORY_LIMIT_PROPERTY, NAMESPACES.get(2).getMemoryLimit());
    NamespaceMeta nsMeta3 =  new NamespaceMeta(NAMESPACE_3, "my ns3", System.currentTimeMillis(),
                                               new NamespaceConfig(config));
    namespaceAdmin.create(nsMeta1);
    namespaceAdmin.create(nsMeta2);
    namespaceAdmin.create(nsMeta3);
  }

  @AfterClass
  public static void teardown() throws Exception {
    namespaceAdmin.delete(new NamespaceId(NAMESPACE_1));
    namespaceAdmin.delete(new NamespaceId(NAMESPACE_2));
    namespaceAdmin.delete(new NamespaceId(NAMESPACE_3));
    if (txManager != null) {
      txManager.stopAndWait();
    }
  }

  @Before
  public void setUp() throws Exception {
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    structuredTableRegistry.initialize();
    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class),
                                    structuredTableRegistry);
    CConfiguration conf = CConfiguration.create();
    serverHandler = new MockTetherServerHandler();
    serverService = new CommonNettyHttpServiceBuilder(conf, getClass().getSimpleName() + "_server")
      .setHttpHandlers(serverHandler).build();
    serverService.start();
    serverConfig = ClientConfig.builder()
      .setConnectionConfig(
        ConnectionConfig.builder()
          .setHostname(serverService.getBindAddress().getHostName())
          .setPort(serverService.getBindAddress().getPort())
          .setSSLEnabled(false)
          .build()).build();

    cConf.setInt(Constants.Tether.CONNECT_INTERVAL, 1);
    cConf.setInt(Constants.Tether.CONNECTION_TIMEOUT_SECONDS, 5);
    cConf.set(Constants.INSTANCE_NAME, CLIENT_INSTANCE);

    clientService = new CommonNettyHttpServiceBuilder(conf, getClass().getSimpleName() + "_client")
      .setHttpHandlers(new TetherClientHandler(cConf, tetherStore, namespaceAdmin),
                       new TetherHandler(cConf, tetherStore, injector.getInstance(TransactionRunner.class)))
      .build();
    clientService.start();
    clientConfig = ClientConfig.builder()
      .setConnectionConfig(
        ConnectionConfig.builder()
          .setHostname(clientService.getBindAddress().getHostName())
          .setPort(clientService.getBindAddress().getPort())
          .setSSLEnabled(false)
          .build()).build();

    remoteAgentService = new RemoteAgentService(cConf, injector.getInstance(TransactionRunner.class));
    remoteAgentService.startUp();
  }

  @After
  public void tearDown() throws Exception {
    remoteAgentService.shutDown();
  }

  @Test
  public void testResendTetherRequests() throws InterruptedException {
    Map<String, String> metadata = ImmutableMap.of("project", PROJECT,
                                                   "location", LOCATION);
    PeerMetadata peerMetadata = new PeerMetadata(NAMESPACES, metadata);
    PeerInfo peer = new PeerInfo(SERVER_INSTANCE, serverConfig.getConnectionConfig().getURI().toString(),
                                 TetherStatus.PENDING, peerMetadata);

    // Server returns 404 before tethering has been accepted by admin on server side.
    serverHandler.setResponseStatus(HttpResponseStatus.NOT_FOUND);

    // Add peer in PENDING state
    tetherStore.addPeer(peer);

    // Remote agent should send a tether request to peer in PENDING state if it
    // responds with 404.
    waitForTetherCreated();

    // cleanup
    tetherStore.deletePeer(SERVER_INSTANCE);
  }

  @Test
  public void testTetherAccept() throws Exception {
    // Server returns 404 before tethering has been accepted by admin on server side.
    serverHandler.setResponseStatus(HttpResponseStatus.NOT_FOUND);

    // Client initiates tethering with the server
    createTether(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES);

    // Server accepts tethering
    serverHandler.setResponseStatus(HttpResponseStatus.OK);

    String serverEndpoint = serverConfig.getConnectionConfig().getURI().toString();
    // Tethering state should transition to accepted on the client.
    waitForTetherStatus(TetherStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                        PROJECT, LOCATION, NAMESPACES, TetherConnectionStatus.ACTIVE);

    // Duplicate tether request should be ignored when tether is accepted.
    createTether(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, TetherStatus.ACCEPTED);

    // Tether rejection should be ignored when tether is accepted.
    serverHandler.setResponseStatus(HttpResponseStatus.FORBIDDEN);
    waitForTetherStatus(TetherStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                        PROJECT, LOCATION, NAMESPACES, TetherConnectionStatus.ACTIVE);

    // Make server respond with 500 error for control messages.
    serverHandler.setResponseStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    Thread.sleep(cConf.getInt(Constants.Tether.CONNECTION_TIMEOUT_SECONDS) * 1000);
    // Control channel state should become inactive
    waitForTetherStatus(TetherStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                        PROJECT, LOCATION, NAMESPACES, TetherConnectionStatus.INACTIVE);
    // Stop returning an error from the server.
    serverHandler.setResponseStatus(HttpResponseStatus.OK);
    // Control channel should become active
    waitForTetherStatus(TetherStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                        PROJECT, LOCATION, NAMESPACES, TetherConnectionStatus.ACTIVE);

    // cleanup
    deleteTether(SERVER_INSTANCE);
  }

  @Test
  public void testTetherReject() throws Exception {
    // Server returns 404 before tethering has been accepted or rejected by admin on server side.
    serverHandler.setResponseStatus(HttpResponseStatus.NOT_FOUND);

    // Client initiate tethering with the server
    createTether(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES);

    // Duplicate tether request should be ignored when tether is pending
    createTether(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES);

    String serverEndpoint = serverConfig.getConnectionConfig().getURI().toString();
    // Control channel should be active at this point.
    waitForTetherStatus(TetherStatus.PENDING, SERVER_INSTANCE, serverEndpoint,
                        PROJECT, LOCATION, NAMESPACES, TetherConnectionStatus.ACTIVE);

    // Server rejects tethering
    serverHandler.setResponseStatus(HttpResponseStatus.FORBIDDEN);
    // Tether state should be updated to rejected
    waitForTetherStatus(TetherStatus.REJECTED, SERVER_INSTANCE, serverEndpoint,
                        PROJECT, LOCATION, NAMESPACES, TetherConnectionStatus.ACTIVE);

    // Server sends unexpected 200. It should be ignored.
    serverHandler.setResponseStatus(HttpResponseStatus.OK);
    waitForTetherStatus(TetherStatus.REJECTED, SERVER_INSTANCE, serverEndpoint,
                        PROJECT, LOCATION, NAMESPACES, TetherConnectionStatus.ACTIVE);

    // Tether request should be ignored when tether is rejected
    createTether(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, TetherStatus.REJECTED);

    // cleanup
    deleteTether(SERVER_INSTANCE);
  }

  @Test
  public void testTetherStatus() throws IOException, InterruptedException {
    // Tether does not exist, should return 404.
    HttpRequest request = HttpRequest.builder(HttpMethod.GET,
                                              clientConfig.resolveURL("tethering/connections/" + SERVER_INSTANCE))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());

    // Server returns 404 before tethering has been accepted or rejected by admin on server side.
    serverHandler.setResponseStatus(HttpResponseStatus.NOT_FOUND);

    // Client initiate tethering with the server
    createTether(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES);

    // Tether status for the peer should be returned.
    request = HttpRequest.builder(HttpMethod.GET,
                                  clientConfig.resolveURL("tethering/connections/" + SERVER_INSTANCE))
      .build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    PeerStatus peerStatus = GSON.fromJson(response.getResponseBodyAsString(), PeerStatus.class);
    Assert.assertEquals(SERVER_INSTANCE, peerStatus.getName());
    Assert.assertEquals(TetherStatus.PENDING, peerStatus.getTetherStatus());
    Assert.assertEquals(TetherConnectionStatus.ACTIVE, peerStatus.getConnectionStatus());
    Assert.assertEquals(serverConfig.getConnectionConfig().getURI().toString(), peerStatus.getEndpoint());
    Assert.assertEquals(PROJECT, peerStatus.getPeerMetadata().getMetadata().get("project"));
    Assert.assertEquals(LOCATION, peerStatus.getPeerMetadata().getMetadata().get("location"));
    Assert.assertEquals(NAMESPACES, peerStatus.getPeerMetadata().getNamespaces());

    // cleanup
    deleteTether(SERVER_INSTANCE);
  }

  @Test
  public void testDeletePendingTether() throws IOException, InterruptedException {
    // Server returns 404 before tethering has been accepted or rejected by admin on server side.
    serverHandler.setResponseStatus(HttpResponseStatus.NOT_FOUND);

    // Client initiate tethering with the server
    createTether(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES);

    // Delete tethering on the client.
    deleteTether(SERVER_INSTANCE);
  }

  @Test
  public void testGetTetherUnknownPeer() throws IOException {
    HttpRequest request = HttpRequest.builder(HttpMethod.GET,
                                              clientConfig.resolveURL("tethering/connections/unknonwn_peer"))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());
  }

  @Test
  public void testDeleteTetherUnknownPeer() throws IOException {
    HttpRequest request = HttpRequest.builder(HttpMethod.DELETE,
                                              clientConfig.resolveURL("tethering/connections/unknonwn_peer"))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());
  }

  private void deleteTether(String instance) throws IOException {
    HttpRequest request = HttpRequest.builder(HttpMethod.DELETE,
                                              clientConfig.resolveURL("tethering/connections/" + instance))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
  }

  private void createTether(String instance, String project, String location,
                            List<NamespaceAllocation> namespaceAllocations) throws IOException, InterruptedException {
    createTether(instance, project, location, namespaceAllocations,
                 TetherStatus.PENDING);
  }

  private void createTether(String instance, String project, String location,
                            List<NamespaceAllocation> namespaceAllocations, TetherStatus expectedTetherStatus)
    throws IOException, InterruptedException {
    // Send tether request
    List<String> namespaces = ImmutableList.of(NAMESPACES.get(0).getNamespace(),
                                               NAMESPACES.get(1).getNamespace(),
                                               NAMESPACES.get(2).getNamespace());
    Map<String, String> metadata = ImmutableMap.of("project", project, "location", location);
    TetherCreationRequest tetherRequest = new TetherCreationRequest(instance,
                                                                    serverConfig.getConnectionConfig().getURI()
                                                                      .toString(),
                                                                    namespaces,
                                                                    metadata);
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, clientConfig.resolveURL("tethering/create"))
      .withBody(GSON.toJson(tetherRequest))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    waitForTetherStatus(expectedTetherStatus, tetherRequest.getPeer(), tetherRequest.getEndpoint(),
                        project, location, namespaceAllocations, TetherConnectionStatus.ACTIVE);
  }

  private void waitForTetherStatus(TetherStatus tetherStatus, String instanceName, String endpoint, String project,
                                   String location, List<NamespaceAllocation> namespaces,
                                   TetherConnectionStatus connectionStatus) throws IOException, InterruptedException {
    List<PeerStatus> peers = new ArrayList<>();
    for (int retry = 0; retry < 5; ++retry) {
      HttpRequest request = HttpRequest.builder(HttpMethod.GET, clientConfig.resolveURL("tethering/connections"))
        .build();
      HttpResponse response = HttpRequests.execute(request);
      Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
      Type type = new TypeToken<List<PeerStatus>>() {
      }.getType();
      peers = GSON.fromJson(response.getResponseBodyAsString(), type);
      Assert.assertEquals(1, peers.size());
      if (peers.get(0).getTetherStatus() == tetherStatus && peers.get(0).getConnectionStatus() == connectionStatus) {
        break;
      }
      Thread.sleep(500);
    }
    Assert.assertEquals(1, peers.size());
    PeerStatus peer = peers.get(0);
    Assert.assertEquals(tetherStatus, peer.getTetherStatus());
    Assert.assertEquals(instanceName, peer.getName());
    Assert.assertEquals(endpoint, peer.getEndpoint());
    Assert.assertEquals(project, peer.getPeerMetadata().getMetadata().get("project"));
    Assert.assertEquals(location, peer.getPeerMetadata().getMetadata().get("location"));
    Assert.assertEquals(namespaces, peer.getPeerMetadata().getNamespaces());
    Assert.assertEquals(connectionStatus, peer.getConnectionStatus());
  }

  private void waitForTetherCreated() throws InterruptedException {
    for (int retry = 0; retry < 5; ++retry) {
      if (serverHandler.isTetherCreated()) {
        return;
      }
      Thread.sleep(500);
    }
    Assert.assertTrue(serverHandler.isTetherCreated());
  }
}
