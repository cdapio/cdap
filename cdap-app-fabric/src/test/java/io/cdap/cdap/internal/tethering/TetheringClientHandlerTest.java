/*
 * Copyright © 2021-2022 Cask Data, Inc.
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


package io.cdap.cdap.internal.tethering;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.runtime.NoOpProgramStateWriter;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.internal.app.store.StoreProgramRunRecordFetcher;
import io.cdap.cdap.logging.gateway.handlers.ProgramRunRecordFetcher;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.InstancePermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.authorization.DefaultContextAccessEnforcer;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public class TetheringClientHandlerTest {
  private static final Gson GSON = new Gson();
  public static final String CLIENT_INSTANCE = "tethering-client";
  public static final String NAMESPACE_1 = "ns1";
  public static final String NAMESPACE_2 = "ns2";
  public static final String NAMESPACE_3 = "ns3";
  public static final List<NamespaceAllocation> NAMESPACES = ImmutableList.of(
    new NamespaceAllocation(NAMESPACE_1, "1", "1Gi"),
    new NamespaceAllocation(NAMESPACE_2, "2", "2Gi"),
    new NamespaceAllocation(NAMESPACE_3, null, null));
  public static final String DESCRIPTION = "my tethering";
  public static final String SERVER_INSTANCE = "my-instance";
  public static final String PROJECT = "my-project";
  public static final String LOCATION = "us-west1";
  private static final long REQUEST_TIME = System.currentTimeMillis();
  private static CConfiguration cConf;
  private static TetheringStore tetheringStore;
  private static Injector injector;
  private static TransactionManager txManager;

  private NettyHttpService serverService;
  private ClientConfig serverConfig;
  private NettyHttpService clientService;
  private ClientConfig clientConfig;
  private MockTetheringServerHandler serverHandler;
  private TetheringAgentService tetheringAgentService;

  // User having tethering permissions
  private static final Principal MASTER_PRINCIPAL = new Principal("master", Principal.PrincipalType.USER);
  // User not having tethering permissions
  private static final Principal UNPRIVILEGED_PRINCIPAL = new Principal("unprivileged",
                                                                        Principal.PrincipalType.USER);

  @BeforeClass
  public static void setup() throws Exception {
    cConf = CConfiguration.create();
    injector = Guice.createInjector(
      new ConfigModule(cConf),
      RemoteAuthenticatorModules.getNoOpModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new TransactionModules().getInMemoryModules(),
      new TransactionExecutorModule(),
      new InMemoryDiscoveryModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new StorageModule(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new PrivateModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
          expose(MetricsCollectionService.class);
          bind(ProgramStateWriter.class).to(NoOpProgramStateWriter.class).in(Scopes.SINGLETON);
          expose(ProgramStateWriter.class);
          bind(ProgramRunRecordFetcher.class).to(StoreProgramRunRecordFetcher.class).in(Scopes.SINGLETON);
          expose(ProgramRunRecordFetcher.class);
        }
      });
    tetheringStore = new TetheringStore(injector.getInstance(TransactionRunner.class));
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

  }

  @AfterClass
  public static void teardown() throws Exception {
    if (txManager != null) {
      txManager.stopAndWait();
    }
  }

  @Before
  public void setUp() throws Exception {
    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));
    CConfiguration conf = CConfiguration.create();
    serverHandler = new MockTetheringServerHandler();
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

    cConf.setInt(Constants.Tethering.CONNECTION_INTERVAL, 1);
    cConf.setInt(Constants.Tethering.CONNECTION_TIMEOUT_SECONDS, 5);
    cConf.set(Constants.INSTANCE_NAME, CLIENT_INSTANCE);

    List<Permission> tetheringPermissions = Arrays.asList(InstancePermission.TETHER);
    InMemoryAccessController inMemoryAccessController = new InMemoryAccessController();
    inMemoryAccessController.grant(Authorizable.fromEntityId(InstanceId.SELF), MASTER_PRINCIPAL,
                                   Collections.unmodifiableSet(new HashSet<>(tetheringPermissions)));
    ContextAccessEnforcer contextAccessEnforcer =
      new DefaultContextAccessEnforcer(new AuthenticationTestContext(), inMemoryAccessController);
    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);

    MessagingService messagingService = injector.getInstance(MessagingService.class);
    clientService = new CommonNettyHttpServiceBuilder(conf, getClass().getSimpleName() + "_client")
      .setHttpHandlers(new TetheringClientHandler(tetheringStore, contextAccessEnforcer),
                       new TetheringHandler(cConf, tetheringStore, messagingService))
      .build();
    clientService.start();
    clientConfig = ClientConfig.builder()
      .setConnectionConfig(
        ConnectionConfig.builder()
          .setHostname(clientService.getBindAddress().getHostName())
          .setPort(clientService.getBindAddress().getPort())
          .setSSLEnabled(false)
          .build()).build();

    tetheringAgentService = new TetheringAgentService(cConf, injector.getInstance(TransactionRunner.class),
                                                      tetheringStore, injector.getInstance(ProgramStateWriter.class),
                                                      messagingService,
                                                      injector.getInstance(ProgramRunRecordFetcher.class),
                                                      injector.getInstance(RemoteAuthenticator.class));
    Assert.assertEquals(Service.State.RUNNING, tetheringAgentService.startAndWait());
  }

  @After
  public void tearDown() throws Exception {
    Assert.assertEquals(Service.State.TERMINATED, tetheringAgentService.stopAndWait());

  }

  @Test
  public void testResendTetheringRequests()
    throws InterruptedException, IOException, PeerNotFoundException, PeerAlreadyExistsException {
    Map<String, String> metadata = ImmutableMap.of("project", PROJECT,
                                                   "location", LOCATION);
    PeerMetadata peerMetadata = new PeerMetadata(NAMESPACES, metadata, DESCRIPTION);
    PeerInfo peer = new PeerInfo(SERVER_INSTANCE, serverConfig.getConnectionConfig().getURI().toString(),
                                 TetheringStatus.PENDING, peerMetadata, REQUEST_TIME);

    // Server returns 404 before tethering has been accepted by admin on server side.
    serverHandler.setResponseStatus(HttpResponseStatus.NOT_FOUND);

    // Add peer in PENDING state
    tetheringStore.addPeer(peer);

    // Remote agent should send a tethering request to peer in PENDING state if it
    // responds with 404.
    waitForTetheringCreated();

    // cleanup
    tetheringStore.deletePeer(SERVER_INSTANCE);
  }

  @Test
  public void testTetheringAccept() throws Exception {
    // Server returns 404 before tethering has been accepted by admin on server side.
    serverHandler.setResponseStatus(HttpResponseStatus.NOT_FOUND);

    // Client initiates tethering with the server
    createTethering(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, REQUEST_TIME, DESCRIPTION);

    // Server accepts tethering
    serverHandler.setResponseStatus(HttpResponseStatus.OK);

    String serverEndpoint = serverConfig.getConnectionConfig().getURI().toString();
    // Tethering state should transition to accepted on the client.
    waitForTetheringStatus(TetheringStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, true);

    // Duplicate tethering request should be fail.
    createTethering(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, REQUEST_TIME, DESCRIPTION,
                    TetheringStatus.ACCEPTED, HttpResponseStatus.BAD_REQUEST);

    // Tethering rejection should be ignored when tether is accepted.
    serverHandler.setResponseStatus(HttpResponseStatus.FORBIDDEN);
    waitForTetheringStatus(TetheringStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, true);

    // Make server respond with 500 error for control messages.
    serverHandler.setResponseStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    Thread.sleep(cConf.getInt(Constants.Tethering.CONNECTION_TIMEOUT_SECONDS) * 1000);
    // Control channel state should become inactive
    waitForTetheringStatus(TetheringStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, false);
    // Stop returning an error from the server.
    serverHandler.setResponseStatus(HttpResponseStatus.OK);
    // Control channel should become active
    waitForTetheringStatus(TetheringStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, true);

    // cleanup
    deleteTethering(SERVER_INSTANCE);
  }

  @Test
  public void testTetheringReject() throws Exception {
    // Server returns 404 before tethering has been accepted or rejected by admin on server side.
    serverHandler.setResponseStatus(HttpResponseStatus.NOT_FOUND);

    // Client initiate tethering with the server
    createTethering(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, REQUEST_TIME, null);

    // Duplicate tethering request should fail
    createTethering(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, REQUEST_TIME, null,
                    HttpResponseStatus.BAD_REQUEST);

    String serverEndpoint = serverConfig.getConnectionConfig().getURI().toString();
    // Control channel should be active at this point.
    waitForTetheringStatus(TetheringStatus.PENDING, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, null, true);

    // Server rejects tethering
    serverHandler.setResponseStatus(HttpResponseStatus.FORBIDDEN);
    // Tethering state should be updated to rejected
    waitForTetheringStatus(TetheringStatus.REJECTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, null, true);

    // Server sends unexpected 200. It should be ignored.
    serverHandler.setResponseStatus(HttpResponseStatus.OK);
    waitForTetheringStatus(TetheringStatus.REJECTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, null, true);

    // Tethering request should be fail as tethering has already been rejected.
    createTethering(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, REQUEST_TIME, null,
                    TetheringStatus.REJECTED,
                    HttpResponseStatus.BAD_REQUEST);

    // cleanup
    deleteTethering(SERVER_INSTANCE);
  }

  @Test
  public void testTetherStatus() throws IOException, InterruptedException {
    // Tethering does not exist, should return 404.
    HttpRequest request = HttpRequest.builder(HttpMethod.GET,
                                              clientConfig.resolveURL("tethering/connections/" + SERVER_INSTANCE))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());

    // Server returns 404 before tethering has been accepted or rejected by admin on server side.
    serverHandler.setResponseStatus(HttpResponseStatus.NOT_FOUND);

    // Client initiate tethering with the server
    createTethering(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, REQUEST_TIME, DESCRIPTION);

    // Tethering status for the peer should be returned.
    request = HttpRequest.builder(HttpMethod.GET,
                                  clientConfig.resolveURL("tethering/connections/" + SERVER_INSTANCE))
      .build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    PeerState peerState = GSON.fromJson(response.getResponseBodyAsString(), PeerState.class);
    Assert.assertEquals(SERVER_INSTANCE, peerState.getName());
    Assert.assertEquals(TetheringStatus.PENDING, peerState.getTetheringStatus());
    Assert.assertTrue(peerState.isActive());
    Assert.assertEquals(serverConfig.getConnectionConfig().getURI().toString(), peerState.getEndpoint());
    Assert.assertEquals(PROJECT, peerState.getMetadata().getMetadata().get("project"));
    Assert.assertEquals(LOCATION, peerState.getMetadata().getMetadata().get("location"));
    Assert.assertEquals(NAMESPACES, peerState.getMetadata().getNamespaceAllocations());

    // cleanup
    deleteTethering(SERVER_INSTANCE);
  }

  @Test
  public void testDeletePendingTether() throws IOException, InterruptedException {
    // Server returns 404 before tethering has been accepted or rejected by admin on server side.
    serverHandler.setResponseStatus(HttpResponseStatus.NOT_FOUND);

    // Client initiate tethering with the server
    createTethering(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, REQUEST_TIME, DESCRIPTION);

    // Delete tethering on the client.
    deleteTethering(SERVER_INSTANCE);
  }

  @Test
  public void testGetTetheringUnknownPeer() throws IOException {
    HttpRequest request = HttpRequest.builder(HttpMethod.GET,
                                              clientConfig.resolveURL("tethering/connections/unknonwn_peer"))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());
  }

  @Test
  public void testDeleteTetheringUnknownPeer() throws IOException {
    HttpRequest request = HttpRequest.builder(HttpMethod.DELETE,
                                              clientConfig.resolveURL("tethering/connections/unknonwn_peer"))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());
  }

  @Test
  public void testTetheringPermissions() throws IOException, InterruptedException {
    Map<String, String> metadata = ImmutableMap.of("project", PROJECT, "location", LOCATION);
    TetheringCreationRequest tetheringRequest = new TetheringCreationRequest(SERVER_INSTANCE,
                                                                             serverConfig.getConnectionConfig()
                                                                               .getURI().toString(),
                                                                             NAMESPACES,
                                                                             metadata,
                                                                             DESCRIPTION);
    HttpRequest.Builder builder = HttpRequest.builder(HttpMethod.PUT, clientConfig.resolveURL("tethering/create"))
      .withBody(GSON.toJson(tetheringRequest));

    // Unprivileged user trying to tether
    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    HttpResponse response = HttpRequests.execute(builder.build());
    Assert.assertEquals(HttpResponseStatus.FORBIDDEN.code(), response.getResponseCode());

    // Privileged user trying to tether
    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    response = HttpRequests.execute(builder.build());
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
  }

  private void deleteTethering(String instance) throws IOException {
    HttpRequest request = HttpRequest.builder(HttpMethod.DELETE,
                                              clientConfig.resolveURL("tethering/connections/" + instance))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
  }

  private void createTethering(String instance, String project, String location,
                               List<NamespaceAllocation> namespaceAllocations,
                               long requestTime, @Nullable String description)
    throws IOException, InterruptedException {
    createTethering(instance, project, location, namespaceAllocations, requestTime, description,
                    TetheringStatus.PENDING);
  }

  private void createTethering(String instance, String project, String location,
                               List<NamespaceAllocation> namespaceAllocations,
                               long requestTime,
                               @Nullable String description,
                               HttpResponseStatus expectedResponseStatus)
    throws IOException, InterruptedException {
    createTethering(instance, project, location, namespaceAllocations, requestTime, description,
                    TetheringStatus.PENDING, expectedResponseStatus);
  }

  private void createTethering(String instance, String project, String location,
                               List<NamespaceAllocation> namespaceAllocations, long requestTime,
                               @Nullable String description, TetheringStatus expectedTetheringStatus)
    throws IOException, InterruptedException {
    createTethering(instance, project, location, namespaceAllocations, requestTime, description,
                    expectedTetheringStatus, HttpResponseStatus.OK);
  }


  private void createTethering(String instance, String project, String location,
                               List<NamespaceAllocation> namespaceAllocations, long requestTime,
                               @Nullable String description, TetheringStatus expectedTetheringStatus,
                               HttpResponseStatus expectedResponseStatus)
    throws IOException, InterruptedException {
    // Send tethering request
    Map<String, String> metadata = ImmutableMap.of("project", project, "location", location);
    TetheringCreationRequest tetheringRequest = new TetheringCreationRequest(instance,
                                                                             serverConfig.getConnectionConfig()
                                                                               .getURI().toString(),
                                                                             namespaceAllocations,
                                                                             metadata,
                                                                             description);
    HttpRequest request = HttpRequest.builder(HttpMethod.PUT, clientConfig.resolveURL("tethering/create"))
      .withBody(GSON.toJson(tetheringRequest))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(expectedResponseStatus.code(), response.getResponseCode());

    waitForTetheringStatus(expectedTetheringStatus, tetheringRequest.getPeer(), tetheringRequest.getEndpoint(),
                           project, location, namespaceAllocations, tetheringRequest.getDescription(), true);
  }

  private void waitForTetheringStatus(TetheringStatus tetheringStatus, String instanceName, String endpoint,
                                      String project, String location, List<NamespaceAllocation> namespaces,
                                      @Nullable String description, boolean expectActive)
    throws IOException, InterruptedException {
    List<PeerState> peers = new ArrayList<>();
    for (int retry = 0; retry < 5; ++retry) {
      HttpRequest request = HttpRequest.builder(HttpMethod.GET, clientConfig.resolveURL("tethering/connections"))
        .build();
      HttpResponse response = HttpRequests.execute(request);
      Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
      Type type = new TypeToken<List<PeerState>>() {
      }.getType();
      peers = GSON.fromJson(response.getResponseBodyAsString(), type);
      Assert.assertEquals(1, peers.size());
      if (peers.get(0).getTetheringStatus() == tetheringStatus
        && peers.get(0).isActive() == expectActive) {
        break;
      }
      Thread.sleep(500);
    }
    Assert.assertEquals(1, peers.size());
    PeerState peer = peers.get(0);
    Assert.assertEquals(tetheringStatus, peer.getTetheringStatus());
    Assert.assertEquals(instanceName, peer.getName());
    Assert.assertEquals(endpoint, peer.getEndpoint());
    Assert.assertEquals(project, peer.getMetadata().getMetadata().get("project"));
    Assert.assertEquals(location, peer.getMetadata().getMetadata().get("location"));
    Assert.assertEquals(namespaces, peer.getMetadata().getNamespaceAllocations());
    Assert.assertEquals(description, peer.getMetadata().getDescription());
    Assert.assertEquals(expectActive, peer.isActive());
  }

  private void waitForTetheringCreated() throws InterruptedException {
    for (int retry = 0; retry < 5; ++retry) {
      if (serverHandler.isTetheringCreated()) {
        return;
      }
      Thread.sleep(500);
    }
    Assert.assertTrue(serverHandler.isTetheringCreated());
  }
}
