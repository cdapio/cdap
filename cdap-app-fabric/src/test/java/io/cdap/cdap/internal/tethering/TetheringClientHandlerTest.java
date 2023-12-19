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
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.app.runtime.NoOpProgramStateWriter;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.metrics.NoOpMetricsSystemClient;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.StoreProgramRunRecordFetcher;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.internal.provision.ProvisionerNotifier;
import io.cdap.cdap.logging.gateway.handlers.ProgramRunRecordFetcher;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
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
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
  private static ProfileService profileService;
  private static NamespaceAdmin namespaceAdmin;

  private NettyHttpService serverService;
  private ClientConfig serverConfig;
  private NettyHttpService clientService;
  private ClientConfig clientConfig;
  private MockTetheringServerHandler serverHandler;
  private TetheringAgentService tetheringAgentService;
  private TetheringProgramEventPublisher tetheringEventPublisher;
  private MessagingService messagingService;
  private TransactionRunner transactionRunner;

  // User having tethering permissions
  private static final Principal MASTER_PRINCIPAL =
      new Principal("master", Principal.PrincipalType.USER);
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
      new NamespaceAdminTestModule(),
      new LocalLocationModule(),
      new PrivateModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
          expose(MetricsCollectionService.class);
          bind(ProgramStateWriter.class).to(NoOpProgramStateWriter.class).in(Scopes.SINGLETON);
          expose(ProgramStateWriter.class);
          bind(ProgramRunRecordFetcher.class).to(StoreProgramRunRecordFetcher.class).in(Scopes.SINGLETON);
          expose(ProgramRunRecordFetcher.class);
          bind(MetricsSystemClient.class).toInstance(new NoOpMetricsSystemClient());
          expose(MetricsSystemClient.class);
        }
      });
    tetheringStore = new TetheringStore(injector.getInstance(TransactionRunner.class));
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    profileService = injector.getInstance(ProfileService.class);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(NAMESPACE_1).build());
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(NAMESPACE_2).build());
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(NAMESPACE_3).build());
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
    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));
    CConfiguration conf = CConfiguration.create();
    serverHandler = new MockTetheringServerHandler();
    serverService = new CommonNettyHttpServiceBuilder(conf, getClass().getSimpleName() + "_server",
                                                      new NoOpMetricsCollectionService(), null)
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

    messagingService = injector.getInstance(MessagingService.class);
    clientService = new CommonNettyHttpServiceBuilder(conf, getClass().getSimpleName() + "_client",
                                                      new NoOpMetricsCollectionService(), null)
      .setHttpHandlers(new TetheringClientHandler(cConf, tetheringStore, contextAccessEnforcer, namespaceAdmin,
                                                  injector.getInstance(RemoteAuthenticator.class), messagingService),
                       new TetheringHandler(cConf, tetheringStore, messagingService, profileService))
      .build();
    clientService.start();
    clientConfig = ClientConfig.builder()
      .setConnectionConfig(
        ConnectionConfig.builder()
          .setHostname(clientService.getBindAddress().getHostName())
          .setPort(clientService.getBindAddress().getPort())
          .setSSLEnabled(false)
          .build()).build();

    transactionRunner = injector.getInstance(TransactionRunner.class);
    tetheringEventPublisher = new TetheringProgramEventPublisher(cConf, tetheringStore, messagingService,
                                                                 injector.getInstance(ProgramRunRecordFetcher.class),
                                                                 transactionRunner);
    Assert.assertEquals(Service.State.RUNNING, tetheringEventPublisher.startAndWait());
    tetheringAgentService = new TetheringAgentService(cConf,
                                                      tetheringStore,
                                                      injector.getInstance(ProgramStateWriter.class),
                                                      messagingService,
                                                      injector.getInstance(RemoteAuthenticator.class),
                                                      injector.getInstance(LocationFactory.class),
                                                      injector.getInstance(ProvisionerNotifier.class),
                                                      injector.getInstance(NamespaceQueryAdmin.class));
    Assert.assertEquals(Service.State.RUNNING, tetheringAgentService.startAndWait());
  }

  @After
  public void tearDown() throws Exception {
    deleteTetheringIfNeeded(SERVER_INSTANCE);
    Assert.assertEquals(Service.State.TERMINATED, tetheringEventPublisher.stopAndWait());
    Assert.assertEquals(Service.State.TERMINATED, tetheringAgentService.stopAndWait());
  }

  @Test
  public void testTetheringCreationFails() throws IOException {
    // Server responds to tethering request with an error
    serverHandler.setResponseStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);

    Map<String, String> metadata = ImmutableMap.of("project", PROJECT, "location", LOCATION);
    TetheringCreationRequest tetheringRequest = new TetheringCreationRequest(SERVER_INSTANCE,
                                                                             serverConfig.getConnectionConfig()
                                                                               .getURI().toString(),
                                                                             NAMESPACES,
                                                                             metadata,
                                                                             DESCRIPTION);
    // Tethering should fail on the client
    HttpRequest request =
        HttpRequest.builder(HttpMethod.PUT, clientConfig.resolveURL("tethering/create"))
            .withBody(GSON.toJson(tetheringRequest))
            .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(500, response.getResponseCode());

    try {
      tetheringStore.getPeer(SERVER_INSTANCE);
      Assert.fail("Peer should not be added");
    } catch (PeerNotFoundException e) {
      // expected
    }
  }

  @Test
  public void testTetheringDeletedOnServer()
    throws InterruptedException, IOException, PeerAlreadyExistsException {
    Map<String, String> metadata = ImmutableMap.of("project", PROJECT,
                                                   "location", LOCATION);
    PeerMetadata peerMetadata = new PeerMetadata(NAMESPACES, metadata, DESCRIPTION);
    PeerInfo peer = new PeerInfo(SERVER_INSTANCE, serverConfig.getConnectionConfig().getURI().toString(),
                                 TetheringStatus.ACCEPTED, peerMetadata, REQUEST_TIME);

    // Add peer in ACCEPTED state
    tetheringStore.addPeer(peer);

    // Server responds with peer not found error
    serverHandler.setTetheringStatus(TetheringStatus.NOT_FOUND);

    // Tethering status should transition to REJECTED
    String serverEndpoint = serverConfig.getConnectionConfig().getURI().toString();
    waitForTetheringStatus(TetheringStatus.REJECTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, true);
  }

  @Test
  public void testTetheringAccept() throws Exception {
    serverHandler.setTetheringStatus(TetheringStatus.PENDING);
    // Client initiates tethering with the server
    createTethering(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, DESCRIPTION);

    // Server accepts tethering
    serverHandler.setTetheringStatus(TetheringStatus.ACCEPTED);

    String serverEndpoint = serverConfig.getConnectionConfig().getURI().toString();
    // Tethering state should transition to accepted on the client.
    waitForTetheringStatus(TetheringStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, true);

    // Duplicate tethering request should be fail.
    createTethering(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, DESCRIPTION,
                    TetheringStatus.ACCEPTED, HttpResponseStatus.BAD_REQUEST);

    // Make server respond with 500 error for control messages.
    serverHandler.setResponseStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    // Control channel state should become inactive
    waitForTetheringStatus(TetheringStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, false);
    // Stop returning an error from the server.
    serverHandler.setResponseStatus(HttpResponseStatus.OK);
    // Control channel should become active
    waitForTetheringStatus(TetheringStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, true);

    // Server responds with not found, connection should transition to rejected state on client side.
    serverHandler.setTetheringStatus(TetheringStatus.NOT_FOUND);
    // Tethering state should be updated to rejected
    waitForTetheringStatus(TetheringStatus.REJECTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, true);

    // Server sends unexpected 200. It should be ignored.
    serverHandler.setResponseStatus(HttpResponseStatus.OK);
    waitForTetheringStatus(TetheringStatus.REJECTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, true);

    // Tethering request should be fail as tethering has already been rejected.
    createTethering(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, DESCRIPTION,
                    TetheringStatus.REJECTED,
                    HttpResponseStatus.BAD_REQUEST);
  }

  @Test
  public void testProgramStatusUpdate() throws IOException, InterruptedException, TopicNotFoundException {
    serverHandler.setTetheringStatus(TetheringStatus.PENDING);
    // Client initiates tethering with the server
    createTethering(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, DESCRIPTION);

    // Server accepts tethering
    serverHandler.setTetheringStatus(TetheringStatus.ACCEPTED);

    String serverEndpoint = serverConfig.getConnectionConfig().getURI().toString();
    // Tethering state should transition to accepted on the client.
    waitForTetheringStatus(TetheringStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, true);

    // Make server respond with 500 error for control messages.
    serverHandler.setResponseStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);

    ApplicationId application = NamespaceId.DEFAULT.app("app");
    ProgramId program = application.program(ProgramType.WORKFLOW, "program");
    RunId runId = RunIds.generate(System.currentTimeMillis());
    ProgramRunId programRunId = program.run(runId);
    Map<String, String> properties = ImmutableMap.of(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId),
                                                     ProgramOptionConstants.PROGRAM_STATUS,
                                                     ProgramRunStatus.STARTING.name());
    Notification notification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
    Map<String, String> systemArgs = ImmutableMap.of(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName(),
                                                     ProgramOptionConstants.PEER_NAME, SERVER_INSTANCE);
    // Persist STARTING program status and publish on TMS
    TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore metadataStoreDataset = AppMetadataStore.create(context);
      metadataStoreDataset.recordProgramProvisioning(programRunId, Collections.emptyMap(), systemArgs,
                                                     AppFabricTestHelper.createSourceId(1),
                                                     artifactId);
      metadataStoreDataset.recordProgramProvisioned(programRunId, 0,
                                                    AppFabricTestHelper.createSourceId(2));
      metadataStoreDataset.recordProgramStart(programRunId, null, ImmutableMap.of(),
                                              AppFabricTestHelper.createSourceId(3));
    });
    MessagePublisher publisher = new MultiThreadMessagingContext(messagingService).getMessagePublisher();
    String topic = cConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC);
    try {
      messagingService.createTopic(
          new DefaultTopicMetadata(
              new TopicId(NamespaceId.SYSTEM.getNamespace(), topic), Collections.emptyMap()));
    } catch (TopicAlreadyExistsException ex) {
      // no-op
    }
    publisher.publish(NamespaceId.SYSTEM.getNamespace(), topic, GSON.toJson(notification));

    // Server stops returning 500. Client should send pending program state updates.
    serverHandler.setResponseStatus(HttpResponseStatus.OK);

    // Program status update should be sent to the server
    waitForProgramStatus(ProgramRunStatus.STARTING.name());
  }

  @Test
  public void testTetherStatus() throws IOException, InterruptedException {
    // Tethering does not exist, should return 404.
    HttpRequest request =
        HttpRequest.builder(
                HttpMethod.GET, clientConfig.resolveURL("tethering/connections/" + SERVER_INSTANCE))
            .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());

    // Client initiate tethering with the server
    createTethering(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, DESCRIPTION);

    // Tethering status for the peer should be returned.
    request =
        HttpRequest.builder(
                HttpMethod.GET, clientConfig.resolveURL("tethering/connections/" + SERVER_INSTANCE))
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
  }

  @Test
  public void testDeletePendingTethering() throws IOException, InterruptedException {
    // Client initiate tethering with the server
    createTethering(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, DESCRIPTION);
    // Tethering is deleted in tearDown()
  }

  @Test
  public void testGetTetheringUnknownPeer() throws IOException {
    HttpRequest request =
        HttpRequest.builder(
                HttpMethod.GET, clientConfig.resolveURL("tethering/connections/unknonwn_peer"))
            .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());
  }

  @Test
  public void testDeleteTetheringUnknownPeer() throws IOException {
    HttpRequest request =
        HttpRequest.builder(
                HttpMethod.DELETE, clientConfig.resolveURL("tethering/connections/unknonwn_peer"))
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
    HttpRequest.Builder builder =
        HttpRequest.builder(HttpMethod.PUT, clientConfig.resolveURL("tethering/create"))
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

  @Test
  public void testInvalidNamespace() throws IOException {
    List<NamespaceAllocation> namespaces =
        Collections.singletonList(new NamespaceAllocation("foo", null, null));
    TetheringCreationRequest tetheringRequest = new TetheringCreationRequest(SERVER_INSTANCE,
                                                                             serverConfig.getConnectionConfig()
                                                                               .getURI().toString(),
                                                                             namespaces,
                                                                             Collections.emptyMap(),
                                                                             DESCRIPTION);
    HttpRequest.Builder builder =
        HttpRequest.builder(HttpMethod.PUT, clientConfig.resolveURL("tethering/create"))
            .withBody(GSON.toJson(tetheringRequest));

    HttpResponse response = HttpRequests.execute(builder.build());
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());
  }

  @Test
  public void testInvalidEndpoint() throws Exception {
    TetheringCreationRequest tetheringRequest = new TetheringCreationRequest("instance",
                                                                             "invalid_endpoint",
                                                                             Collections.emptyList(),
                                                                             Collections.emptyMap(),
                                                                             "");
    HttpRequest request =
        HttpRequest.builder(HttpMethod.PUT, clientConfig.resolveURL("tethering/create"))
            .withBody(GSON.toJson(tetheringRequest))
            .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getResponseCode());
  }

  @Test
  public void testNullEndpoint() throws Exception {
    TetheringCreationRequest tetheringRequest = new TetheringCreationRequest("instance",
                                                                             null,
                                                                             Collections.emptyList(),
                                                                             Collections.emptyMap(),
                                                                             "");
    HttpRequest request =
        HttpRequest.builder(HttpMethod.PUT, clientConfig.resolveURL("tethering/create"))
            .withBody(GSON.toJson(tetheringRequest))
            .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getResponseCode());
  }

  @Test
  public void testServerReturns404ForConnectedPeer() throws Exception {
    // Client initiates tethering with the server
    createTethering(SERVER_INSTANCE, PROJECT, LOCATION, NAMESPACES, DESCRIPTION);

    // Server accepts tethering
    serverHandler.setTetheringStatus(TetheringStatus.ACCEPTED);

    String serverEndpoint = serverConfig.getConnectionConfig().getURI().toString();
    // Tethering state should transition to accepted on the client.
    waitForTetheringStatus(TetheringStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, true);

    // Server returns not found  on the next poll
    serverHandler.setTetheringStatus(TetheringStatus.NOT_FOUND);

    // Tethering state should transition to rejected on the client.
    waitForTetheringStatus(TetheringStatus.REJECTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, true);
  }

  @Test
  public void testServerReturns404ForPeerInPendingState() throws Exception {
    Map<String, String> metadata = ImmutableMap.of("project", PROJECT,
                                                   "location", LOCATION);
    PeerMetadata peerMetadata = new PeerMetadata(NAMESPACES, metadata, DESCRIPTION);
    PeerInfo peer = new PeerInfo(SERVER_INSTANCE, serverConfig.getConnectionConfig().getURI().toString(),
                                 TetheringStatus.PENDING, peerMetadata, REQUEST_TIME);

    // Add peer in PENDING state
    tetheringStore.addPeer(peer);

    // Server returns not found
    serverHandler.setTetheringStatus(TetheringStatus.NOT_FOUND);

    // Tethering status should transition to REJECTED
    String serverEndpoint = serverConfig.getConnectionConfig().getURI().toString();
    waitForTetheringStatus(TetheringStatus.REJECTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, false);
  }

  @Test
  public void testServerNotFound() throws Exception {
    Map<String, String> metadata = ImmutableMap.of("project", PROJECT,
                                                   "location", LOCATION);
    PeerMetadata peerMetadata = new PeerMetadata(NAMESPACES, metadata, DESCRIPTION);
    PeerInfo peer = new PeerInfo(SERVER_INSTANCE, serverConfig.getConnectionConfig().getURI().toString(),
                                 TetheringStatus.ACCEPTED, peerMetadata, REQUEST_TIME);

    // Add peer in ACCEPTED state
    tetheringStore.addPeer(peer);
    serverHandler.setResponseStatus(HttpResponseStatus.OK);
    String serverEndpoint = serverConfig.getConnectionConfig().getURI().toString();
    waitForTetheringStatus(TetheringStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, true);

    // Server is unreachable so 404 error is returned, but not from TetheringServerHandler
    serverHandler.setResponseStatus(HttpResponseStatus.NOT_FOUND);

    // Tethering status should remain ACCEPTED, but connection becomes inactive
    waitForTetheringStatus(TetheringStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, false);
  }

  @Test
  public void testServerStatusRejected() throws Exception {
    Map<String, String> metadata = ImmutableMap.of("project", PROJECT,
                                                   "location", LOCATION);
    PeerMetadata peerMetadata = new PeerMetadata(NAMESPACES, metadata, DESCRIPTION);
    PeerInfo peer = new PeerInfo(SERVER_INSTANCE, serverConfig.getConnectionConfig().getURI().toString(),
                                 TetheringStatus.ACCEPTED, peerMetadata, REQUEST_TIME);

    // Add peer in ACCEPTED state
    tetheringStore.addPeer(peer);
    serverHandler.setResponseStatus(HttpResponseStatus.OK);
    String serverEndpoint = serverConfig.getConnectionConfig().getURI().toString();
    waitForTetheringStatus(TetheringStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, true);

    // Server's tethering status is REJECTED
    serverHandler.setTetheringStatus(TetheringStatus.REJECTED);

    // Tethering status should transition to REJECTED state.
    waitForTetheringStatus(TetheringStatus.ACCEPTED, SERVER_INSTANCE, serverEndpoint,
                           PROJECT, LOCATION, NAMESPACES, DESCRIPTION, true);
  }

  private void deleteTetheringIfNeeded(String peer) throws IOException {
    if (!tetheringExists(peer)) {
      // Tethering connection doesn't exist, nothing to do here.
      return;
    }
    HttpRequest request =
        HttpRequest.builder(
                HttpMethod.DELETE, clientConfig.resolveURL("tethering/connections/" + peer))
            .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
  }

  private boolean tetheringExists(String peer) throws IOException {
    HttpRequest request =
        HttpRequest.builder(
                HttpMethod.GET, clientConfig.resolveURL("tethering/connections/" + peer))
            .build();
    HttpResponse response = HttpRequests.execute(request);
    int responseCode = response.getResponseCode();
    Assert.assertTrue(responseCode == 200 || responseCode == 404);
    return responseCode == 200;
  }

  private void createTethering(String instance, String project, String location,
                               List<NamespaceAllocation> namespaceAllocations,
                               @Nullable String description)
    throws IOException, InterruptedException {
    createTethering(instance, project, location, namespaceAllocations, description,
                    TetheringStatus.PENDING);
  }

  private void createTethering(String instance, String project, String location,
                               List<NamespaceAllocation> namespaceAllocations,
                               @Nullable String description, TetheringStatus expectedTetheringStatus)
    throws IOException, InterruptedException {
    createTethering(instance, project, location, namespaceAllocations, description,
                    expectedTetheringStatus, HttpResponseStatus.OK);
  }


  private void createTethering(String instance, String project, String location,
                               List<NamespaceAllocation> namespaceAllocations,
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
    HttpRequest request =
        HttpRequest.builder(HttpMethod.PUT, clientConfig.resolveURL("tethering/create"))
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
    for (int retry = 0; retry < 10; ++retry) {
      HttpRequest request =
          HttpRequest.builder(HttpMethod.GET, clientConfig.resolveURL("tethering/connections"))
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
      Thread.sleep(1000);
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

  private void waitForProgramStatus(String programStatus) throws InterruptedException {
    for (int retry = 0; retry < 10; ++retry) {
      if (programStatus.equals(serverHandler.getProgramStatus())) {
        return;
      }
      Thread.sleep(1000);
    }
    Assert.assertEquals(programStatus, serverHandler.getProgramStatus());
  }
}
