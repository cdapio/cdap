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

package io.cdap.cdap.internal.tethering;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.MethodNotAllowedException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProfileConflictException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.metrics.NoOpMetricsSystemClient;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.internal.app.program.MessagingProgramStatePublisher;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedProgramRunner;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.internal.tethering.proto.v1.TetheringLaunchMessage;
import io.cdap.cdap.internal.tethering.runtime.spi.provisioner.TetheringConf;
import io.cdap.cdap.internal.tethering.runtime.spi.provisioner.TetheringProvisioner;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.provisioner.ProvisionerInfo;
import io.cdap.cdap.proto.provisioner.ProvisionerPropertyValue;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.InstancePermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.authorization.DefaultContextAccessEnforcer;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
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
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TetheringServerHandlerTest {
  private static final Gson GSON = new GsonBuilder().create();
  private static final List<NamespaceAllocation> NAMESPACES =
      ImmutableList.of(
          new NamespaceAllocation("testns1", "1", "2Gi"),
          new NamespaceAllocation("testns2", null, null));
  private static final String DESCRIPTION = "my tethering";
  private static final long REQUEST_TIME = System.currentTimeMillis();
  private static TetheringStore tetheringStore;
  private static MessagingService messagingService;
  private static MessagingProgramStatePublisher messagingProgramStatePublisher;
  private static ProfileService profileService;
  private static CConfiguration cConf;
  private static Injector injector;
  private static TransactionManager txManager;
  private static String topicPrefix;

  private NettyHttpService service;
  private ClientConfig config;

  // User having tethering permissions
  private static final Principal MASTER_PRINCIPAL =
      new Principal("master", Principal.PrincipalType.USER);
  // User not having tethering permissions
  private static final Principal UNPRIVILEGED_PRINCIPAL =
      new Principal("unprivileged", Principal.PrincipalType.USER);

  @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws IOException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(Constants.AppFabric.PROGRAM_STATUS_EVENT_NUM_PARTITIONS, 1);
    topicPrefix = cConf.get(Constants.Tethering.CLIENT_TOPIC_PREFIX);
    injector =
        Guice.createInjector(
            new ConfigModule(cConf),
            new SystemDatasetRuntimeModule().getInMemoryModules(),
            new TransactionModules().getInMemoryModules(),
            new TransactionExecutorModule(),
            new InMemoryDiscoveryModule(),
            new LocalLocationModule(),
            new MessagingServerRuntimeModule().getInMemoryModules(),
            new StorageModule(),
            new AuthorizationTestModule(),
            new AuthorizationEnforcementModule().getInMemoryModules(),
            new AuthenticationContextModules().getMasterModule(),
            new PrivateModule() {
              @Override
              protected void configure() {
                bind(MetricsCollectionService.class)
                    .to(NoOpMetricsCollectionService.class)
                    .in(Scopes.SINGLETON);
                expose(MetricsCollectionService.class);
                bind(MetricsSystemClient.class).toInstance(new NoOpMetricsSystemClient());
                expose(MetricsSystemClient.class);
              }
            });
    tetheringStore = new TetheringStore(injector.getInstance(TransactionRunner.class));
    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    messagingProgramStatePublisher = injector.getInstance(MessagingProgramStatePublisher.class);
    profileService = injector.getInstance(ProfileService.class);
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
  }

  @AfterClass
  public static void teardown() {
    if (txManager != null) {
      txManager.stopAndWait();
    }
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  @Before
  public void setUp() throws Exception {
    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));
    cConf.setBoolean(Constants.Tethering.TETHERING_SERVER_ENABLED, true);
    cConf.setInt(Constants.Tethering.CONNECTION_TIMEOUT_SECONDS, 1);

    List<Permission> tetheringPermissions = Arrays.asList(InstancePermission.TETHER);
    InMemoryAccessController inMemoryAccessController = new InMemoryAccessController();
    inMemoryAccessController.grant(
        Authorizable.fromEntityId(InstanceId.SELF),
        MASTER_PRINCIPAL,
        Collections.unmodifiableSet(new HashSet<>(tetheringPermissions)));
    ContextAccessEnforcer contextAccessEnforcer =
        new DefaultContextAccessEnforcer(new AuthenticationTestContext(), inMemoryAccessController);
    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);

    service =
        new CommonNettyHttpServiceBuilder(
                CConfiguration.create(),
                getClass().getSimpleName(),
                new NoOpMetricsCollectionService(), null)
            .setHttpHandlers(
                new TetheringServerHandler(
                    cConf,
                    tetheringStore,
                    messagingService,
                    contextAccessEnforcer,
                    messagingProgramStatePublisher),
                new TetheringHandler(cConf, tetheringStore, messagingService, profileService))
            .build();
    service.start();
    config =
        ClientConfig.builder()
            .setConnectionConfig(
                ConnectionConfig.builder()
                    .setHostname(service.getBindAddress().getHostName())
                    .setPort(service.getBindAddress().getPort())
                    .setSSLEnabled(false)
                    .build())
            .build();
  }

  @After
  public void tearDown() throws IOException {
    // Delete tethering if it exists
    HttpRequest.Builder builder =
        HttpRequest.builder(HttpMethod.DELETE, config.resolveURL("tethering/connections/xyz"));

    HttpResponse response = HttpRequests.execute(builder.build());
    int responseCode = response.getResponseCode();
    Assert.assertTrue(
        responseCode == HttpResponseStatus.OK.code()
            || responseCode == HttpResponseStatus.NOT_FOUND.code());
  }

  @Test
  public void testAcceptTether() throws IOException, InterruptedException {
    // Tethering is initiated by peer
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);
    // Tethering status should be PENDING
    expectTetheringStatus(
        "xyz",
        TetheringStatus.PENDING,
        NAMESPACES,
        REQUEST_TIME,
        DESCRIPTION,
        TetheringConnectionStatus.INACTIVE);

    // Duplicate tether initiation should be ignored
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);
    // Tethering status should be PENDING
    expectTetheringStatus(
        "xyz",
        TetheringStatus.PENDING,
        NAMESPACES,
        REQUEST_TIME,
        DESCRIPTION,
        TetheringConnectionStatus.INACTIVE);

    TetheringControlResponseV2 expectedResponse =
        new TetheringControlResponseV2(Collections.emptyList(), TetheringStatus.PENDING);
    // Tethering status on server side should be PENDING.
    expectTetheringControlResponse("xyz", HttpResponseStatus.OK, GSON.toJson(expectedResponse));

    // User accepts tethering on the server
    acceptTethering();
    // Tethering status should become ACTIVE
    expectTetheringStatus(
        "xyz",
        TetheringStatus.ACCEPTED,
        NAMESPACES,
        REQUEST_TIME,
        DESCRIPTION,
        TetheringConnectionStatus.ACTIVE);

    // Duplicate accept tethering should fail
    TetheringActionRequest request = new TetheringActionRequest("accept");
    HttpRequest.Builder builder =
        HttpRequest.builder(HttpMethod.POST, config.resolveURL("tethering/connections/xyz"))
            .withBody(GSON.toJson(request));
    HttpResponse response = HttpRequests.execute(builder.build());
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getResponseCode());

    // Wait until we don't receive any control messages from the peer for upto the timeout interval.
    Thread.sleep(cConf.getInt(Constants.Tethering.CONNECTION_TIMEOUT_SECONDS) * 1000);
    // Tethering connection status should become INACTIVE
    expectTetheringStatus(
        "xyz",
        TetheringStatus.ACCEPTED,
        NAMESPACES,
        REQUEST_TIME,
        DESCRIPTION,
        TetheringConnectionStatus.INACTIVE);
  }

  @Test
  public void testRejectTether() throws IOException, InterruptedException {
    // Tethering is initiated by peer
    createTethering("xyz", NAMESPACES, REQUEST_TIME, null);
    // Tethering status should be PENDING
    expectTetheringStatus(
        "xyz",
        TetheringStatus.PENDING,
        NAMESPACES,
        REQUEST_TIME,
        null,
        TetheringConnectionStatus.INACTIVE);

    // User rejects tethering
    rejectTethering();
    // Tethering should be deleted
    expectTetheringDeleted("xyz");
  }

  @Test
  public void testConnectControlChannelUnknownPeer() throws IOException {
    HttpRequest request =
        HttpRequest.builder(HttpMethod.POST, config.resolveURL("/tethering/channels/bad_peer"))
            .build();
    HttpResponse response = HttpRequests.execute(request);
    TetheringControlResponseV2 controlResponse =
        GSON.fromJson(response.getResponseBodyAsString(), TetheringControlResponseV2.class);
    Assert.assertEquals(TetheringStatus.NOT_FOUND, controlResponse.getTetheringStatus());
  }

  @Test
  public void testInvalidAction() throws IOException {
    // Create tethering
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);

    TetheringActionRequest invalidAction = new TetheringActionRequest("foo");
    // Perform invalid action, should get BAD_REQUEST error
    HttpRequest request =
        HttpRequest.builder(HttpMethod.POST, config.resolveURL("tethering/connections/xyz"))
            .withBody(GSON.toJson(invalidAction))
            .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getResponseCode());
  }

  @Test
  public void testTetheringTopic() throws IOException, TopicNotFoundException {
    // Create tethering
    createTethering("xyz", NAMESPACES, REQUEST_TIME, null);
    // User accepts tethering
    acceptTethering();

    TopicId topic = new TopicId(NamespaceId.SYSTEM.getNamespace(), topicPrefix + "xyz");
    // Per-peer messaging topic should be created
    Assert.assertNotNull(messagingService.getTopicMetadataProperties(topic));

    // Delete the messaging topic
    messagingService.deleteTopic(topic);
    // The topic should be recreated when a control message is received
    expectTetheringControlResponse("xyz", HttpResponseStatus.OK);
    Assert.assertNotNull(messagingService.getTopicMetadataProperties(topic));

    // Delete tethering
    deleteTethering();

    // Messaging topic should be deleted
    try {
      messagingService.getTopicMetadataProperties(topic);
      Assert.fail(String.format("Messaging topic %s was not deleted", topic.getTopic()));
    } catch (TopicNotFoundException ignored) {
      // expected
    }
  }

  @Test
  public void testInvalidMessageId() throws IOException {
    // Create tethering
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);

    // User accepts tethering
    acceptTethering();

    // control message with invalid message id should return BAD_REQUEST
    HttpRequest.Builder builder =
        HttpRequest.builder(HttpMethod.POST, config.resolveURL("tethering/channels/xyz"));
    builder.withBody(GSON.toJson(new TetheringControlChannelRequest("abcd", null)));
    HttpResponse response = HttpRequests.execute(builder.build());
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getResponseCode());
  }

  @Test
  public void testTetheringPermissions() throws IOException {
    TetheringConnectionRequest tetheringRequest =
        new TetheringConnectionRequest(NAMESPACES, REQUEST_TIME, DESCRIPTION);
    HttpRequest.Builder builder =
        HttpRequest.builder(HttpMethod.PUT, config.resolveURL("tethering/connections/xyz"));
    builder.withBody(GSON.toJson(tetheringRequest));

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
  public void testProcessControlChannelProgramUpdates() throws Exception {
    // Create and accept tethering
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);
    acceptTethering();
    expectTetheringControlResponse("xyz", HttpResponseStatus.OK);

    // Add program update Notifications to body
    HttpRequest.Builder builder =
        HttpRequest.builder(HttpMethod.POST, config.resolveURL("tethering/channels/xyz"));
    ProgramRunId programRunId =
        new ProgramRunId("system", "app", ProgramType.SPARK, "program", "run");
    Notification programUpdate =
        new Notification(
            Notification.Type.PROGRAM_STATUS,
            ImmutableMap.of(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programRunId)));
    TetheringControlChannelRequest content =
        new TetheringControlChannelRequest(null, ImmutableList.of(programUpdate));
    builder.withBody(GSON.toJson(content));
    HttpResponse response = HttpRequests.execute(builder.build());
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    // Check that program update was persisted as a state transition in TMS
    try (CloseableIterator<Message> iterator =
        new MultiThreadMessagingContext(messagingService)
            .getMessageFetcher()
            .fetch(
                NamespaceId.SYSTEM.getNamespace(),
                cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC),
                1,
                null)) {
      Assert.assertTrue(iterator.hasNext());
      Notification notification =
          iterator.next().decodePayload(r -> GSON.fromJson(r, Notification.class));
      Assert.assertEquals(programUpdate, notification);
      Map<String, String> properties = notification.getProperties();
      Assert.assertEquals(
          GSON.toJson(programRunId), properties.get(ProgramOptionConstants.PROGRAM_RUN_ID));
    }
  }

  @Test
  public void testControlResponses() throws Exception {
    // Create and accept tethering
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);
    acceptTethering();
    TetheringControlResponseV2 expectedResponse =
        new TetheringControlResponseV2(Collections.emptyList(), TetheringStatus.ACCEPTED);

    expectTetheringControlResponse("xyz", HttpResponseStatus.OK, GSON.toJson(expectedResponse));

    // Queue up a couple of messages for the peer
    MessagePublisher publisher =
        new MultiThreadMessagingContext(messagingService).getMessagePublisher();
    String topicPrefix = cConf.get(Constants.Tethering.CLIENT_TOPIC_PREFIX);
    String topic = topicPrefix + "xyz";
    TetheringLaunchMessage launchMessage =
        new TetheringLaunchMessage.Builder()
            .addFileNames(DistributedProgramRunner.LOGBACK_FILE_NAME)
            .addFileNames(DistributedProgramRunner.PROGRAM_OPTIONS_FILE_NAME)
            .addFileNames(DistributedProgramRunner.APP_SPEC_FILE_NAME)
            .addRuntimeNamespace("default")
            .build();
    TetheringControlMessage message1 =
        new TetheringControlMessage(
            TetheringControlMessage.Type.START_PROGRAM, Bytes.toBytes(GSON.toJson(launchMessage)));
    publisher.publish(NamespaceId.SYSTEM.getNamespace(), topic, GSON.toJson(message1));
    ProgramRunInfo programRunInfo =
        new ProgramRunInfo.Builder()
            .setNamespace("ns")
            .setApplication("app")
            .setVersion("1.0")
            .setProgramType("workflow")
            .setProgram("program")
            .setRun("runId")
            .build();
    TetheringControlMessage message2 =
        new TetheringControlMessage(
            TetheringControlMessage.Type.STOP_PROGRAM, Bytes.toBytes(GSON.toJson(programRunInfo)));
    publisher.publish(NamespaceId.SYSTEM.getNamespace(), topic, GSON.toJson(message2));

    // Poll the server
    HttpRequest.Builder builder =
        HttpRequest.builder(HttpMethod.POST, config.resolveURL("tethering/channels/xyz"))
            .withBody(GSON.toJson(new TetheringControlChannelRequest(null, null)));

    // Response should contain 2 messages
    HttpResponse response = HttpRequests.execute(builder.build());
    TetheringControlResponseV2 controlResponse =
        GSON.fromJson(response.getResponseBodyAsString(), TetheringControlResponseV2.class);
    List<TetheringControlMessageWithId> controlMessages = controlResponse.getControlMessages();

    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    Assert.assertEquals(2, controlMessages.size());
    Assert.assertEquals(message1, controlMessages.get(0).getControlMessage());
    Assert.assertEquals(message2, controlMessages.get(1).getControlMessage());

    // Poll again with lastMessageId set to id of last message received from the server
    String lastMessageId = controlMessages.get(1).getMessageId();
    builder =
        HttpRequest.builder(HttpMethod.POST, config.resolveURL("tethering/channels/xyz"))
            .withBody(GSON.toJson(new TetheringControlChannelRequest(lastMessageId, null)));

    // There should be no more messages queued up for this client
    response = HttpRequests.execute(builder.build());
    controlResponse =
        GSON.fromJson(response.getResponseBodyAsString(), TetheringControlResponseV2.class);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    Assert.assertEquals(0, controlResponse.getControlMessages().size());
  }

  @Test
  public void testControlMessagesNotSentWhenTetheringNotAccepted() throws Exception {
    // Create tethering, but don't accept it
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);

    // Queue up a message for the peer
    TetheringControlMessage controlMessage =
        new TetheringControlMessage(TetheringControlMessage.Type.KEEPALIVE, new byte[0]);
    MessagePublisher publisher =
        new MultiThreadMessagingContext(messagingService).getMessagePublisher();
    String topicPrefix = cConf.get(Constants.Tethering.CLIENT_TOPIC_PREFIX);
    String topic = topicPrefix + "xyz";
    publisher.publish(NamespaceId.SYSTEM.getNamespace(), topic, GSON.toJson(controlMessage));

    // Poll the server
    HttpRequest.Builder builder =
        HttpRequest.builder(HttpMethod.POST, config.resolveURL("tethering/channels/xyz"))
            .withBody(GSON.toJson(new TetheringControlChannelRequest(null, null)));
    HttpResponse response = HttpRequests.execute(builder.build());
    // Response should not contain any messages because the peer is in PENDING state
    TetheringControlResponseV2 controlResponse =
        GSON.fromJson(response.getResponseBodyAsString(), TetheringControlResponseV2.class);
    Assert.assertTrue(controlResponse.getControlMessages().isEmpty());
  }

  @Test
  public void testDeleteTetheringWhileUsedInProfile()
      throws IOException, MethodNotAllowedException, NotFoundException, ProfileConflictException {
    // Create tethering
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);

    // User accepts tethering
    acceptTethering();

    // Create a profile that uses this tethering
    createTetheringProfile("tethering_profile", "xyz");

    // Tethering deletion should fail because a profile is using this tethering
    HttpRequest request =
        HttpRequest.builder(HttpMethod.DELETE, config.resolveURL("tethering/connections/xyz"))
            .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getResponseCode());

    // Delete the profile
    deleteTetheringProfile("tethering_profile");

    // Create another tethering profile that's associated with a different peer
    createTetheringProfile("profile2", "another_peer");

    // Tethering should be deleted successfully
    deleteTethering();

    // Delete the other profile
    deleteTetheringProfile("profile2");
  }

  @Test
  public void testUpdateTetheringConnectionForAcceptedPeer() throws IOException {
    // Create tethering
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);

    // User accepts tethering
    acceptTethering();

    TetheringControlResponseV2 expectedResponse =
        new TetheringControlResponseV2(Collections.emptyList(), TetheringStatus.ACCEPTED);
    expectTetheringControlResponse("xyz", HttpResponseStatus.OK, GSON.toJson(expectedResponse));
    expectTetheringStatus(
        "xyz",
        TetheringStatus.ACCEPTED,
        NAMESPACES,
        REQUEST_TIME,
        DESCRIPTION,
        TetheringConnectionStatus.ACTIVE);

    // Client removes and recreates tethering with a different list of namespaces and description.
    long requestTime = System.currentTimeMillis();
    List<NamespaceAllocation> updatedNamespaces =
        Collections.singletonList(new NamespaceAllocation("default", null, null));
    String updatedDescription = "recreated tethering connection";
    createTethering("xyz", updatedNamespaces, requestTime, updatedDescription);
    // Tethering connection should have the new list of namespaces and description.
    // Tethering status should be ACCEPTED as the tethering request was accepted previously.
    expectTetheringStatus(
        "xyz",
        TetheringStatus.ACCEPTED,
        updatedNamespaces,
        requestTime,
        updatedDescription,
        TetheringConnectionStatus.ACTIVE);
  }

  @Test
  public void testUpdateTetheringConnectionForPendingPeer() throws IOException {
    // Create tethering
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);

    TetheringControlResponseV2 expectedResponse =
        new TetheringControlResponseV2(Collections.emptyList(), TetheringStatus.PENDING);
    expectTetheringControlResponse("xyz", HttpResponseStatus.OK, GSON.toJson(expectedResponse));
    expectTetheringStatus(
        "xyz",
        TetheringStatus.PENDING,
        NAMESPACES,
        REQUEST_TIME,
        DESCRIPTION,
        TetheringConnectionStatus.ACTIVE);

    // Client removes and recreates tethering with a different list of namespaces and description.
    long requestTime = System.currentTimeMillis();
    List<NamespaceAllocation> updatedNamespaces =
        Collections.singletonList(new NamespaceAllocation("default", null, null));
    String updatedDescription = "recreated tethering connection";
    createTethering("xyz", updatedNamespaces, requestTime, updatedDescription);
    // Tethering connection should have the new list of namespaces and description.
    // Tethering status should be PENDING as the tethering request was accepted previously.
    expectTetheringStatus(
        "xyz",
        TetheringStatus.PENDING,
        updatedNamespaces,
        requestTime,
        updatedDescription,
        TetheringConnectionStatus.ACTIVE);
  }

  @Test
  public void testPeerDeletedDuringTetheringConnectionUpdate()
      throws IOException, InterruptedException {
    // Create tethering
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);

    // User accepts tethering
    acceptTethering();

    expectTetheringControlResponse("xyz", HttpResponseStatus.OK);
    expectTetheringStatus(
        "xyz",
        TetheringStatus.ACCEPTED,
        NAMESPACES,
        REQUEST_TIME,
        DESCRIPTION,
        TetheringConnectionStatus.ACTIVE);

    // Delete tethering
    Thread thread =
        new Thread(
            () -> {
              try {
                deleteTethering();
              } catch (IOException e) {
                Assert.fail("Failed to delete tethering");
              }
            });
    thread.start();

    // Resend the tethering request concurrently with tethering deletion operation.
    // Depending on the ordering we may or may not end with a tethering connection configured,
    // but all operations should be successful.
    long requestTime = System.currentTimeMillis();
    List<NamespaceAllocation> updatedNamespaces =
        Collections.singletonList(new NamespaceAllocation("default", null, null));
    String updatedDescription = "recreated tethering connection";
    createTethering("xyz", updatedNamespaces, requestTime, updatedDescription);
    thread.join();
  }

  @Test
  public void testPollUnknownPeer() throws Exception {
    HttpRequest.Builder builder =
        HttpRequest.builder(HttpMethod.POST, config.resolveURL("tethering/channels/xyz"))
            .withBody(GSON.toJson(new TetheringControlChannelRequest(null, null)));
    HttpResponse resp = HttpRequests.execute(builder.build());
    Assert.assertEquals(404, resp.getResponseCode());
    TetheringControlResponseV2 controlResponse =
        GSON.fromJson(
            resp.getResponseBodyAsString(StandardCharsets.UTF_8), TetheringControlResponseV2.class);
    Assert.assertEquals(TetheringStatus.NOT_FOUND, controlResponse.getTetheringStatus());
  }

  @Test
  public void testAcceptUnknownPeer() throws Exception {
    HttpRequest.Builder builder =
        HttpRequest.builder(HttpMethod.POST, config.resolveURL("tethering/connections/xyz"))
            .withBody(GSON.toJson(new TetheringActionRequest("accept")));
    HttpResponse resp = HttpRequests.execute(builder.build());
    Assert.assertEquals(404, resp.getResponseCode());
    TetheringControlResponseV2 controlResponse =
        GSON.fromJson(
            resp.getResponseBodyAsString(StandardCharsets.UTF_8), TetheringControlResponseV2.class);
    Assert.assertEquals(TetheringStatus.NOT_FOUND, controlResponse.getTetheringStatus());
  }

  private void createTetheringProfile(String profileName, String peer)
      throws MethodNotAllowedException {
    List<ProvisionerPropertyValue> provisionerProperties = new ArrayList<>();
    provisionerProperties.add(
        new ProvisionerPropertyValue(TetheringConf.TETHERED_INSTANCE_PROPERTY, peer, true));
    provisionerProperties.add(
        new ProvisionerPropertyValue(TetheringConf.TETHERED_NAMESPACE_PROPERTY, "default", true));

    ProvisionerInfo provisionerInfo =
        new ProvisionerInfo(TetheringProvisioner.TETHERING_NAME, provisionerProperties);
    Profile profile = new Profile(profileName, "label", "desc", provisionerInfo);
    ProfileId profileId = NamespaceId.DEFAULT.profile(profileName);
    profileService.saveProfile(profileId, profile);
  }

  private void deleteTetheringProfile(String profileName)
      throws MethodNotAllowedException, NotFoundException, ProfileConflictException {
    ProfileId profileId = NamespaceId.DEFAULT.profile(profileName);
    profileService.disableProfile(profileId);
    profileService.deleteProfile(profileId);
  }

  private void expectTetheringControlResponse(String peerName, HttpResponseStatus status)
      throws IOException {
    expectTetheringControlResponse(peerName, status, null);
  }

  private void expectTetheringControlResponse(
      String peerName, HttpResponseStatus status, @Nullable String message) throws IOException {

    HttpRequest.Builder builder =
        HttpRequest.builder(HttpMethod.POST, config.resolveURL("tethering/channels/" + peerName));
    builder.withBody(GSON.toJson(new TetheringControlChannelRequest(null, null)));
    HttpResponse response = HttpRequests.execute(builder.build());
    Assert.assertEquals(status.code(), response.getResponseCode());
    if (message != null) {
      Assert.assertEquals(message, response.getResponseBodyAsString(StandardCharsets.UTF_8));
    }
  }

  private void expectTetheringDeleted(String peerName) throws IOException {
    HttpRequest.Builder builder =
        HttpRequest.builder(HttpMethod.POST, config.resolveURL("tethering/channels/" + peerName));
    builder.withBody(GSON.toJson(new TetheringControlChannelRequest(null, null)));
    HttpResponse response = HttpRequests.execute(builder.build());
    TetheringControlResponseV2 controlResponse =
        GSON.fromJson(response.getResponseBodyAsString(), TetheringControlResponseV2.class);
    Assert.assertEquals(TetheringStatus.NOT_FOUND, controlResponse.getTetheringStatus());
  }

  private void createTethering(
      String peerName,
      List<NamespaceAllocation> namespaces,
      long requestTime,
      @Nullable String description)
      throws IOException {
    TetheringConnectionRequest tetheringRequest =
        new TetheringConnectionRequest(namespaces, requestTime, description);
    doHttpRequest(
        HttpMethod.PUT, "tethering/connections/" + peerName, GSON.toJson(tetheringRequest));
  }

  private List<PeerState> getTetheringStatus() throws IOException {
    Type type = new TypeToken<List<PeerState>>() {}.getType();
    String responseBody = doHttpRequest(HttpMethod.GET, "tethering/connections");
    return GSON.fromJson(responseBody, type);
  }

  private void expectTetheringStatus(
      String peerName,
      TetheringStatus tetheringStatus,
      List<NamespaceAllocation> namespaces,
      long requestTime,
      @Nullable String description,
      TetheringConnectionStatus connectionStatus)
      throws IOException {
    List<PeerState> peerStateList = getTetheringStatus();
    Assert.assertEquals(1, peerStateList.size());
    PeerMetadata expectedPeerMetadata =
        new PeerMetadata(namespaces, Collections.emptyMap(), description);
    PeerState expectedPeerInfo =
        new PeerState(
            peerName, null, tetheringStatus, expectedPeerMetadata, requestTime, connectionStatus);
    Assert.assertEquals(expectedPeerInfo, peerStateList.get(0));
  }

  private void deleteTethering() throws IOException {
    doHttpRequest(HttpMethod.DELETE, "tethering/connections/xyz");
  }

  private void acceptTethering() throws IOException {
    TetheringActionRequest request = new TetheringActionRequest("accept");
    doHttpRequest(HttpMethod.POST, "tethering/connections/xyz", GSON.toJson(request));
  }

  private void rejectTethering() throws IOException {
    TetheringActionRequest request = new TetheringActionRequest("reject");
    doHttpRequest(HttpMethod.POST, "tethering/connections/xyz", GSON.toJson(request));
  }

  private String doHttpRequest(HttpMethod method, String endpoint, @Nullable String body)
      throws IOException {
    HttpRequest.Builder builder = HttpRequest.builder(method, config.resolveURL(endpoint));
    if (body != null) {
      builder = builder.withBody(body);
    }
    HttpResponse response = HttpRequests.execute(builder.build());
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    return response.getResponseBodyAsString(StandardCharsets.UTF_8);
  }

  private String doHttpRequest(HttpMethod method, String endpoint) throws IOException {
    return doHttpRequest(method, endpoint, null);
  }
}
