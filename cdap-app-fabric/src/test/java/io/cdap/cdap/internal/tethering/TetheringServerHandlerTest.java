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
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import javax.annotation.Nullable;

public class TetheringServerHandlerTest {
  private static final Gson GSON = new GsonBuilder().create();
  private static final List<NamespaceAllocation> NAMESPACES = ImmutableList.of(
    new NamespaceAllocation("testns1", "1", "2Gi"),
    new NamespaceAllocation("testns2", null, null));
  private static final String DESCRIPTION = "my tethering";
  private static final long REQUEST_TIME = System.currentTimeMillis();
  private static TetheringStore tetheringStore;
  private static MessagingService messagingService;
  private static CConfiguration cConf;
  private static Injector injector;
  private static TransactionManager txManager;
  private static String topicPrefix;

  private NettyHttpService service;
  private ClientConfig config;

  // User having tethering permissions
  private static final Principal MASTER_PRINCIPAL = new Principal("master", Principal.PrincipalType.USER);
  // User not having tethering permissions
  private static final Principal UNPRIVILEGED_PRINCIPAL = new Principal("unprivileged",
                                                                        Principal.PrincipalType.USER);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws IOException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    topicPrefix = cConf.get(Constants.Tethering.TOPIC_PREFIX);
    injector = Guice.createInjector(
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
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
          expose(MetricsCollectionService.class);
        }
      });
    tetheringStore = new TetheringStore(injector.getInstance(TransactionRunner.class));
    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
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
    inMemoryAccessController.grant(Authorizable.fromEntityId(InstanceId.SELF), MASTER_PRINCIPAL,
                                   Collections.unmodifiableSet(new HashSet<>(tetheringPermissions)));
    ContextAccessEnforcer contextAccessEnforcer =
      new DefaultContextAccessEnforcer(new AuthenticationTestContext(), inMemoryAccessController);
    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);

    service = new CommonNettyHttpServiceBuilder(CConfiguration.create(), getClass().getSimpleName())
      .setHttpHandlers(
        new TetheringServerHandler(cConf, tetheringStore, messagingService, contextAccessEnforcer),
        new TetheringHandler(cConf, tetheringStore, messagingService)).build();
    service.start();
    config = ClientConfig.builder()
      .setConnectionConfig(
        ConnectionConfig.builder()
          .setHostname(service.getBindAddress().getHostName())
          .setPort(service.getBindAddress().getPort())
          .setSSLEnabled(false)
          .build()).build();
  }

  @Test
  public void testAcceptTether() throws IOException, InterruptedException {
    // Tethering is initiated by peer
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);
    // Tethering status should be PENDING
    expectTetheringStatus("xyz", TetheringStatus.PENDING, NAMESPACES, REQUEST_TIME, DESCRIPTION,
                          TetheringConnectionStatus.INACTIVE);

    // Duplicate tether initiation should be ignored
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);
    // Tethering status should be PENDING
    expectTetheringStatus("xyz", TetheringStatus.PENDING, NAMESPACES, REQUEST_TIME, DESCRIPTION,
                          TetheringConnectionStatus.INACTIVE);

    // Server should respond with 404 because tether is still pending.
    expectTetheringControlResponse("xyz", HttpResponseStatus.NOT_FOUND);

    // User accepts tethering
    acceptTethering();
    expectTetheringStatus("xyz", TetheringStatus.ACCEPTED, NAMESPACES, REQUEST_TIME, DESCRIPTION,
                          TetheringConnectionStatus.ACTIVE);

    // Duplicate accept tethering should be ignored
    acceptTethering();
    expectTetheringControlResponse("xyz", HttpResponseStatus.OK);
    expectTetheringStatus("xyz", TetheringStatus.ACCEPTED, NAMESPACES, REQUEST_TIME, DESCRIPTION,
                          TetheringConnectionStatus.ACTIVE);

    // Reject tethering should be ignored
    rejectTethering();
    expectTetheringControlResponse("xyz", HttpResponseStatus.OK);

    // Tethering initiation should be ignored
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);
    expectTetheringControlResponse("xyz", HttpResponseStatus.OK);
    expectTetheringStatus("xyz", TetheringStatus.ACCEPTED, NAMESPACES, REQUEST_TIME, DESCRIPTION,
                          TetheringConnectionStatus.ACTIVE);

    // Wait until we don't receive any control messages from the peer for upto the timeout interval.
    Thread.sleep(cConf.getInt(Constants.Tethering.CONNECTION_TIMEOUT_SECONDS) * 1000);
    expectTetheringStatus("xyz", TetheringStatus.ACCEPTED, NAMESPACES, REQUEST_TIME, DESCRIPTION,
                          TetheringConnectionStatus.INACTIVE);

    // Delete tethering
    deleteTethering();
  }

  @Test
  public void testRejectTether() throws IOException, InterruptedException {
    // Tethering is initiated by peer
    createTethering("xyz", NAMESPACES, REQUEST_TIME, null);
    // Tethering status should be PENDING
    expectTetheringStatus("xyz", TetheringStatus.PENDING, NAMESPACES, REQUEST_TIME, null,
                          TetheringConnectionStatus.INACTIVE);

    // User rejects tethering
    rejectTethering();
    // Server should return 403 when tethering is rejected.
    expectTetheringControlResponse("xyz", HttpResponseStatus.FORBIDDEN);
    expectTetheringStatus("xyz", TetheringStatus.REJECTED, NAMESPACES, REQUEST_TIME, null,
                          TetheringConnectionStatus.ACTIVE);


    // Duplicate reject tethering should be ignored
    rejectTethering();
    expectTetheringControlResponse("xyz", HttpResponseStatus.FORBIDDEN);
    expectTetheringStatus("xyz", TetheringStatus.REJECTED, NAMESPACES, REQUEST_TIME, null,
                          TetheringConnectionStatus.ACTIVE);

    // Accept tethering should be ignored
    acceptTethering();
    expectTetheringControlResponse("xyz", HttpResponseStatus.FORBIDDEN);
    expectTetheringStatus("xyz", TetheringStatus.REJECTED, NAMESPACES, REQUEST_TIME, null,
                          TetheringConnectionStatus.ACTIVE);

    // Delete tethering
    deleteTethering();
  }

  @Test
  public void testConnectControlChannelUnknownPeer() throws IOException {
    HttpRequest request = HttpRequest.builder(HttpMethod.GET,
                                              config.resolveURL("/tethering/controlchannels/bad_peer")).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());
  }

  @Test
  public void testInvalidAction() throws IOException {
    // Create tethering
    createTethering("xyz", NAMESPACES, REQUEST_TIME, DESCRIPTION);

    TetheringActionRequest invalidAction = new TetheringActionRequest("foo");
    // Perform invalid action, should get BAD_REQUEST error
    HttpRequest request = HttpRequest.builder(HttpMethod.POST,
                                              config.resolveURL("tethering/connections/xyz"))
      .withBody(GSON.toJson(invalidAction))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getResponseCode());

    // Delete tethering
    deleteTethering();
  }

  @Test
  public void testTetheringTopic() throws IOException, TopicNotFoundException {
    // Create tethering
    createTethering("xyz", NAMESPACES, REQUEST_TIME, null);

    TopicId topic = new TopicId(NamespaceId.SYSTEM.getNamespace(),
                                topicPrefix + "xyz");
    // Per-peer messaging topic should be created
    TopicMetadata metadata = messagingService.getTopic(topic);
    Assert.assertEquals(topic, metadata.getTopicId());

    // Delete tethering
    deleteTethering();

    // Messaging topic should be deleted
    try {
      messagingService.getTopic(topic);
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
    HttpRequest.Builder builder = HttpRequest.builder(HttpMethod.GET,
                                                      config.resolveURL(
                                                        "tethering/controlchannels/xyz?messageId=abcd"));
    HttpResponse response = HttpRequests.execute(builder.build());
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getResponseCode());

    // Delete tethering
    deleteTethering();
  }

  @Test
  public void testTetheringPermissions() throws IOException {
    TetheringConnectionRequest tetheringRequest = new TetheringConnectionRequest(NAMESPACES, REQUEST_TIME, DESCRIPTION);
    HttpRequest.Builder builder = HttpRequest.builder(HttpMethod.PUT, config.resolveURL("tethering/connections/xyz"));
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

  private void expectTetheringControlResponse(String peerName, HttpResponseStatus status) throws IOException {

    HttpRequest.Builder builder = HttpRequest.builder(HttpMethod.GET,
                                                      config.resolveURL("tethering/controlchannels/" + peerName));
    HttpResponse response = HttpRequests.execute(builder.build());
    Assert.assertEquals(status.code(), response.getResponseCode());
  }

  private void createTethering(String peerName, List<NamespaceAllocation> namespaces,
                               long requestTime,
                               @Nullable String description) throws IOException {
    TetheringConnectionRequest tetheringRequest = new TetheringConnectionRequest(namespaces, requestTime, description);
    doHttpRequest(HttpMethod.PUT, "tethering/connections/" + peerName, GSON.toJson(tetheringRequest));
  }

  private List<PeerState> getTetheringStatus() throws IOException {
    Type type = new TypeToken<List<PeerState>>() { }.getType();
    String responseBody = doHttpRequest(HttpMethod.GET, "tethering/connections");
    return GSON.fromJson(responseBody, type);
  }

  private void expectTetheringStatus(String peerName, TetheringStatus tetheringStatus,
                                     List<NamespaceAllocation> namespaces,
                                     long requestTime,
                                     @Nullable String description,
                                     TetheringConnectionStatus connectionStatus)
    throws IOException {
    List<PeerState> peerStateList = getTetheringStatus();
    Assert.assertEquals(1, peerStateList.size());
    PeerMetadata expectedPeerMetadata = new PeerMetadata(namespaces, Collections.emptyMap(), description);
    PeerState expectedPeerInfo = new PeerState(peerName, null, tetheringStatus,
                                               expectedPeerMetadata, requestTime, connectionStatus);
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

  private String doHttpRequest(HttpMethod method, String endpoint, @Nullable String body) throws IOException {
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
