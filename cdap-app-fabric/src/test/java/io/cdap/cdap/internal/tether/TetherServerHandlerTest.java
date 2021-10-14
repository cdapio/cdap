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
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

public class TetherServerHandlerTest {
  private static final Gson GSON = new GsonBuilder().create();
  private static final List<NamespaceAllocation> NAMESPACES = ImmutableList.of(
    new NamespaceAllocation("testns1", "1", "2Gi"),
    new NamespaceAllocation("testns2", null, null));
  private static TetherStore tetherStore;
  private static MessagingService messagingService;
  private static CConfiguration cConf;
  private static Injector injector;
  private static TransactionManager txManager;

  private NettyHttpService service;
  private ClientConfig config;

  @BeforeClass
  public static void setup() {
    cConf = CConfiguration.create();
    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new TransactionModules().getInMemoryModules(),
      new TransactionExecutorModule(),
      new InMemoryDiscoveryModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new StorageModule(),
      new PrivateModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
          expose(MetricsCollectionService.class);
        }
      });
    tetherStore = new TetherStore(injector.getInstance(TransactionRunner.class));
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
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
    structuredTableRegistry.initialize();
    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class),
                                    structuredTableRegistry);
    cConf.setBoolean(Constants.Tether.TETHER_SERVER_ENABLE, true);
    cConf.setInt(Constants.Tether.CONNECTION_TIMEOUT, 1);
    service = new CommonNettyHttpServiceBuilder(CConfiguration.create(), getClass().getSimpleName())
      .setHttpHandlers(new TetherServerHandler(cConf, tetherStore, messagingService, transactionRunner),
                       new TetherHandler(cConf, tetherStore))
      .build();
    service.start();
    config = ClientConfig.builder()
      .setConnectionConfig(
        ConnectionConfig.builder()
          .setHostname(service.getBindAddress().getHostName())
          .setPort(service.getBindAddress().getPort())
          .setSSLEnabled(false)
          .build()).build();

    messagingService.deleteTopic(new TopicId(NamespaceId.SYSTEM.getNamespace(), "tethering_xyz"));
  }

  @Test
  public void testAcceptTether() throws IOException, InterruptedException {
    // Tether is initiated by peer
    createTether("xyz", NAMESPACES);
    // Tether status should be PENDING
    expectTetherStatus("xyz", TetherStatus.PENDING, NAMESPACES, TetherConnectionStatus.INACTIVE);

    // Duplicate tether initiation should be ignored
    createTether("xyz", NAMESPACES);
    // Tether status should be PENDING
    expectTetherStatus("xyz", TetherStatus.PENDING, NAMESPACES, TetherConnectionStatus.INACTIVE);

    // Server should respond with 404 because tether is still pending.
    expectTetherControlResponse("xyz", HttpResponseStatus.NOT_FOUND);

    // User accepts tethering
    acceptTether();
    expectTetherStatus("xyz", TetherStatus.ACCEPTED, NAMESPACES, TetherConnectionStatus.ACTIVE);

    // Duplicate accept tethering should be ignored
    acceptTether();
    expectTetherControlResponse("xyz", HttpResponseStatus.OK);
    expectTetherStatus("xyz", TetherStatus.ACCEPTED, NAMESPACES, TetherConnectionStatus.ACTIVE);

    // Reject tethering should be ignored
    rejectTether();
    expectTetherControlResponse("xyz", HttpResponseStatus.OK);

    // Wait until we don't receive any control messages from the peer for upto the timeout interval.
    Thread.sleep(cConf.getInt(Constants.Tether.CONNECTION_TIMEOUT) * 1000);
    expectTetherStatus("xyz", TetherStatus.ACCEPTED, NAMESPACES, TetherConnectionStatus.INACTIVE);

    // Delete tethering
    deleteTether();
  }

  @Test
  public void testRejectTether() throws IOException, InterruptedException {
    // Tether is initiated by peer
    createTether("xyz", NAMESPACES);
    // Tether status should be PENDING
    expectTetherStatus("xyz", TetherStatus.PENDING, NAMESPACES, TetherConnectionStatus.INACTIVE);

    // User rejects tethering
    rejectTether();
    // Server should return 403 when tethering is rejected.
    expectTetherControlResponse("xyz", HttpResponseStatus.FORBIDDEN);
    expectTetherStatus("xyz", TetherStatus.REJECTED, NAMESPACES, TetherConnectionStatus.ACTIVE);


    // Duplicate reject tethering should be ignored
    rejectTether();
    expectTetherControlResponse("xyz", HttpResponseStatus.FORBIDDEN);
    expectTetherStatus("xyz", TetherStatus.REJECTED, NAMESPACES, TetherConnectionStatus.ACTIVE);

    // Accept tethering should be ignored
    acceptTether();
    expectTetherControlResponse("xyz", HttpResponseStatus.FORBIDDEN);
    expectTetherStatus("xyz", TetherStatus.REJECTED, NAMESPACES, TetherConnectionStatus.ACTIVE);

    // Delete tethering
    deleteTether();
  }

  private void expectTetherControlResponse(String peerName, HttpResponseStatus status) throws IOException {

    HttpRequest.Builder builder = HttpRequest.builder(HttpMethod.GET,
                                                      config.resolveURL("tethering/controlchannels/" + peerName));

    HttpResponse response = HttpRequests.execute(builder.build());
    Assert.assertEquals(status.code(), response.getResponseCode());
  }

  private void createTether(String peerName, List<NamespaceAllocation> namespaces) throws IOException {
    TetherConnectionRequest tetherRequest = new TetherConnectionRequest(peerName, namespaces);
    doHttpRequest(HttpMethod.POST, "tethering/connect", GSON.toJson(tetherRequest));

  }

  private List<PeerStatus> getTetherStatus() throws IOException {
    Type type = new TypeToken<List<PeerStatus>>() { }.getType();
    String responseBody = doHttpRequest(HttpMethod.GET, "tethering/connections");
    return GSON.fromJson(responseBody, type);
  }

  private void expectTetherStatus(String peerName, TetherStatus tetherStatus,
                                  List<NamespaceAllocation> namespaces, TetherConnectionStatus connectionStatus)
    throws IOException {
    List<PeerStatus> peerStatusList = getTetherStatus();
    Assert.assertEquals(1, peerStatusList.size());
    PeerMetadata expectedPeerMetadata = new PeerMetadata(namespaces, Collections.emptyMap());
    PeerStatus expectedPeerInfo = new PeerStatus(peerName, null, tetherStatus,
                                                 expectedPeerMetadata, connectionStatus);
    Assert.assertEquals(expectedPeerInfo, peerStatusList.get(0));
  }

  private void deleteTether() throws IOException {
    doHttpRequest(HttpMethod.DELETE, "tethering/connections/xyz");
  }

  private void acceptTether() throws IOException {
    doHttpRequest(HttpMethod.POST, "tethering/connections/xyz/accept");
  }

  private void rejectTether() throws IOException {
    doHttpRequest(HttpMethod.POST, "tethering/connections/xyz/reject");
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
