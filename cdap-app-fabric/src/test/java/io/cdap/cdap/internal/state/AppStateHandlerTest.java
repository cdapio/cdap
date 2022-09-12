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


package io.cdap.cdap.internal.state;

import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.metrics.NoOpMetricsSystemClient;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class AppStateHandlerTest {
  private static final Gson GSON = new Gson();
  public static final String NAMESPACE_1 = "ns1";
  public static final String NAMESPACE_2 = "ns2";
  public static final String APP_NAME = "testapp";
  public static final long APP_ID = 92177123;
  public static final String STATE_KEY = "kafka";
  public static final String STATE_VALUE_STRING = "{\n" +
          "\"offset\" : 12345\n" +
          "}";
  public static final byte[] STATE_VALUE = STATE_VALUE_STRING.getBytes(StandardCharsets.UTF_8);

  private static AppState appState;
  private static String endpoint;

  private static CConfiguration cConf;
  private static AppStateStore appStateStore;
  private static Injector injector;
  private static NamespaceAdmin namespaceAdmin;
  private static TransactionManager txManager;

  private NettyHttpService service;
  private ClientConfig config;

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    injector = Guice.createInjector(
            new ConfigModule(cConf),
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
                bind(MetricsSystemClient.class).toInstance(new NoOpMetricsSystemClient());
                expose(MetricsSystemClient.class);
              }
            });
    appStateStore = new AppStateStore(injector.getInstance(TransactionRunner.class));
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(NAMESPACE_1).build());

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    appState = new AppState(NAMESPACE_1, APP_NAME, APP_ID, STATE_KEY, STATE_VALUE);
    endpoint = "namespaces/" + NAMESPACE_1 + "/app/" + APP_NAME;
  }

  @AfterClass
  public static void teardown() throws Exception {
    namespaceAdmin.delete(new NamespaceId(NAMESPACE_1));
  }

  @Before
  public void setUp() throws Exception {
    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));

    service = new CommonNettyHttpServiceBuilder(CConfiguration.create(), getClass().getSimpleName(),
                                                      new NoOpMetricsCollectionService())
      .setHttpHandlers(new AppStateHandler(namespaceAdmin, appStateStore))
      .build();
    service.start();
    config = ClientConfig.builder()
      .setConnectionConfig(
        ConnectionConfig.builder()
          .setHostname(service.getBindAddress().getHostName())
          .setPort(service.getBindAddress().getPort())
          .setSSLEnabled(false)
          .build()).build();
  }

  @After
  public void tearDown() throws Exception {
    if (txManager != null) {
      txManager.stopAndWait();
    }
  }

  @Test
  public void testAppStateSave() throws IOException {
    HttpResponse response = executeHttpRequest(HttpMethod.POST, endpoint, appState);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
  }

  @Test
  public void testAppStateGet() throws IOException {
    HttpResponse response = executeHttpRequest(HttpMethod.POST, endpoint, appState);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    response = executeHttpRequest(HttpMethod.GET, endpoint, appState);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    Assert.assertEquals(STATE_VALUE, response.getResponseBody());
  }

  @Test
  public void testAppStateDelete() throws IOException {
    HttpResponse response = executeHttpRequest(HttpMethod.POST, endpoint, appState);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    response = executeHttpRequest(HttpMethod.DELETE, endpoint, appState);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
  }

  @Test
  public void testAppStateNamespaceInvalid() throws IOException {
    appState = new AppState(NAMESPACE_2, APP_NAME, APP_ID, STATE_KEY, STATE_VALUE);
    endpoint = "/namespaces/" + NAMESPACE_2 + "/app/" + APP_NAME;

    HttpResponse response = executeHttpRequest(HttpMethod.GET, endpoint, appState);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.code(), response.getResponseCode());
  }

  private HttpResponse executeHttpRequest(HttpMethod method, String endpoint, AppState appStateRequest)
          throws IOException {
    HttpRequest.Builder builder = HttpRequest
            .builder(method, config.resolveURL(endpoint))
            .withBody(GSON.toJson(appStateRequest));
    return HttpRequests.execute(builder.build());
  }
}
