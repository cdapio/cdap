/*
 * Copyright © 2022 Cask Data, Inc.
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


package io.cdap.cdap.internal.app.store.state;

import com.google.inject.Injector;
import io.cdap.cdap.ConfigTestApp;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.gateway.handlers.AppStateHandler;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.tephra.TransactionManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AppStateHandlerTest extends AppFabricTestBase {

  public static final String NAMESPACE_1 = "ns1";
  public static final String NAMESPACE_2 = "ns2";
  public static final String APP_NAME = "testapp";
  public static final String APP_NAME_2 = "testapp2";
  public static final String STATE_KEY = "kafka";
  public static final String STATE_VALUE = "{\n"
      + "\"offset\" : 12345\n"
      + "}";

  private static String endpoint;
  private static ApplicationLifecycleService applicationLifecycleService;
  private static NamespaceAdmin namespaceAdmin;
  private static TransactionManager txManager;
  private static Id.Artifact artifactId;
  private static Id.Application appId;

  private ClientConfig config;

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    Injector injector = getInjector();

    // Add a new namespace
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(NAMESPACE_1).build());

    applicationLifecycleService = injector.getInstance(ApplicationLifecycleService.class);
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    // Endpoint for all state APIs
    endpoint = "namespaces/" + NAMESPACE_1 + "/apps/" + APP_NAME + "/states/" + STATE_KEY;
    artifactId = Id.Artifact.from(Id.Namespace.from(NAMESPACE_1), "appWithConfig",
                                  "1.0.0-SNAPSHOT");
    appId = Id.Application.from(NAMESPACE_1, APP_NAME);
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (namespaceAdmin != null) {
      namespaceAdmin.delete(new NamespaceId(NAMESPACE_1));
    }

    if (txManager != null) {
      txManager.stopAndWait();
    }
  }

  @Before
  public void setUp() throws Exception {
    NettyHttpService service = new CommonNettyHttpServiceBuilder(CConfiguration.create(), getClass().getSimpleName(),
                                                                 new NoOpMetricsCollectionService(),
                                                                 auditLogContexts -> {})
      .setHttpHandlers(new AppStateHandler(applicationLifecycleService, namespaceAdmin))
      .build();
    service.start();
    config = ClientConfig.builder()
                         .setApiVersion(Constants.Gateway.INTERNAL_API_VERSION_3_TOKEN)
                         .setConnectionConfig(
                           ConnectionConfig.builder()
                                           .setHostname(service.getBindAddress().getHostName())
                                           .setPort(service.getBindAddress().getPort())
                                           .setSSLEnabled(false)
                                           .build()).build();

    addAppArtifact(artifactId, ConfigTestApp.class);
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    deploy(appId, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId()), config));
  }

  @After
  public void tearDown() throws Exception {
    // Cleanup
    try {
      executeHttpRequest(HttpMethod.DELETE, endpoint, null);
    } catch (Exception e) {
      // Exception because state might already have been deleted.
      // Don't do anything.
    }

    deleteApp(appId, 200);
    deleteArtifact(artifactId, 200);
  }

  @Test
  public void testAppStateSave() throws IOException {
    // Save state
    HttpResponse response = executeHttpRequest(HttpMethod.PUT, endpoint, STATE_VALUE);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
  }

  @Test
  public void testAppStateGet() throws IOException {
    // Save state
    HttpResponse response = executeHttpRequest(HttpMethod.PUT, endpoint, STATE_VALUE);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    // Read state
    response = executeHttpRequest(HttpMethod.GET, endpoint, null);

    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    Assert.assertArrayEquals(STATE_VALUE.getBytes(StandardCharsets.UTF_8), response.getResponseBody());
  }

  @Test
  public void testAppStateDelete() throws IOException {
    // Save state
    HttpResponse response = executeHttpRequest(HttpMethod.PUT, endpoint, STATE_VALUE);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());

    // Delete state
    response = executeHttpRequest(HttpMethod.DELETE, endpoint, null);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
  }

  @Test
  public void testAppStateNamespaceInvalid() throws IOException {
    String endpoint = "namespaces/" + NAMESPACE_2 + "/apps/" + APP_NAME + "/states/" + STATE_KEY;

    // Get state with invalid namespace
    HttpResponse response = executeHttpRequest(HttpMethod.GET, endpoint, null);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());
  }

  @Test
  public void testAppNameInvalid() throws IOException {
    String endpoint = "namespaces/" + NAMESPACE_1 + "/apps/" + APP_NAME_2 + "/states/" + STATE_KEY;

    // Get state with invalid namespace
    HttpResponse response = executeHttpRequest(HttpMethod.GET, endpoint, null);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.code(), response.getResponseCode());
  }

  @Test
  public void testAppStateDoesNotExist() throws IOException {
    // Get state that does not exist in table, but the app is valid
    HttpResponse response = executeHttpRequest(HttpMethod.GET, endpoint, null);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
    Assert.assertArrayEquals(new byte[]{}, response.getResponseBody());
  }

  private HttpResponse executeHttpRequest(HttpMethod method, String endpoint, String body)
    throws IOException {
    HttpRequest.Builder httpRequest = HttpRequest
      .builder(method, config.resolveURL(endpoint));
    if (body != null) {
      httpRequest.withBody(body);
    }
    return HttpRequests.execute(httpRequest.build());
  }
}
