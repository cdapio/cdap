/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.InternalRouter;
import io.cdap.cdap.common.conf.Constants.Service;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.gateway.handlers.PingHandler;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.NettyHttpService;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.EnumSet;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for the {@link InternalServiceRoutingHandler}.
 */
public class InternalServiceRoutingHandlerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final String MOCK_SERVICE = "mock";

  private Injector injector;
  private InternalRouterService internalRouterService;
  private NettyHttpService mockService;
  private Cancellable mockServiceCancellable;
  private CConfiguration cConf;

  @Before
  public void beforeTest() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR,
        TEMP_FOLDER.newFolder().getAbsolutePath());

    injector = Guice.createInjector(
        new ConfigModule(cConf),
        new LocalLocationModule(),
        new InMemoryDiscoveryModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).to(
                NoOpMetricsCollectionService.class);
            bind(InternalAuthenticator.class).to(
                NoOpInternalAuthenticator.class);
            bind(RemoteAuthenticator.class).to(NoOpRemoteAuthenticator.class);
          }
        }
    );

    internalRouterService = injector.getInstance(InternalRouterService.class);
    internalRouterService.startAndWait();

    mockService = NettyHttpService.builder(MOCK_SERVICE)
        .setHost(InetAddress.getLocalHost().getCanonicalHostName())
        .setHttpHandlers(new PingHandler(), new MockServiceHandler())
        .build();

    mockService.start();
    mockServiceCancellable = injector.getInstance(DiscoveryService.class)
        .register(URIScheme.createDiscoverable(MOCK_SERVICE, mockService));
  }

  @After
  public void afterTest() throws Exception {
    mockServiceCancellable.cancel();
    mockService.stop();
    internalRouterService.stopAndWait();
  }

  @Test
  public void testGetAndDelete() throws IOException, UnauthorizedException {
    RemoteClient remoteClient = injector.getInstance(RemoteClientFactory.class)
        .createRemoteClient(
            Constants.Service.INTERNAL_ROUTER,
            DefaultHttpRequestConfig.DEFAULT,
            Constants.Gateway.INTERNAL_API_VERSION_3 + "/router");

    for (HttpMethod method : EnumSet.of(HttpMethod.GET, HttpMethod.DELETE)) {
      for (int status : Arrays.asList(200, 400, 404, 501)) {
        HttpRequest request = remoteClient.requestBuilder(method,
                String.format("services/%s/mock/%s/%d",
                    MOCK_SERVICE, method.name().toLowerCase(), status))
            .build();

        HttpResponse response = remoteClient.execute(request);
        Assert.assertEquals(status, response.getResponseCode());
      }
    }
  }

  @Test
  public void testPutAndPost() throws IOException, UnauthorizedException {
    RemoteClient remoteClient = injector.getInstance(RemoteClientFactory.class)
        .createRemoteClient(
            Service.INTERNAL_ROUTER,
            DefaultHttpRequestConfig.DEFAULT,
            Constants.Gateway.INTERNAL_API_VERSION_3 + "/router");
    String largeContent = Strings.repeat("Testing", 32768);

    for (String content : Arrays.asList("", "Small content", largeContent)) {
      for (HttpMethod method : EnumSet.of(HttpMethod.PUT, HttpMethod.POST)) {
        for (int status : Arrays.asList(200, 400, 404, 501)) {
          HttpRequest request =
              remoteClient.requestBuilder(method,
                      String.format("services/%s/mock/%s/%d",
                          MOCK_SERVICE, method.name().toLowerCase(), status))
                  .withBody(content)
                  .build();

          HttpResponse response = remoteClient.execute(request);
          Assert.assertEquals(status, response.getResponseCode());
          Assert.assertEquals(content, response.getResponseBodyAsString(
              StandardCharsets.UTF_8));
        }
      }
    }
  }

  @Test
  public void testProxyClientCreation() throws IOException {
    // Enable routing through the internal router in cConf.
    cConf.setBoolean(InternalRouter.INTERNAL_ROUTER_ENABLED, true);
    HttpMethod method = HttpMethod.GET;
    int status = 200;
    RemoteClient remoteClient = injector.getInstance(RemoteClientFactory.class)
        .createRemoteClient(MOCK_SERVICE,
            DefaultHttpRequestConfig.DEFAULT, "");
    HttpRequest request = remoteClient.requestBuilder(method,
            String.format("mock/%s/%d", method.name().toLowerCase(), status))
        .build();

    // Find the actual URL that will be used to send requests.
    URL url = remoteClient.resolve("mock");
    HttpResponse response = remoteClient.execute(request);

    Assert.assertEquals(url.getPath(), "/v3Internal/router/services/mock/mock");
    Assert.assertEquals(status, response.getResponseCode());
  }
}
