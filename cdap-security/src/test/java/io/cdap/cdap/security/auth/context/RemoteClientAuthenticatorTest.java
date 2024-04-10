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

package io.cdap.cdap.security.auth.context;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.net.HttpURLConnection;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for verifying authentication for the {@link io.cdap.cdap.common.internal.remote.RemoteClient}.
 */
public class RemoteClientAuthenticatorTest {
  private static final String TEST_SERVICE = "test";

  private static TestHttpHandler testHttpHandler;
  private static NettyHttpService httpService;
  private static Injector injector;
  private static MockRemoteAuthenticatorProvider mockRemoteAuthenticatorProvider;

  @BeforeClass
  public static void setup() throws Exception {
    mockRemoteAuthenticatorProvider = new MockRemoteAuthenticatorProvider();

    // Setup Guice injector.
    injector = Guice.createInjector(new ConfigModule(), new InMemoryDiscoveryModule(),
                                    new PrivateModule() {
                                      @Override
                                      protected void configure() {
                                        bind(RemoteAuthenticator.class).toProvider(mockRemoteAuthenticatorProvider);
                                        expose(RemoteAuthenticator.class);
                                      }
                                    },
                                    new AuthenticationContextModules().getNoOpModule());
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);

    // Setup test HTTP handler and register the service.
    testHttpHandler = new TestHttpHandler();
    httpService = new CommonNettyHttpServiceBuilder(cConf, TEST_SERVICE, new NoOpMetricsCollectionService(), null)
      .setHttpHandlers(testHttpHandler).build();
    httpService.start();
    discoveryService.register(new Discoverable(TEST_SERVICE, httpService.getBindAddress()));
  }

  @AfterClass
  public static void teardown() throws Exception {
    httpService.stop();
  }

  @Test
  public void testRemoteClientWithoutInternalAuthInjectsNoAuthenticationContext() throws Exception {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, false);
    RemoteClientFactory remoteClientFactory = injector.getInstance(RemoteClientFactory.class);
    RemoteClient remoteClient = remoteClientFactory
      .createRemoteClient(TEST_SERVICE, new HttpRequestConfig(15000, 15000, false), "/");
    HttpURLConnection conn = remoteClient.openConnection(HttpMethod.GET, "");
    int responseCode = conn.getResponseCode();

    // Verify that the request received the expected headers.
    HttpHeaders headers = testHttpHandler.getRequest().headers();

    Assert.assertEquals(HttpResponseStatus.OK.code(), responseCode);
    Assert.assertFalse(headers.contains(Constants.Security.Headers.USER_ID));
    Assert.assertFalse(headers.contains(Constants.Security.Headers.RUNTIME_TOKEN));
  }

  @Test
  public void testRemoteClientWithInternalAuthInjectsAuthenticationContext() throws Exception {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    RemoteClientFactory remoteClientFactory = injector.getInstance(RemoteClientFactory.class);
    RemoteClient remoteClient = remoteClientFactory
      .createRemoteClient(TEST_SERVICE, new HttpRequestConfig(15000, 15000, false), "/");

    // Set authentication context principal.
    String expectedName = "somebody";
    String expectedCredValue = "credential";
    Credential.CredentialType expectedCredType = Credential.CredentialType.EXTERNAL;
    System.setProperty("user.name", expectedName);
    System.setProperty("user.credential.value", expectedCredValue);
    System.setProperty("user.credential.type", expectedCredType.name());

    HttpURLConnection conn = remoteClient.openConnection(HttpMethod.GET, "");
    int responseCode = conn.getResponseCode();

    // Verify that the request received the expected headers.
    HttpHeaders headers = testHttpHandler.getRequest().headers();

    Assert.assertEquals(HttpResponseStatus.OK.code(), responseCode);
    Assert.assertEquals(expectedName, headers.get(Constants.Security.Headers.USER_ID));
    Assert.assertEquals(String.format("%s %s", expectedCredType.getQualifiedName(), expectedCredValue),
                        headers.get(Constants.Security.Headers.RUNTIME_TOKEN));
  }

  @Test
  public void testRemoteClientWithRemoteAuthenticatorIncludesAuthorizationHeader() throws Exception {
    String mockAuthenticatorName = "mock-remote-authenticator";
    Credential expectedCredential = new Credential("test-credential", Credential.CredentialType.EXTERNAL_BEARER);

    RemoteAuthenticator mockRemoteAuthenticator = mock(RemoteAuthenticator.class);
    when(mockRemoteAuthenticator.getName()).thenReturn(mockAuthenticatorName);
    when(mockRemoteAuthenticator.getCredentials()).thenReturn(expectedCredential);
    mockRemoteAuthenticatorProvider.setAuthenticator(mockRemoteAuthenticator);

    RemoteClientFactory remoteClientFactory = injector.getInstance(RemoteClientFactory.class);
    RemoteClient remoteClient = remoteClientFactory
      .createRemoteClient(TEST_SERVICE, new HttpRequestConfig(15000, 15000, false), "/");

    HttpURLConnection conn = remoteClient.openConnection(HttpMethod.GET, "");
    int responseCode = conn.getResponseCode();

    // Verify that the request received the expected headers.
    HttpHeaders headers = testHttpHandler.getRequest().headers();

    Assert.assertEquals(HttpResponseStatus.OK.code(), responseCode);
    Assert.assertEquals(String.format("%s %s", expectedCredential.getType().getQualifiedName(),
                                      expectedCredential.getValue()),
                        headers.get(javax.ws.rs.core.HttpHeaders.AUTHORIZATION));
  }

  /**
   * HTTP handler for testing.
   */
  public static class TestHttpHandler extends AbstractHttpHandler {
    private HttpRequest request;

    @GET
    @Path("/")
    public void get(HttpRequest request, HttpResponder responder) {
      this.request = request;
      responder.sendStatus(HttpResponseStatus.OK);
    }

    public HttpRequest getRequest() {
      return request;
    }
  }

  /**
   * A {@link RemoteAuthenticator} provider for testing.
   */
  private static final class MockRemoteAuthenticatorProvider implements Provider<RemoteAuthenticator> {

    private RemoteAuthenticator remoteAuthenticator = new NoOpRemoteAuthenticator();

    public void setAuthenticator(RemoteAuthenticator remoteAuthenticator) {
      this.remoteAuthenticator = remoteAuthenticator;
    }

    @Override
    @Nullable
    public RemoteAuthenticator get() {
      return remoteAuthenticator;
    }
  }
}
