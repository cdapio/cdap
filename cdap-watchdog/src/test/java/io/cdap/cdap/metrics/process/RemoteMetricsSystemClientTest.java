/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.metrics.process;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests that {@link RemoteMetricsSystemClient} attaches internal authentication headers via
 * {@link io.cdap.cdap.common.internal.remote.RemoteClientFactory} and preserves the
 * {@link IOException} contract on unavailable or failing endpoints.
 */
public class RemoteMetricsSystemClientTest {

  @Test
  public void testInternalAuthenticatorHeadersAndForbiddenPropagation() throws Exception {
    List<String> capturedAuthHeaders = new ArrayList<>();
    NettyHttpService httpService = NettyHttpService.builder("metrics-auth-test")
        .setHttpHandlers(new TestMetricsHandler(capturedAuthHeaders))
        .build();
    httpService.start();
    try {
      Injector injector = Guice.createInjector(
          new ConfigModule(CConfiguration.create()),
          new InMemoryDiscoveryModule(),
          RemoteAuthenticatorModules.getNoOpModule(),
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(MessagingService.class).toInstance(Mockito.mock(MessagingService.class));
              bind(InternalAuthenticator.class)
                  .toInstance(setter -> setter.accept("X-Test-Auth", "bearer-token"));
            }
          });
      DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);
      discoveryService.register(
          new Discoverable(Constants.Service.METRICS, httpService.getBindAddress()));
      RemoteMetricsSystemClient client = injector.getInstance(RemoteMetricsSystemClient.class);

      Assert.assertEquals(Collections.singletonList("system.m"),
          client.search(Collections.singletonMap("ns", "allowed")));
      Assert.assertTrue(client.query(0, 10, 1,
          Collections.singletonMap("namespace", "allowed"),
          Collections.singletonList("system.m"),
          Collections.emptyList()).isEmpty());
      Assert.assertEquals(Arrays.asList("bearer-token", "bearer-token"), capturedAuthHeaders);

      try {
        client.search(Collections.singletonMap("ns", "forbidden"));
        Assert.fail("Expected UnauthorizedException");
      } catch (UnauthorizedException e) {
        // expected
      }

      try {
        client.query(0, 10, 1,
            Collections.singletonMap("namespace", "forbidden"),
            Collections.singletonList("system.m"),
            Collections.emptyList());
        Assert.fail("Expected UnauthorizedException");
      } catch (UnauthorizedException e) {
        // expected
      }

      try {
        client.search(Collections.singletonMap("ns", "unavailable"));
        Assert.fail("Expected IOException on 503 response");
      } catch (IOException e) {
        Assert.assertTrue(e.getCause() instanceof ServiceUnavailableException);
      }
    } finally {
      httpService.stop();
    }
  }

  @Test
  public void testUnregisteredMetricsServiceThrowsIOException() throws Exception {
    Injector injector = Guice.createInjector(
        new ConfigModule(CConfiguration.create()),
        new InMemoryDiscoveryModule(),
        RemoteAuthenticatorModules.getNoOpModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MessagingService.class).toInstance(Mockito.mock(MessagingService.class));
            bind(InternalAuthenticator.class).toInstance(setter -> { });
          }
        });
    RemoteMetricsSystemClient client = injector.getInstance(RemoteMetricsSystemClient.class);

    try {
      client.search(Collections.singletonMap("ns", "allowed"));
      Assert.fail("Expected IOException when metrics service is not discoverable");
    } catch (IOException e) {
      Assert.assertTrue(e.getCause() instanceof ServiceUnavailableException);
    }

    try {
      client.query(0, 10, 1,
          Collections.singletonMap("namespace", "allowed"),
          Collections.singletonList("system.m"),
          Collections.emptyList());
      Assert.fail("Expected IOException when metrics service is not discoverable");
    } catch (IOException e) {
      Assert.assertTrue(e.getCause() instanceof ServiceUnavailableException);
    }
  }

  /** Test metrics HTTP handler capturing internal authentication headers. */
  public static final class TestMetricsHandler extends AbstractHttpHandler {

    private final List<String> capturedAuthHeaders;

    TestMetricsHandler(List<String> capturedAuthHeaders) {
      this.capturedAuthHeaders = capturedAuthHeaders;
    }

    @POST
    @Path("/v3/metrics/search")
    public void search(HttpRequest request, HttpResponder responder,
        @QueryParam("tag") List<String> tags) {
      capturedAuthHeaders.add(request.headers().get("X-Test-Auth"));
      if (tags.contains("ns:forbidden")) {
        responder.sendString(HttpResponseStatus.FORBIDDEN, "denied");
        return;
      }
      if (tags.contains("ns:unavailable")) {
        responder.sendString(HttpResponseStatus.SERVICE_UNAVAILABLE, "restarting");
        return;
      }
      responder.sendJson(HttpResponseStatus.OK, "[\"system.m\"]");
    }

    @POST
    @Path("/v3/metrics/query")
    public void query(HttpRequest request, HttpResponder responder,
        @QueryParam("tag") List<String> tags) {
      capturedAuthHeaders.add(request.headers().get("X-Test-Auth"));
      if (tags != null && tags.contains("namespace:forbidden")) {
        responder.sendString(HttpResponseStatus.FORBIDDEN, "denied");
        return;
      }
      responder.sendJson(HttpResponseStatus.OK, "{\"startTime\":0,\"endTime\":0,\"series\":[]}");
    }
  }
}
