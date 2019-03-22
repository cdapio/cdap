/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.monitor.proxy;

import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Unit tests for {@link ServiceSocksProxy}.
 */
public class ServiceSocksProxyTest {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceSocksProxy.class);

  private static InMemoryDiscoveryService discoveryService;
  private static NettyHttpService httpService;
  private static ServiceSocksProxy proxyServer;
  private static ProxySelector defaultProxySelector;

  @BeforeClass
  public static void init() throws Exception {
    // Start a HTTP service for hosting services to test
    httpService = NettyHttpService.builder("test")
      .setHttpHandlers(new TestHandler())
      .build();
    httpService.start();

    LOG.info("Http service started on {}", httpService.getBindAddress());

    // Register discovery
    discoveryService = new InMemoryDiscoveryService();
    discoveryService.register(ResolvingDiscoverable.of((new Discoverable("test-service",
                                                                         httpService.getBindAddress()))));
    proxyServer = new ServiceSocksProxy(discoveryService);
    proxyServer.startAndWait();

    defaultProxySelector = ProxySelector.getDefault();

    // Set the proxy for URLConnection
    Proxy proxy = new Proxy(Proxy.Type.SOCKS, proxyServer.getBindAddress());
    ProxySelector.setDefault(new ProxySelector() {
      @Override
      public List<Proxy> select(URI uri) {
        return Collections.singletonList(proxy);
      }

      @Override
      public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
        LOG.error("Connect failed {} {}", uri, sa, ioe);
      }
    });
  }

  @AfterClass
  public static void finish() throws Exception {
    ProxySelector.setDefault(defaultProxySelector);
    proxyServer.stopAndWait();
    httpService.stop();
  }

  @Test
  public void testGet() throws Exception {
    URL url = new URL("http://test-service/ping");
    HttpResponse response = HttpRequests.execute(io.cdap.common.http.HttpRequest.get(url).build());
    Assert.assertEquals(200, response.getResponseCode());
  }

  @Test
  public void testPost() throws Exception {
    URL url = new URL("http://test-service/echo");
    String body = "Echo body";
    HttpResponse response = HttpRequests.execute(
      io.cdap.common.http.HttpRequest.post(url)
        .withBody(body)
        .build());
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(body, response.getResponseBodyAsString());
  }

  @Test (expected = IOException.class)
  public void testTimeout() throws Exception {
    // Connect to a service that is not discoverable
    URL url = new URL("http://not-exist/ping");
    HttpRequests.execute(io.cdap.common.http.HttpRequest.get(url).build(), new HttpRequestConfig(500, 10000));
  }

  @Test
  public void testDelayRegister() throws Exception {
    URL url = new URL("http://test-service-2/ping");
    // Delay the service registration by 2 seconds.
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    ScheduledFuture<Cancellable> future = scheduler.schedule(
      () -> discoveryService.register(ResolvingDiscoverable.of(new Discoverable("test-service-2",
                                                                                httpService.getBindAddress()))),
      2, TimeUnit.SECONDS);

    try {
      HttpResponse response = HttpRequests.execute(io.cdap.common.http.HttpRequest.get(url).build(),
                                                   new HttpRequestConfig(5000, 5000));
      Assert.assertEquals(200, response.getResponseCode());
    } finally {
      future.get(5, TimeUnit.SECONDS).cancel();
    }
  }

  /**
   * A {@link HttpHandler} for unit-test.
   */
  public static final class TestHandler extends AbstractHttpHandler {

    @GET
    @Path("/ping")
    public void ping(HttpRequest request, HttpResponder responder) {
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @POST
    @Path("/echo")
    public void echo(FullHttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, request.content().toString(StandardCharsets.UTF_8));
    }
  }
}
