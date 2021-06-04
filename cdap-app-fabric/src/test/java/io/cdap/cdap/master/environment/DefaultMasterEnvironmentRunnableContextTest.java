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

package io.cdap.cdap.master.environment;


import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.common.http.HttpMethod;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Test for {@link DefaultMasterEnvironmentRunnableContextTest}.
 */
public class DefaultMasterEnvironmentRunnableContextTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static DefaultMasterEnvironmentRunnableContext context;
  private static Cancellable cancelDiscovery;
  private static NettyHttpService httpService;

  @BeforeClass
  public static void setup() throws Exception {
    DiscoveryService discoveryService = new InMemoryDiscoveryService();
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    context = new DefaultMasterEnvironmentRunnableContext((DiscoveryServiceClient) discoveryService, locationFactory);

    httpService = NettyHttpService.builder(Constants.Service.APP_FABRIC_HTTP)
      .setHttpHandlers(new MockHttpHandler())
      .build();
    httpService.start();
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.APP_FABRIC_HTTP, httpService)));
  }

  @AfterClass
  public static void stop() throws Exception {
    if (cancelDiscovery != null) {
      cancelDiscovery.cancel();
    }
    if (httpService != null) {
      httpService.stop();
    }
  }

  @Test
  public void testOpenHttpConnection() throws Exception {
    String message = "hello";
    String resource = String.format("echo/%s", message);
    HttpURLConnection conn = context.openHttpURLConnection(resource);
    conn.setRequestMethod(HttpMethod.GET.name());
    String respContent;
    try (InputStream is = conn.getInputStream()) {
      respContent = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
        .lines().collect(Collectors.joining("\n"));
    }
    Assert.assertEquals(HttpResponseStatus.OK.code(), conn.getResponseCode());
    Assert.assertEquals(message, respContent);
  }

  /**
   * Mock http service handler
   */
  public static final class MockHttpHandler extends AbstractHttpHandler {
    @GET
    @Path("/echo/{message}")
    public void echo(HttpRequest request, HttpResponder responder, @PathParam("message") String message) {
      responder.sendString(HttpResponseStatus.OK, message);
    }
  }
}
