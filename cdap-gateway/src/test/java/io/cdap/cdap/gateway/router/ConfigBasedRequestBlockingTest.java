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

package io.cdap.cdap.gateway.router;

import com.google.common.io.ByteStreams;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.security.auth.TokenValidator;
import io.cdap.cdap.security.encryption.NoOpAeadCipher;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests config-based request-blocking
 */
public class ConfigBasedRequestBlockingTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static NettyRouter router;
  private static NettyHttpService httpService;
  private static CConfiguration cConf;
  private static Cancellable cancelDiscovery;

  @BeforeClass
  public static void init() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.Router.ADDRESS, InetAddress.getLoopbackAddress().getHostAddress());
    cConf.setInt(Constants.Router.ROUTER_PORT, 0);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    TokenValidator successValidator = new SuccessTokenValidator();

    router = new NettyRouter(cConf, SConfiguration.create(), InetAddress.getLoopbackAddress(),
        new RouterServiceLookup(cConf, discoveryService, new RouterPathLookup()),
        successValidator,
        new MockAccessTokenIdentityExtractor(successValidator), discoveryService,
        new NoOpAeadCipher());
    router.startAndWait();

    httpService = NettyHttpService.builder("test").setHttpHandlers(new AuditLogTest.TestHandler())
        .build();
    httpService.start();

    cancelDiscovery = discoveryService.register(new Discoverable(Constants.Service.APP_FABRIC_HTTP,
        httpService.getBindAddress()));
  }

  @Test
  public void testStatusCode() throws Exception {
    // Enable request-blocking
    cConf.setBoolean(Constants.Router.BLOCK_REQUEST_ENABLED, true);

    testGet(cConf.getInt(Constants.Router.BLOCK_REQUEST_STATUS_CODE), null, "/get");
  }

  @Test
  public void testResponseBody() throws Exception {
    // Enable request-blocking
    cConf.setBoolean(Constants.Router.BLOCK_REQUEST_ENABLED, true);

    // When no config is present for message, response should be empty
    cConf.unset(Constants.Router.BLOCK_REQUEST_MESSAGE);
    testGet(cConf.getInt(Constants.Router.BLOCK_REQUEST_STATUS_CODE), "", "/get");

    // Custom message passed in config
    cConf.set(Constants.Router.BLOCK_REQUEST_MESSAGE, "custom message");
    testGet(cConf.getInt(Constants.Router.BLOCK_REQUEST_STATUS_CODE),
        cConf.get(Constants.Router.BLOCK_REQUEST_MESSAGE), "/get");
  }

  @Test
  public void testRequestBlockingDisabled() throws Exception {
    // Disable request-blocking
    cConf.setBoolean(Constants.Router.BLOCK_REQUEST_ENABLED, false);

    testGet(HttpResponseStatus.OK.code(), null, "/get");
  }

  @Test
  public void testRouterStatus() throws Exception {
    // Enable request-blocking
    cConf.setBoolean(Constants.Router.BLOCK_REQUEST_ENABLED, true);

    testGet(HttpResponseStatus.OK.code(), null, Constants.EndPoints.STATUS);
  }

  @AfterClass
  public static void finish() throws Exception {
    cancelDiscovery.cancel();
    httpService.stop();
    router.stopAndWait();
  }

  private void testGet(int expectedStatus, String expectedResponse, String path)
      throws Exception {

    InetSocketAddress address = router.getBoundAddress().orElseThrow(IllegalStateException::new);
    URL url = new URL("http", address.getHostName(), address.getPort(), path);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    connection.setDoInput(true);
    connection.connect();
    try {
      InputStream inputStream;

      try {
        inputStream = connection.getInputStream();
      } catch (IOException e) {
        // In case of status code >= 400, IOException is thrown and the original response is sent through Error Stream
        inputStream = connection.getErrorStream();
      }

      if (expectedResponse != null) {
        if (expectedResponse.length() == 0) {
          // Special case when input HttpURLConnection will throw IOException if status code >= 400
          // but Error Stream won't be populated so rely on content-length header instead
          Assert.assertEquals("0", connection.getHeaderField("content-length"));
        } else {
          Assert
              .assertEquals(expectedResponse, Bytes.toString(ByteStreams.toByteArray(inputStream)));
        }
      }
      Assert.assertEquals(expectedStatus, connection.getResponseCode());
    } finally {
      connection.disconnect();
    }
  }
}
