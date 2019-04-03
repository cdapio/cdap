/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.Constants;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collections;
import java.util.Map;

/**
 * Verify the ordering of events in the RouterPipeline.
 */
public class NettyRouterPipelineAuthTest {

  private static final String HOSTNAME = "127.0.0.1";

  private static final String SERVICE_NAME = Constants.Service.APP_FABRIC_HTTP;

  private static final DiscoveryService DISCOVERY_SERVICE = new InMemoryDiscoveryService();
  private static final RouterResource ROUTER = new RouterResource(HOSTNAME, DISCOVERY_SERVICE, ImmutableMap.of(
    Constants.Security.ENABLED, "true",
    Constants.Security.Router.BYPASS_AUTHENTICATION_REGEX, "(/v1/repeat/.*|/v1/echo/dontfail)"
  ));
  private static final ServerResource GATEWAY_SERVER = new ServerResource(HOSTNAME, DISCOVERY_SERVICE, SERVICE_NAME);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @SuppressWarnings("UnusedDeclaration")
  @ClassRule
  public static TestRule chain = RuleChain.outerRule(ROUTER).around(GATEWAY_SERVER);

  @Test
  public void testRouterAuthBypass() throws Exception {
    // mock token validator passes for no token and any token other than "Bearer failme"
    testGet(200, "hello", "/v1/echo/hello");
    testGet(200, "hello", "/v1/echo/hello", ImmutableMap.of("Authorization", "Bearer x"));
    // so this should fail
    testGet(401, null, "/v1/echo/hello", ImmutableMap.of("Authorization", "Bearer failme"));
    // but /v1/echo/dontfail is configured to bypass auth
    testGet(200, "dontfail", "/v1/echo/dontfail", ImmutableMap.of("Authorization", "Bearer failme"));
    // it only bypasses on exact match, not prefix match
    testGet(401, null, "/v1/echo/dontfailme", ImmutableMap.of("Authorization", "Bearer failme"));

    // /v1/repeat is configured to bypass auth validation, on prefix match
    testGet(200, "hello", "/v1/repeat/hello");
    testGet(200, "hello", "/v1/repeat/hello", ImmutableMap.of("Authorization", "Bearer x"));
    testGet(200, "hello", "/v1/repeat/hello", ImmutableMap.of("Authorization", "Bearer failme"));
    // even with a token that fails validation, we get the correct status code 404
    testGet(404, null, "/v1/repeat/dontfail/me", ImmutableMap.of("Authorization", "Bearer failme"));
  }

  private void testGet(int expectedStatus, String expectedResponse, String path) throws Exception {
    testGet(expectedStatus, expectedResponse, path, Collections.<String, String>emptyMap());
  }

  private void testGet(int expectedStatus, String expectedResponse, String path, Map<String, String> headers)
    throws Exception {

    InetSocketAddress address = ROUTER.getRouterAddress();
    URL url = new URL("http", address.getHostName(), address.getPort(), path);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("GET");
    connection.setDoInput(true);
    for (Map.Entry<String, String> header : headers.entrySet()) {
      connection.setRequestProperty(header.getKey(), header.getValue());
    }
    connection.connect();
    try {
      Assert.assertEquals(expectedStatus, connection.getResponseCode());
      if (expectedResponse != null) {
        byte[] content = ByteStreams.toByteArray(connection.getInputStream());
        Assert.assertEquals(expectedResponse, Bytes.toString(content));
      }
    } finally {
      connection.disconnect();
    }
  }

}
