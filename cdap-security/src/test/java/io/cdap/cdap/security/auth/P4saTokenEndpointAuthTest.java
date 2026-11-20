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

package io.cdap.cdap.security.auth;

import com.google.common.io.ByteStreams;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link P4saTokenEndpointAuth}.
 */
public class P4saTokenEndpointAuthTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @After
  public void tearDown() {
    P4saTokenEndpointAuth.closeAllBridgesForTesting();
  }

  @Test
  public void testJsonAndBinaryKeyfilesProduceIdenticalToken() throws Exception {
    byte[] rawKeyBytes = new byte[32];
    new SecureRandom().nextBytes(rawKeyBytes);
    SecretKeySpec keySpec = new SecretKeySpec(rawKeyBytes, "HmacSHA256");
    KeyIdentifier keyIdentifier = new KeyIdentifier(keySpec, 42, Long.MAX_VALUE);

    File binaryKeyFile = tempFolder.newFile("tokenManagerKey.bin");
    Files.write(binaryKeyFile.toPath(), new KeyIdentifierCodec().encode(keyIdentifier));

    File jsonKeyFile = tempFolder.newFile("tokenManagerKey.json");
    String clhJson = String.format(
        "{\"encodedKey\":\"%s\",\"algorithm\":\"AES\",\"keyId\":42,\"expiration\":%d}",
        Base64.getEncoder().encodeToString(rawKeyBytes), Long.MAX_VALUE);
    Files.write(jsonKeyFile.toPath(), clhJson.getBytes(StandardCharsets.UTF_8));

    CConfiguration binConf = CConfiguration.create();
    binConf.set(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH, binaryKeyFile.getAbsolutePath());

    CConfiguration jsonConf = CConfiguration.create();
    jsonConf.set(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH, jsonKeyFile.getAbsolutePath());

    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(new SecretKeySpec(rawKeyBytes, "HmacSHA256"));
    String expectedHmac = Base64.getUrlEncoder().withoutPadding().encodeToString(
        mac.doFinal(P4saTokenEndpointAuth.HMAC_PURPOSE.getBytes(StandardCharsets.UTF_8)));

    String binToken = P4saTokenEndpointAuth.computeToken(binConf);
    String jsonToken = P4saTokenEndpointAuth.computeToken(jsonConf);
    Assert.assertEquals(expectedHmac, binToken);
    Assert.assertEquals(expectedHmac, jsonToken);

    Assert.assertTrue(P4saTokenEndpointAuth.verifyToken(binConf, expectedHmac));
    Assert.assertTrue(P4saTokenEndpointAuth.verifyToken(jsonConf, expectedHmac));
    Assert.assertFalse(P4saTokenEndpointAuth.verifyToken(jsonConf, expectedHmac + "x"));
    Assert.assertFalse(P4saTokenEndpointAuth.verifyToken(jsonConf, ""));
    Assert.assertFalse(P4saTokenEndpointAuth.verifyToken(jsonConf, null));
  }

  @Test
  public void testMissingKeyfileFailsClosedAndSkipsAugmentation() throws Exception {
    File missingFile = new File(tempFolder.getRoot(), "does-not-exist.json");
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH, missingFile.getAbsolutePath());

    Assert.assertNull(P4saTokenEndpointAuth.computeToken(cConf));
    Assert.assertFalse(P4saTokenEndpointAuth.verifyToken(cConf, "any-token"));

    Map<String, String> input = new HashMap<>();
    input.put("token.endpoint", "https://cdap-appfabric:11017/v3Internal/system/p4sa/token");
    Map<String, String> augmented = P4saTokenEndpointAuth.augmentEndpointProperties(cConf, input);
    Assert.assertEquals(
        "https://cdap-appfabric:11017/v3Internal/system/p4sa/token",
        augmented.get("token.endpoint"));
  }

  @Test
  public void testLoopbackBridgeForwardsHeaderWithoutPuttingSecretsInUrlsOrMutatingConfig()
      throws Exception {
    AtomicReference<String> receivedHmacHeader = new AtomicReference<>();
    AtomicReference<String> receivedMetadataFlavor = new AtomicReference<>();
    AtomicReference<String> receivedUri = new AtomicReference<>();
    NettyHttpService upstreamServer = NettyHttpService.builder("stub-upstream-appfabric")
        .setHost("127.0.0.1")
        .setPort(0)
        .setHttpHandlers(new StubUpstreamTokenHandler(
            receivedHmacHeader, receivedMetadataFlavor, receivedUri))
        .build();
    upstreamServer.start();
    try {
      byte[] rawKeyBytes = new byte[32];
      new SecureRandom().nextBytes(rawKeyBytes);
      File jsonKeyFile = tempFolder.newFile("tokenManagerKey.json");
      String clhJson = String.format(
          "{\"encodedKey\":\"%s\",\"algorithm\":\"AES\",\"keyId\":7,\"expiration\":%d}",
          Base64.getEncoder().encodeToString(rawKeyBytes), Long.MAX_VALUE);
      Files.write(jsonKeyFile.toPath(), clhJson.getBytes(StandardCharsets.UTF_8));

      CConfiguration cConf = CConfiguration.create();
      cConf.set(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH, jsonKeyFile.getAbsolutePath());
      String rawEndpoint = String.format("http://127.0.0.1:%d/v3Internal/system/p4sa/token",
          upstreamServer.getBindAddress().getPort());
      cConf.set("runtime.config.provider.properties.gcp-dataproc.token.endpoint", rawEndpoint);

      String expectedToken = P4saTokenEndpointAuth.computeToken(cConf);
      Assert.assertNotNull(expectedToken);

      Map<String, String> originalProps = new HashMap<>();
      originalProps.put("token.endpoint", rawEndpoint);
      originalProps.put("custom.token.endpoint", rawEndpoint + "?audience=internal");
      originalProps.put("project.id", "priyanshu-cdf");

      Map<String, String> firstPass =
          P4saTokenEndpointAuth.augmentEndpointProperties(cConf, originalProps);
      Map<String, String> secondPass =
          P4saTokenEndpointAuth.augmentEndpointProperties(cConf, firstPass);

      String bridgeEndpoint = firstPass.get("token.endpoint");
      String customBridgeEndpoint = firstPass.get("custom.token.endpoint");
      Assert.assertTrue(bridgeEndpoint.startsWith("http://127.0.0.1:"));
      Assert.assertTrue(bridgeEndpoint.endsWith("/token"));
      Assert.assertFalse(bridgeEndpoint.contains(expectedToken));
      Assert.assertFalse(bridgeEndpoint.contains("?"));
      Assert.assertEquals("priyanshu-cdf", firstPass.get("project.id"));
      Assert.assertEquals(firstPass, secondPass);

      // Input map and CConfiguration must remain completely unmutated.
      Assert.assertEquals(rawEndpoint, originalProps.get("token.endpoint"));
      Assert.assertEquals(
          rawEndpoint,
          cConf.get("runtime.config.provider.properties.gcp-dataproc.token.endpoint"));

      // Calling the loopback bridge forwards X-CDAP-System-P4SA-Token and query parameters.
      HttpURLConnection conn = (HttpURLConnection) new URL(
          customBridgeEndpoint + "?scopes=cloud-platform").openConnection();
      Assert.assertEquals(200, conn.getResponseCode());
      try (InputStream in = conn.getInputStream()) {
        String body = new String(ByteStreams.toByteArray(in), StandardCharsets.UTF_8);
        Assert.assertTrue(body.contains("stub-access-token"));
      } finally {
        conn.disconnect();
      }
      Assert.assertEquals(expectedToken, receivedHmacHeader.get());
      Assert.assertEquals("Google", receivedMetadataFlavor.get());
      Assert.assertEquals(
          "/v3Internal/system/p4sa/token?audience=internal&scopes=cloud-platform",
          receivedUri.get());

      // Removing the keyfile makes the loopback bridge fail closed with 403.
      Assert.assertTrue(jsonKeyFile.delete());
      HttpURLConnection deletedKeyConn =
          (HttpURLConnection) new URL(bridgeEndpoint).openConnection();
      Assert.assertEquals(403, deletedKeyConn.getResponseCode());
      deletedKeyConn.disconnect();
    } finally {
      upstreamServer.stop();
    }
  }

  @Path("/v3Internal")
  public static final class StubUpstreamTokenHandler extends AbstractHttpHandler {
    private final AtomicReference<String> receivedHmacHeader;
    private final AtomicReference<String> receivedMetadataFlavor;
    private final AtomicReference<String> receivedUri;

    StubUpstreamTokenHandler(AtomicReference<String> receivedHmacHeader,
        AtomicReference<String> receivedMetadataFlavor,
        AtomicReference<String> receivedUri) {
      this.receivedHmacHeader = receivedHmacHeader;
      this.receivedMetadataFlavor = receivedMetadataFlavor;
      this.receivedUri = receivedUri;
    }

    @GET
    @Path("/system/p4sa/token")
    public void token(HttpRequest request, HttpResponder responder) {
      receivedHmacHeader.set(request.headers().get(P4saTokenEndpointAuth.HEADER_NAME));
      receivedMetadataFlavor.set(request.headers().get("Metadata-Flavor"));
      receivedUri.set(request.uri());
      responder.sendJson(HttpResponseStatus.OK,
          "{\"access_token\":\"stub-access-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}");
    }
  }
}
