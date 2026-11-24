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

package io.cdap.cdap.gateway.router.handlers;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Security.Encryption;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.encryption.FakeAeadCipher;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.auth.ProxyUserIdentityExtractor;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.auth.UserIdentityExtractionResponse;
import io.cdap.cdap.security.auth.UserIdentityExtractor;
import io.cdap.cdap.security.auth.UserIdentityPair;
import io.cdap.cdap.security.spi.encryption.CipherException;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Collections;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link AuthenticationHandler}.
 */
public class AuthenticationHandlerTest {

  @Test
  public void testSanitizesSpoofedHeadersEvenWhenAuthBypassed() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.set(Constants.Security.Router.BYPASS_AUTHENTICATION_REGEX, "/v3/system/services/status");
    SConfiguration sConf = SConfiguration.create();

    UserIdentityExtractor extractor = request -> {
      throw new AssertionError("Bypassed URI should not invoke identity extractor");
    };
    AuthenticationHandler handler = new AuthenticationHandler(
        cConf, sConf, new InMemoryDiscoveryService(), extractor, new NoOpAeadCipher());
    EmbeddedChannel channel = new EmbeddedChannel(handler);

    HttpRequest req = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.GET, "/v3/system/services/status");
    req.headers().set(Constants.Security.Headers.USER_ID, "admin");
    req.headers().set(Constants.Security.Headers.USER_IP, "10.0.0.99");
    req.headers().set(Constants.Security.Headers.RUNTIME_TOKEN, "CDAP-External dummy");
    req.headers().set(io.cdap.cdap.common.http.HttpHeaderNames.WORKER_DECRYPTION_HDR, "ANY_WORKER");

    Assert.assertTrue(channel.writeInbound(req));
    HttpRequest forwarded = channel.readInbound();
    Assert.assertNotNull(forwarded);
    Assert.assertFalse(forwarded.headers().contains(Constants.Security.Headers.USER_ID));
    Assert.assertFalse(forwarded.headers().contains(Constants.Security.Headers.USER_IP));
    Assert.assertFalse(forwarded.headers().contains(Constants.Security.Headers.RUNTIME_TOKEN));
    Assert.assertFalse(
        forwarded.headers().contains(
            io.cdap.cdap.common.http.HttpHeaderNames.WORKER_DECRYPTION_HDR));
  }

  @Test
  public void testEncryptedUserCredentialBindsAuthenticatedUserId() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.set(Constants.Security.Authentication.PROXY_USER_ID_HEADER, "X-CDAP-User-ID");
    SConfiguration sConf = SConfiguration.create();
    sConf.setBoolean(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_ENABLED, true);

    FakeAeadCipher cipher = new FakeAeadCipher();
    cipher.initialize();

    AuthenticationHandler handler = new AuthenticationHandler(
        cConf, sConf, new InMemoryDiscoveryService(),
        new ProxyUserIdentityExtractor(cConf), cipher);
    EmbeddedChannel channel = new EmbeddedChannel(handler);

    HttpRequest req = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.GET, "/v3/namespaces");
    req.headers().set("X-CDAP-User-ID", "alice");
    req.headers().set(HttpHeaderNames.AUTHORIZATION, "Bearer oauth-secret-token");
    // Attempt to smuggle a conflicting CDAP-UserId header on inbound request
    req.headers().set(Constants.Security.Headers.USER_ID, "admin");

    Assert.assertTrue(channel.writeInbound(req));
    HttpRequest forwarded = channel.readInbound();
    Assert.assertEquals("alice", forwarded.headers().get(Constants.Security.Headers.USER_ID));

    String runtimeToken = forwarded.headers().get(Constants.Security.Headers.RUNTIME_TOKEN);
    Assert.assertNotNull(runtimeToken);
    String prefix = Credential.CredentialType.EXTERNAL_ENCRYPTED.getQualifiedName() + " ";
    Assert.assertTrue(runtimeToken.startsWith(prefix));
    String encryptedValue = runtimeToken.substring(prefix.length());

    Assert.assertEquals("oauth-secret-token",
        cipher.decryptStringFromBase64(encryptedValue,
            Encryption.userCredentialAssociatedData("alice")));
    try {
      cipher.decryptStringFromBase64(encryptedValue,
          Encryption.userCredentialAssociatedData("admin"));
      Assert.fail("Expected decryption with mismatched userId associated data to fail");
    } catch (CipherException expected) {
      // expected
    }
  }

  @Test
  public void testManagedTokenPropagatedWhenInternalAuthEnabledEvenIfPropagateFlagDisabled() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    cConf.setBoolean(Constants.Security.Authentication.PROPAGATE_USER_CREDENTIAL, false);
    SConfiguration sConf = SConfiguration.create();

    long now = System.currentTimeMillis();
    UserIdentityExtractor extractor = request -> new UserIdentityExtractionResponse(
        new UserIdentityPair("signed-managed-token",
            new UserIdentity("alice", UserIdentity.IdentifierType.EXTERNAL,
                Collections.emptyList(), now, now + 60000L)));

    AuthenticationHandler handler = new AuthenticationHandler(
        cConf, sConf, new InMemoryDiscoveryService(), extractor, new NoOpAeadCipher());
    EmbeddedChannel channel = new EmbeddedChannel(handler);

    HttpRequest req = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.GET, "/v3/namespaces");
    req.headers().set(HttpHeaderNames.AUTHORIZATION, "Bearer signed-managed-token");

    Assert.assertTrue(channel.writeInbound(req));
    HttpRequest forwarded = channel.readInbound();
    Assert.assertEquals("alice", forwarded.headers().get(Constants.Security.Headers.USER_ID));
    Assert.assertEquals("CDAP-External signed-managed-token",
        forwarded.headers().get(Constants.Security.Headers.RUNTIME_TOKEN));
  }
}
