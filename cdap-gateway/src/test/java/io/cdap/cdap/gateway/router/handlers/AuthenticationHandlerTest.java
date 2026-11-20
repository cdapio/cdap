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
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.gateway.router.MockAccessTokenIdentityExtractor;
import io.cdap.cdap.gateway.router.MockTokenValidator;
import io.cdap.cdap.security.auth.UserIdentityExtractor;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AuthenticationHandler} bypass path canonicalization.
 */
public class AuthenticationHandlerTest {

  private static final String MATCH_ANY_STRING_REGEX = "(?s).*";

  @Test
  public void testBypassOriginAndSyntaxChecks() {
    // Non-origin-form URIs, leading '//' authority-prefix ambiguity, and malformed URI syntax must not bypass auth
    assertRejectedFromBypass(null);
    assertRejectedFromBypass("");
    assertRejectedFromBypass("*");
    assertRejectedFromBypass("relative/path");
    assertRejectedFromBypass("http://evil/v1/repeat/hi");
    assertRejectedFromBypass("foo:bar");
    assertRejectedFromBypass("//evil/v1/repeat/hi");
    assertRejectedFromBypass("///v1/repeat/hi");
    assertRejectedFromBypass("/v1/repeat/%ZZ");
    assertRejectedFromBypass("/v1/repeat/%");
    assertRejectedFromBypass("/v1/repeat/a\\b");
    assertRejectedFromBypass("/v1/repeat/a b");

    // Origin-form paths collapse interior/trailing slashes and strip queries/fragments (even with %2F/%0a)
    assertCanonicalBypassPath("/", "/");
    assertCanonicalBypassPath("/", "/?foo=bar#frag");
    assertCanonicalBypassPath("/v3/security/authorization/privileges/grant",
                              "/v3/security/authorization//privileges/grant/");
    assertCanonicalBypassPath("/v1/echo/dontfail", "/v1/echo/dontfail?foo=bar#section");
    assertCanonicalBypassPath("/v1/echo/dontfail", "/v1/echo/dontfail?redirect=%2Fhome&nl=%0a#%2f");
  }

  @Test
  public void testBypassPercentEncodingAndDotSegmentChecks() {
    // Encoded slashes (lower and upper hex) must not bypass auth
    assertRejectedFromBypass("/v3/namespaces/default/streams/s1%2fprograms");
    assertRejectedFromBypass("/v3/namespaces/default/streams/s1%2Fprograms");
    assertRejectedFromBypass("/v3/namespaces/default/streams/s1%20%2fprograms");
    assertRejectedFromBypass("/v1/repeat/a%25%2fb");

    // Literal and percent-encoded '.' and '..' segments must not bypass auth
    assertRejectedFromBypass("/v1/echo/./dontfail");
    assertRejectedFromBypass("/v1/echo/%2e/dontfail");
    assertRejectedFromBypass("/v1/repeat/../echo/hi");
    assertRejectedFromBypass("/v1/repeat/%2e%2e/echo/hi");
    assertRejectedFromBypass("/v1/repeat/%2E%2E/echo/hi");
    assertRejectedFromBypass("/../v1/repeat/hi");

    // Decoded ISO control characters must not bypass auth
    assertRejectedFromBypass("/v1/repeat/a%00/secret");
    assertRejectedFromBypass("/v1/echo/dontfail%00");
    assertRejectedFromBypass("/v1/repeat/a%0a/secret");
    assertRejectedFromBypass("/v1/repeat/a%0d/secret");
    assertRejectedFromBypass("/v1/repeat/a%c2%85/secret");

    // Legitimate encoded path params (%7E, %20, double-encoded %252f, UTF-8) and dots inside names remain valid
    assertCanonicalBypassPath("/v1/repeat/my~schedule", "/v1/repeat/my%7Eschedule");
    assertCanonicalBypassPath("/v1/repeat/Sample Buckets", "/v1/repeat/Sample%20Buckets");
    assertCanonicalBypassPath("/v1/repeat/a%2fb+c", "/v1/repeat/a%252fb%2bc");
    assertCanonicalBypassPath("/v1/repeat/café", "/v1/repeat/caf%c3%a9");
    assertCanonicalBypassPath("/v3/namespaces/default/streams/events.v1",
                              "/v3/namespaces/default/streams/events.v1");
    assertCanonicalBypassPath("/v3/namespaces/default/streams/a..b",
                              "/v3/namespaces/default/streams/a..b");
  }

  @Test
  public void testBypassRejectsEncodedTraversalFragmentSmugglingAndNewlineInjection() {
    // Unset or non-matching bypass regex requires authentication
    assertUnauthorized(null, "/v1/echo/dontfail");
    assertUnauthorized("/v1/.*", "/");

    // Single-segment wildcard regex allows normal stream names but rejects encoded path traversal
    String streamsRegex = "/v3/namespaces/default/streams/[^/]+";
    assertBypassed(streamsRegex, "/v3/namespaces/default/streams/events.v1");
    assertUnauthorized(streamsRegex,
                       "/v3/namespaces/default/streams/..%2f..%2f..%2fsystem%2fservices%2fappfabric%2fstatus");

    // Raw wire #fragment suffix smuggling (stripped client-side by java.net.URL, so tested directly on channel)
    assertUnauthorized(".*/status", "/v1/echo/secret#/status");
    assertBypassed(".*/status", "/v1/echo/status#section");

    // Negative-lookahead regex must not be bypassed via %0a newline injection
    String lookaheadRegex = "(?!.*/secret)/v1/repeat/[^/]+/[^/]+";
    assertBypassed(lookaheadRegex, "/v1/repeat/cat/public");
    assertUnauthorized(lookaheadRegex, "/v1/repeat/cat%0a/secret");
  }

  private static void assertCanonicalBypassPath(String expectedCanonicalPath, String rawUri) {
    assertBypassed(Pattern.quote(expectedCanonicalPath), rawUri);
  }

  private static void assertRejectedFromBypass(@Nullable final String rawUri) {
    FullHttpRequest request = rawUri != null
      ? new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, rawUri)
      : new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/") {
        @Override
        public String uri() {
          return null;
        }
      };
    assertUnauthorizedRequest(MATCH_ANY_STRING_REGEX, request);
  }

  private static void assertBypassed(String bypassRegex, String rawUri) {
    CConfiguration cConf = createBaseConfig(bypassRegex);
    EmbeddedChannel channel = createChannel(
      cConf, new MockAccessTokenIdentityExtractor(new MockTokenValidator("failme")));
    try {
      FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, rawUri);
      Assert.assertTrue(channel.writeInbound(request));
      HttpRequest forwarded = channel.readInbound();
      Assert.assertNotNull(forwarded);
      try {
        Assert.assertEquals(rawUri, forwarded.uri());
      } finally {
        ReferenceCountUtil.release(forwarded);
      }
      Assert.assertNull(channel.readOutbound());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  private static void assertUnauthorized(@Nullable String bypassRegex, String rawUri) {
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, rawUri);
    assertUnauthorizedRequest(bypassRegex, request);
  }

  private static void assertUnauthorizedRequest(@Nullable String bypassRegex, FullHttpRequest request) {
    CConfiguration cConf = createBaseConfig(bypassRegex);
    EmbeddedChannel channel = createChannel(
      cConf, new MockAccessTokenIdentityExtractor(new MockTokenValidator("failme")));
    try {
      Assert.assertFalse(channel.writeInbound(request));
      Assert.assertNull(channel.readInbound());
      FullHttpResponse response = channel.readOutbound();
      Assert.assertNotNull(response);
      try {
        Assert.assertEquals(HttpResponseStatus.UNAUTHORIZED, response.status());
      } finally {
        ReferenceCountUtil.release(response);
      }
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  private static CConfiguration createBaseConfig(@Nullable String bypassRegex) {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.set(Constants.Security.AUTH_SERVER_ANNOUNCE_URLS, "http://127.0.0.1:10000");
    if (bypassRegex != null) {
      cConf.set(Constants.Security.Router.BYPASS_AUTHENTICATION_REGEX, bypassRegex);
    }
    return cConf;
  }

  private static EmbeddedChannel createChannel(CConfiguration cConf, UserIdentityExtractor extractor) {
    AuthenticationHandler handler = new AuthenticationHandler(
      cConf, SConfiguration.create(), new InMemoryDiscoveryService(), extractor, new NoOpAeadCipher());
    return new EmbeddedChannel(handler);
  }
}
