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
    // Missing, opaque, and malformed URIs must not bypass auth
    assertRejectedFromBypass(null);
    assertRejectedFromBypass("foo:bar");
    assertRejectedFromBypass("/v1/repeat/%ZZ");
    assertRejectedFromBypass("/v1/repeat/%");
    assertRejectedFromBypass("/v1/repeat/a\\b");
    assertRejectedFromBypass("/v1/repeat/a b");

    // Authority and absolute-form URIs match on their path, which is what netty-http routes on
    assertCanonicalBypassPath("/v1/repeat/hi", "//evil/v1/repeat/hi");
    assertCanonicalBypassPath("/v1/repeat/hi", "///v1/repeat/hi");
    assertCanonicalBypassPath("/v1/repeat/hi", "http://evil/v1/repeat/hi");

    // Origin-form paths collapse interior/trailing slashes and strip queries/fragments, even when the
    // query or fragment contains %2F or %0a
    assertCanonicalBypassPath("/", "/");
    assertCanonicalBypassPath("/", "/?foo=bar#frag");
    assertCanonicalBypassPath("/v3/security/authorization/privileges/grant",
                              "/v3/security/authorization//privileges/grant/");
    assertCanonicalBypassPath("/v1/echo/dontfail", "/v1/echo/dontfail?foo=bar#section");
    assertCanonicalBypassPath("/v1/echo/dontfail", "/v1/echo/dontfail?redirect=%2Fhome&nl=%0a#%2f");
  }

  @Test
  public void testBypassPercentEncodingAndDotSegmentChecks() {
    // Encoded slashes are decoded to '/', so they cannot escape a single-segment wildcard
    String streamsRegex = "/v3/namespaces/default/streams/[^/]+";
    assertUnauthorized(streamsRegex, "/v3/namespaces/default/streams/s1%2fprograms");
    assertUnauthorized(streamsRegex, "/v3/namespaces/default/streams/s1%2Fprograms");
    assertCanonicalBypassPath("/v3/namespaces/default/streams/s1/programs",
                              "/v3/namespaces/default/streams/s1%2fprograms");
    assertCanonicalBypassPath("/v3/namespaces/default/streams/s1 /programs",
                              "/v3/namespaces/default/streams/s1%20%2fprograms");
    assertCanonicalBypassPath("/v1/repeat/a%/b", "/v1/repeat/a%25%2fb");

    // Literal and percent-encoded '.' and '..' segments must not bypass auth
    assertRejectedFromBypass("/v1/echo/./dontfail");
    assertRejectedFromBypass("/v1/echo/%2e/dontfail");
    assertRejectedFromBypass("/v1/repeat/../echo/hi");
    assertRejectedFromBypass("/v1/repeat/%2e%2e/echo/hi");
    assertRejectedFromBypass("/v1/repeat/%2E%2E/echo/hi");
    assertRejectedFromBypass("/../v1/repeat/hi");
    assertRejectedFromBypass("../v1/repeat/hi");
    assertRejectedFromBypass("./v1/repeat/hi");
    // Only whole '.' and '..' segments are rejected; other dot names remain valid
    assertCanonicalBypassPath("/v1/repeat/...", "/v1/repeat/...");
    assertCanonicalBypassPath("/v1/repeat/.hidden", "/v1/repeat/.hidden");
    assertCanonicalBypassPath("/v1/repeat/a../b", "/v1/repeat/a../b");
    // Normalizes to a bypassed path, but handlers that re-parse the raw URI would act on "secret"
    assertUnauthorized("/v3/namespaces/public/.*",
                       "/v3/namespaces/secret/datasets/x/metadata/../../../../.."
                         + "/namespaces/public/datasets/y/%6detadata");

    // Decoded ISO control characters and Unicode line/paragraph separators must not bypass auth
    assertRejectedFromBypass("/v1/repeat/a%00/secret");
    assertRejectedFromBypass("/v1/echo/dontfail%00");
    assertRejectedFromBypass("/v1/repeat/a%0a/secret");
    assertRejectedFromBypass("/v1/repeat/a%0d/secret");
    assertRejectedFromBypass("/v1/repeat/a%c2%85/secret");
    assertRejectedFromBypass("/v1/repeat/a%e2%80%a8/secret");
    assertRejectedFromBypass("/v1/repeat/a%e2%80%a9/secret");

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

    // Negative-lookahead regex must not be bypassed via ASCII or Unicode line-terminator injection
    String lookaheadRegex = "(?!.*/secret)/v1/repeat/[^/]+/[^/]+";
    assertBypassed(lookaheadRegex, "/v1/repeat/cat/public");
    assertUnauthorized(lookaheadRegex, "/v1/repeat/cat%0a/secret");
    assertUnauthorized(lookaheadRegex, "/v1/repeat/cat%e2%80%a8/secret");
    assertUnauthorized(lookaheadRegex, "/v1/repeat/cat%e2%80%a9/secret");
  }

  private static void assertCanonicalBypassPath(String expectedCanonicalPath, String rawUri) {
    assertBypassed(Pattern.quote(expectedCanonicalPath), rawUri);
  }

  private static void assertRejectedFromBypass(@Nullable String rawUri) {
    assertUnauthorized(MATCH_ANY_STRING_REGEX, rawUri);
  }

  private static void assertBypassed(String bypassRegex, String rawUri) {
    EmbeddedChannel channel = createChannel(bypassRegex);
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

  private static void assertUnauthorized(@Nullable String bypassRegex, @Nullable String rawUri) {
    EmbeddedChannel channel = createChannel(bypassRegex);
    try {
      FullHttpRequest request = rawUri != null
        ? new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, rawUri)
        : new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/") {
          @Override
          public String uri() {
            return null;
          }
        };
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

  private static EmbeddedChannel createChannel(@Nullable String bypassRegex) {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.set(Constants.Security.AUTH_SERVER_ANNOUNCE_URLS, "http://127.0.0.1:10000");
    if (bypassRegex != null) {
      cConf.set(Constants.Security.Router.BYPASS_AUTHENTICATION_REGEX, bypassRegex);
    }
    UserIdentityExtractor extractor =
      new MockAccessTokenIdentityExtractor(new MockTokenValidator("failme"));
    AuthenticationHandler handler = new AuthenticationHandler(
      cConf, SConfiguration.create(), new InMemoryDiscoveryService(), extractor, new NoOpAeadCipher());
    return new EmbeddedChannel(handler);
  }
}
