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

package io.cdap.cdap.datapipeline.oauth;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit tests for {@link UrlOrigin}.
 */
public class UrlOriginTest {

  @Test
  public void testHasSameOriginMatchesEquivalentOrigins() {
    assertTrue(UrlOrigin.hasSameOrigin(
        "https://auth.internal/o/oauth2/auth?scope=openid email",
        "HTTPS://AUTH.INTERNAL:443/v2/auth?prompt=login|consent"));
    assertTrue(UrlOrigin.hasSameOrigin(
        "https://oauth_gateway.internal:8443/v1/token",
        "https://OAUTH_GATEWAY.INTERNAL:8443/v2/token"));
    assertTrue(UrlOrigin.hasSameOrigin(
        "http://127.0.0.1/token",
        "http://127.0.0.1:80/v2/token"));
    assertTrue(UrlOrigin.hasSameOrigin(
        "https://auth.internal./services/oauth2/token",
        "https://auth.internal/services/oauth2/token"));
    assertTrue(UrlOrigin.hasSameOrigin(
        "http://[::1]/token",
        "http://[::1]:80/v2/token"));
    assertTrue(UrlOrigin.hasSameOrigin(
        "https://[2001:db8::1]:8443/a",
        "https://[2001:DB8::1]:8443/b"));
  }

  @Test
  public void testHasSameOriginRejectsMismatchedOrigins() {
    assertFalse(UrlOrigin.hasSameOrigin("https://127.0.0.1/token", "https://127.0.0.2/token"));
    assertFalse(UrlOrigin.hasSameOrigin("https://127.0.0.1/token", "http://127.0.0.1/token"));
    assertFalse(UrlOrigin.hasSameOrigin("https://127.0.0.1:8443/token", "https://127.0.0.1:9443/token"));
    assertFalse(UrlOrigin.hasSameOrigin("http://[::1]:8080/token", "http://[::1]:9090/token"));
  }

  @Test
  public void testRejectsUserinfoInvalidPortsAndMalformedUrls() {
    assertFalse(UrlOrigin.isValid("https://user@127.0.0.1/token"));
    assertFalse(UrlOrigin.isValid("https://@127.0.0.1/token"));
    assertFalse(UrlOrigin.isValid("https://127.0.0.1\\@127.0.0.2/token"));
    assertFalse(UrlOrigin.isValid("https://auth.internal../token"));
    assertFalse(UrlOrigin.isValid("https://./token"));
    assertFalse(UrlOrigin.isValid("https://.auth.internal/token"));
    assertFalse(UrlOrigin.isValid("https://127.0.0.1:0/token"));
    assertFalse(UrlOrigin.isValid("https://127.0.0.1:65536/token"));
    assertFalse(UrlOrigin.isValid("https://127.0.0.1:+443/token"));
    assertFalse(UrlOrigin.isValid("file:///etc/passwd"));
    assertFalse(UrlOrigin.isValid("//auth.internal/token"));
    assertFalse(UrlOrigin.isValid("/relative/path"));
    assertFalse(UrlOrigin.isValid("not a url"));
    assertFalse(UrlOrigin.isValid(null));
    assertFalse(UrlOrigin.hasSameOrigin("https://127.0.0.1/token", "https://127.0.0.1@127.0.0.2/token"));
  }

  @Test
  public void testRejectsNonHttpSchemesEmptyPortsAndUnsafeCharacters() {
    assertFalse(UrlOrigin.isValid("ftp://auth.internal/token"));
    assertFalse(UrlOrigin.isValid("file://localhost/etc/hosts"));
    assertFalse(UrlOrigin.isValid("jar:https://auth.internal/a.jar!/token"));
    assertFalse(UrlOrigin.hasSameOrigin("ftp://auth.internal/a", "ftp://auth.internal/b"));
    assertFalse(UrlOrigin.isValid("https://auth.internal:-1/token"));
    assertFalse(UrlOrigin.isValid("https://auth.internal:/token"));
    assertFalse(UrlOrigin.isValid("http://[::1]:/token"));
    assertFalse(UrlOrigin.isValid("https://auth.internal;param=1/token"));
    assertFalse(UrlOrigin.isValid("https://auth.internal/to\r\nken"));
    assertFalse(UrlOrigin.isValid("https://auth.internal/token\u0000"));
    assertFalse(UrlOrigin.isValid("https://auth.internal/token\u007f"));
    assertTrue(UrlOrigin.isValid("HTTP://auth.internal/token?scope=openid email"));
    assertTrue(UrlOrigin.isValid("https://oauth-gw_1.internal:8443/token"));
  }
}
