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

package io.cdap.cdap.common.utils;

import java.security.SecureRandom;
import java.util.Base64;

/**
 * Process-local shared bearer token used by the task worker sidecar to
 * correlate legitimate in-process callers with its context-management
 * endpoints (e.g. {@code /set-context}, {@code /clear-context}).
 *
 * <p>The sidecar HTTP service is bound to loopback and was previously
 * reachable by any code able to make an HTTP request to the loopback port,
 * including anything co-resident in the same JVM. The token is generated
 * once per JVM before the sidecar starts accepting connections and is
 * required on every call to the context-management endpoints. Only the
 * framework components that install the token can produce matching
 * requests, so an HTTP caller that does not know the token is rejected
 * before it can change the sidecar's per-task context.
 *
 * <p>The token is a 256-bit random value and is compared in constant time.
 */
public final class SidecarAuthToken {

  /**
   * HTTP header name clients must send with the token on context-management
   * endpoints.
   */
  public static final String HEADER = "X-CDAP-Sidecar-Auth";

  private static final SecureRandom RANDOM = new SecureRandom();
  private static volatile String token;

  private SidecarAuthToken() {
  }

  /**
   * Generates and installs a fresh random token if one has not already been
   * installed for this JVM. Returns the current token either way.
   *
   * <p>Intended to be called once at sidecar startup before the HTTP service
   * begins accepting requests. Subsequent calls are no-ops so that components
   * that initialize independently observe the same value.
   */
  public static synchronized String installIfAbsent() {
    if (token == null) {
      byte[] bytes = new byte[32];
      RANDOM.nextBytes(bytes);
      token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
    return token;
  }

  /**
   * Returns the current token, or {@code null} if none has been installed
   * (e.g. when the sidecar is not running in this JVM). Callers that receive
   * {@code null} should treat the token header as unavailable rather than
   * sending an empty string.
   */
  public static String get() {
    return token;
  }

  /**
   * Constant-time comparison between a candidate token supplied by a caller
   * and the installed token. Returns {@code false} if either value is
   * {@code null} or if the lengths or contents differ.
   */
  public static boolean matches(String candidate) {
    String expected = token;
    if (expected == null || candidate == null) {
      return false;
    }
    byte[] a = expected.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    byte[] b = candidate.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    if (a.length != b.length) {
      return false;
    }
    int diff = 0;
    for (int i = 0; i < a.length; i++) {
      diff |= a[i] ^ b[i];
    }
    return diff == 0;
  }
}
