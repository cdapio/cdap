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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link RuntimeScopes}.
 */
public class RuntimeScopesTest {

  @Test
  public void testJsonRoundTrip() {
    RuntimeScopes original = new RuntimeScopes(
        ImmutableSet.of("banking_db_conn", "*"),
        ImmutableSet.of("google-drive/my-cred", "salesforce/*"),
        ImmutableSet.of("prod-db-password", "region-*-api-key"),
        true);

    String json = original.toJson();
    JsonObject obj = new JsonParser().parse(json).getAsJsonObject();
    Assert.assertTrue(obj.has("connections"));
    Assert.assertTrue(obj.has("oauth"));
    Assert.assertTrue(obj.has("secrets"));
    Assert.assertTrue(obj.get("hasExternalDynamicSecret").getAsBoolean());

    RuntimeScopes decoded = RuntimeScopes.fromJson(json);
    Assert.assertEquals(original, decoded);
    Assert.assertEquals(original.hashCode(), decoded.hashCode());
    Assert.assertEquals(ImmutableSet.of("banking_db_conn", "*"), decoded.getConnections());
    Assert.assertEquals(ImmutableSet.of("google-drive/my-cred", "salesforce/*"), decoded.getOauth());
    Assert.assertEquals(ImmutableSet.of("prod-db-password", "region-*-api-key"), decoded.getSecrets());
    Assert.assertTrue(decoded.hasExternalDynamicSecret());
  }

  @Test
  public void testJsonRoundTripEmptyAndFalseDynamic() {
    RuntimeScopes original = new RuntimeScopes(
        Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), false);

    RuntimeScopes decoded = RuntimeScopes.fromJson(original.toJson());
    Assert.assertEquals(original, decoded);
    Assert.assertTrue(decoded.getConnections().isEmpty());
    Assert.assertTrue(decoded.getOauth().isEmpty());
    Assert.assertTrue(decoded.getSecrets().isEmpty());
    Assert.assertFalse(decoded.hasExternalDynamicSecret());
  }

  @Test
  public void testFromJsonMissingNullAndUnknownFields() {
    RuntimeScopes fromEmpty = RuntimeScopes.fromJson("{}");
    Assert.assertTrue(fromEmpty.getConnections().isEmpty());
    Assert.assertTrue(fromEmpty.getOauth().isEmpty());
    Assert.assertTrue(fromEmpty.getSecrets().isEmpty());
    Assert.assertFalse(fromEmpty.hasExternalDynamicSecret());

    RuntimeScopes fromNulls = RuntimeScopes.fromJson(
        "{\"connections\":null,\"oauth\":null,\"secrets\":null,"
            + "\"hasExternalDynamicSecret\":null,\"futureField\":123}");
    Assert.assertEquals(fromEmpty, fromNulls);
  }

  @Test
  public void testFromJsonInvalidInputsRejected() {
    List<String> invalidInputs = ImmutableList.of(
        "",
        "   ",
        "null",
        "[]",
        "123",
        "\"text\"",
        "{not-json",
        "{\"connections\":\"not-an-array\"}",
        "{\"oauth\":123}",
        "{\"secrets\":{}}",
        "{\"connections\":[null]}",
        "{\"oauth\":[42]}",
        "{\"secrets\":[true]}",
        "{\"hasExternalDynamicSecret\":\"true\"}",
        "{\"hasExternalDynamicSecret\":1}",
        "{\"hasExternalDynamicSecret\":[]}",
        "{\"hasExternalDynamicSecret\":{}}",
        "{\"connections\":[]} trailing",
        "{}{}");

    assertThrows("null input", JsonParseException.class, () -> RuntimeScopes.fromJson(null));
    for (String input : invalidInputs) {
      assertThrows("input: " + input, JsonParseException.class, () -> RuntimeScopes.fromJson(input));
    }
  }

  @Test
  public void testImmutabilityAndDefensiveCopies() {
    Set<String> connections = new HashSet<>(Collections.singleton("conn_a"));
    Set<String> oauth = new HashSet<>(Collections.singleton("p/c"));
    Set<String> secrets = new HashSet<>(Collections.singleton("sec_a"));

    final RuntimeScopes scopes = new RuntimeScopes(connections, oauth, secrets, false);
    connections.add("conn_b");
    oauth.add("p/other");
    secrets.add("sec_b");

    Assert.assertEquals(Collections.singleton("conn_a"), scopes.getConnections());
    Assert.assertEquals(Collections.singleton("p/c"), scopes.getOauth());
    Assert.assertEquals(Collections.singleton("sec_a"), scopes.getSecrets());

    assertThrows("connections", UnsupportedOperationException.class,
        () -> scopes.getConnections().add("mutated"));
    assertThrows("oauth", UnsupportedOperationException.class,
        () -> scopes.getOauth().add("mutated"));
    assertThrows("secrets", UnsupportedOperationException.class,
        () -> scopes.getSecrets().add("mutated"));
  }

  @Test
  public void testConstructorRejectsNulls() {
    assertThrows("null connections", NullPointerException.class,
        () -> new RuntimeScopes(null, Collections.emptySet(), Collections.emptySet(), false));
    assertThrows("null oauth", NullPointerException.class,
        () -> new RuntimeScopes(Collections.emptySet(), null, Collections.emptySet(), false));
    assertThrows("null secrets", NullPointerException.class,
        () -> new RuntimeScopes(Collections.emptySet(), Collections.emptySet(), null, false));

    Set<String> setWithNull = Collections.singleton(null);
    assertThrows("null connections element", NullPointerException.class,
        () -> new RuntimeScopes(setWithNull, Collections.emptySet(), Collections.emptySet(), false));
    assertThrows("null oauth element", NullPointerException.class,
        () -> new RuntimeScopes(Collections.emptySet(), setWithNull, Collections.emptySet(), false));
    assertThrows("null secrets element", NullPointerException.class,
        () -> new RuntimeScopes(Collections.emptySet(), Collections.emptySet(), setWithNull, false));
  }

  @Test
  public void testEqualsAndHashCodeContract() {
    RuntimeScopes base = new RuntimeScopes(
        Collections.singleton("c1"), Collections.singleton("p/c"),
        Collections.singleton("s1"), false);
    Assert.assertEquals(base, base);
    Assert.assertNotEquals(base, null);
    Assert.assertNotEquals(base, "not-scopes");
    Assert.assertNotEquals(base, new RuntimeScopes(
        Collections.singleton("c2"), Collections.singleton("p/c"),
        Collections.singleton("s1"), false));
    Assert.assertNotEquals(base, new RuntimeScopes(
        Collections.singleton("c1"), Collections.singleton("p/other"),
        Collections.singleton("s1"), false));
    Assert.assertNotEquals(base, new RuntimeScopes(
        Collections.singleton("c1"), Collections.singleton("p/c"),
        Collections.singleton("s2"), false));
    Assert.assertNotEquals(base, new RuntimeScopes(
        Collections.singleton("c1"), Collections.singleton("p/c"),
        Collections.singleton("s1"), true));
  }

  @Test
  public void testAllowsSecretExactPatternMetacharsAndDynamic() {
    RuntimeScopes scoped = new RuntimeScopes(
        Collections.emptySet(), Collections.emptySet(),
        ImmutableSet.of(
            "exact-secret",
            "prefix-*",
            "*-suffix",
            "region-*-api-key",
            "multi-*-*-end",
            "*b",
            "literal.a+b?(c)[d]{1}^$|\\",
            "regex.meta-*-$end[0]"),
        false);

    // Exact match; literal '*' in requested name is not treated as a caller wildcard
    Assert.assertTrue(scoped.allowsSecret("exact-secret"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED, scoped.checkSecret("exact-secret"));
    Assert.assertFalse(scoped.allowsSecret("exact-secret-2"));
    Assert.assertFalse(scoped.allowsSecret("other"));
    Assert.assertFalse(scoped.allowsSecret("*"));
    Assert.assertFalse(scoped.allowsSecret("exact-*"));

    // Pattern match: leading '*', trailing '*' (0-char and multi-char), middle '*', multi-'*'
    Assert.assertTrue(scoped.allowsSecret("prefix-"));
    Assert.assertTrue(scoped.allowsSecret("prefix-abc"));
    Assert.assertTrue(scoped.allowsSecret("prefix-**"));
    RuntimeScopes trailingMultiStar = new RuntimeScopes(
        Collections.emptySet(), Collections.emptySet(), Collections.singleton("prefix-**"), false);
    Assert.assertTrue(trailingMultiStar.allowsSecret("prefix-"));
    Assert.assertTrue(scoped.allowsSecret("-suffix"));
    Assert.assertTrue(scoped.allowsSecret("prod-suffix"));
    Assert.assertTrue(scoped.allowsSecret("region-us-east-1-api-key"));
    Assert.assertTrue(scoped.allowsSecret("region--api-key"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED,
        scoped.checkSecret("region-us-api-key"));
    Assert.assertFalse(scoped.allowsSecret("region-us-api-key-extra"));
    Assert.assertTrue(scoped.allowsSecret("multi-a-b-end"));
    Assert.assertTrue(scoped.allowsSecret("multi---end"));
    Assert.assertFalse(scoped.allowsSecret("multi-a-end"));

    // '*' inside requested value must still match wildcard patterns (backtracking preserved)
    Assert.assertTrue(scoped.allowsSecret("*ab"));
    Assert.assertTrue(scoped.allowsSecret("region-*-prod-api-key"));

    // Regex metacharacters in both exact names and patterns are matched literally
    Assert.assertTrue(scoped.allowsSecret("literal.a+b?(c)[d]{1}^$|\\"));
    Assert.assertFalse(scoped.allowsSecret("literalXa+b?(c)[d]{1}^$|\\"));
    Assert.assertTrue(scoped.allowsSecret("regex.meta-mid-$end[0]"));
    Assert.assertFalse(scoped.allowsSecret("regexXmeta-mid-$end[0]"));

    // Null / empty denied
    Assert.assertFalse(scoped.allowsSecret(null));
    Assert.assertFalse(scoped.allowsSecret(""));

    // All-wildcard '*' / '**' entry in secrets admits as ALLOWED_BY_WILDCARD (even with
    // dynamic=false); put '*' first in insertion order to prove partial patterns still win ALLOWED.
    RuntimeScopes starSecrets = new RuntimeScopes(
        Collections.emptySet(), Collections.emptySet(),
        ImmutableSet.of("*", "region-*-key", "known-secret"), false);
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED, starSecrets.checkSecret("known-secret"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED,
        starSecrets.checkSecret("region-us-key"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD,
        starSecrets.checkSecret("any-other-secret"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD,
        starSecrets.checkSecret("*"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD,
        starSecrets.checkSecret("**"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD,
        starSecrets.checkSecret("*foo"));

    RuntimeScopes doubleStarOnly = new RuntimeScopes(
        Collections.emptySet(), Collections.emptySet(), Collections.singleton("**"), false);
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD,
        doubleStarOnly.checkSecret("any-secret"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD,
        doubleStarOnly.checkSecret("**"));

    // Dynamic run: explicit exact/pattern match returns ALLOWED; others return ALLOWED_BY_WILDCARD
    RuntimeScopes dynamic = new RuntimeScopes(
        Collections.emptySet(), Collections.emptySet(),
        ImmutableSet.of("known-secret", "region-*-key"), true);
    Assert.assertTrue(dynamic.allowsSecret("known-secret"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED, dynamic.checkSecret("known-secret"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED, dynamic.checkSecret("region-us-key"));
    Assert.assertTrue(dynamic.allowsSecret("any-other-secret"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD,
        dynamic.checkSecret("any-other-secret"));
    Assert.assertFalse(dynamic.allowsSecret(null));
    Assert.assertFalse(dynamic.allowsSecret(""));
  }

  @Test
  public void testAllowsConnectionNormalizationAndWildcard() {
    String fiftyAs = Strings.repeat("a", 50);
    RuntimeScopes scoped = new RuntimeScopes(
        ImmutableSet.of("my_conn", "a_b_c", "A_B_c", fiftyAs, "_"),
        Collections.emptySet(), Collections.emptySet(), false);

    // Inner space, '+', punctuation, leading/trailing spaces
    Assert.assertTrue(scoped.allowsConnection("my conn"));
    Assert.assertTrue(scoped.allowsConnection("my+conn"));
    Assert.assertTrue(scoped.allowsConnection("  my conn  "));
    Assert.assertTrue(scoped.allowsConnection("a.b-c"));
    Assert.assertTrue(scoped.allowsConnection("a^b%c"));

    // Case is preserved (matches ConnectionId.getConnectionId)
    Assert.assertTrue(scoped.allowsConnection("A%B%c"));
    Assert.assertFalse(scoped.allowsConnection("A%B%C"));

    // 50-character truncation (after trim(), and macro-char check happens before truncation)
    Assert.assertTrue(scoped.allowsConnection(fiftyAs + "extraCharsBeyondFifty"));
    Assert.assertTrue(scoped.allowsConnection("  " + fiftyAs + "extra  "));
    Assert.assertFalse(scoped.allowsConnection("b" + fiftyAs));
    Assert.assertFalse(scoped.allowsConnection(fiftyAs + "${x}"));

    // Literal '*' normalizes to '_', NOT the wildcard '*'
    Assert.assertTrue(scoped.allowsConnection("*"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED, scoped.checkConnection("*"));
    RuntimeScopes withoutUnderscore = new RuntimeScopes(
        Collections.singleton("my_conn"),
        Collections.emptySet(), Collections.emptySet(), false);
    Assert.assertFalse(withoutUnderscore.allowsConnection("*"));

    // Segments rejected by the connection ID rule are denied without throwing
    List<String> rejectedSegments = ImmutableList.of(
        "", "   ", "$", "${conn}", "a{b}", "a}b", "a(b)", "a)b", fiftyAs + "${x}");
    for (String bad : rejectedSegments) {
      Assert.assertFalse("Expected denied for: " + bad, scoped.allowsConnection(bad));
    }
    Assert.assertFalse(scoped.allowsConnection(null));

    // Wildcard '*' entry admits valid segments as ALLOWED_BY_WILDCARD, still denies invalid ones
    RuntimeScopes wildcard = new RuntimeScopes(
        ImmutableSet.of("known_conn", "*"),
        Collections.emptySet(), Collections.emptySet(), false);
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED, wildcard.checkConnection("known conn"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD,
        wildcard.checkConnection("other conn"));
    for (String bad : rejectedSegments) {
      Assert.assertFalse("Wildcard must still deny invalid segment: " + bad,
          wildcard.allowsConnection(bad));
    }
    Assert.assertFalse(wildcard.allowsConnection(null));
  }

  @Test
  public void testAllowsOauthPerPartAndWildcards() {
    // Exact entry only
    RuntimeScopes exact = new RuntimeScopes(
        Collections.emptySet(),
        ImmutableSet.of("github/cred1", "malformed", "/missing-provider", "missing-cred/", "a/b/c"),
        Collections.emptySet(), false);
    Assert.assertTrue(exact.allowsOauth("github", "cred1"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED, exact.checkOauth("github", "cred1"));
    Assert.assertFalse(exact.allowsOauth("github", "cred2"));
    Assert.assertFalse(exact.allowsOauth("gitlab", "cred1"));
    Assert.assertFalse(exact.allowsOauth("*", "cred1"));
    Assert.assertFalse(exact.allowsOauth("github", "*"));
    Assert.assertFalse(exact.allowsOauth(null, "cred1"));
    Assert.assertFalse(exact.allowsOauth("github", null));
    Assert.assertFalse(exact.allowsOauth("", "cred1"));
    Assert.assertFalse(exact.allowsOauth("github", ""));
    Assert.assertFalse(exact.allowsOauth("a/b", "c"));
    Assert.assertFalse(exact.allowsOauth("a", "b/c"));

    // Provider wildcard: */cred1 (put wildcard before exact entry to exercise exact fast-path)
    RuntimeScopes providerWildcard = new RuntimeScopes(
        Collections.emptySet(), ImmutableSet.of("*/cred1", "github/cred1"),
        Collections.emptySet(), false);
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED,
        providerWildcard.checkOauth("github", "cred1"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD,
        providerWildcard.checkOauth("any-provider", "cred1"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD,
        providerWildcard.checkOauth("*", "cred1"));
    Assert.assertFalse(providerWildcard.allowsOauth("any-provider", "cred2"));

    // Credential wildcard: github/* (put wildcard before exact entry to exercise exact fast-path)
    RuntimeScopes credWildcard = new RuntimeScopes(
        Collections.emptySet(), ImmutableSet.of("github/*", "github/exact-cred"),
        Collections.emptySet(), false);
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED,
        credWildcard.checkOauth("github", "exact-cred"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD,
        credWildcard.checkOauth("github", "other-cred"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD,
        credWildcard.checkOauth("github", "*"));
    Assert.assertFalse(credWildcard.allowsOauth("gitlab", "other-cred"));

    // Both wildcard: */*
    RuntimeScopes bothWildcard = new RuntimeScopes(
        Collections.emptySet(), Collections.singleton("*/*"),
        Collections.emptySet(), false);
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD,
        bothWildcard.checkOauth("any-provider", "any-cred"));
    Assert.assertEquals(RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD,
        bothWildcard.checkOauth("*", "*"));

    // Partial '*' inside either part is not a wildcard
    RuntimeScopes partialStar = new RuntimeScopes(
        Collections.emptySet(), ImmutableSet.of("git*/cred1", "github/cred*"),
        Collections.emptySet(), false);
    Assert.assertFalse(partialStar.allowsOauth("github", "cred1"));
  }

  private static void assertThrows(String description, Class<? extends Throwable> expectedType,
      Runnable action) {
    try {
      action.run();
    } catch (Throwable actual) {
      if (expectedType.isInstance(actual)) {
        return;
      }
      throw new AssertionError("Unexpected exception for " + description + ": " + actual, actual);
    }
    Assert.fail("Expected " + expectedType.getSimpleName() + " for " + description);
  }
}
