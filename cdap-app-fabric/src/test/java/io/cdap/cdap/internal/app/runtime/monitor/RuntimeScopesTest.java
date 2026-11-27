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
