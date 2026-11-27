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
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Immutable set of resource scopes allowed for a remote program run.
 */
public final class RuntimeScopes {

  public static final String WILDCARD = "*";
  public static final String OAUTH_WILDCARD = "*/*";

  private static final RuntimeScopes WILDCARD_ALL = new RuntimeScopes(
      Collections.singleton(WILDCARD),
      Collections.singleton(OAUTH_WILDCARD),
      Collections.singleton(WILDCARD),
      true);

  private static final Gson GSON = new Gson();
  private static final String FIELD_CONNECTIONS = "connections";
  private static final String FIELD_OAUTH = "oauth";
  private static final String FIELD_SECRETS = "secrets";
  private static final String FIELD_HAS_EXTERNAL_DYNAMIC_SECRET = "hasExternalDynamicSecret";

  // Keep in sync with io.cdap.cdap.etl.proto.connection.ConnectionId#getConnectionId.
  private static final Pattern CONNECTION_MACRO_CHARS = Pattern.compile("[${}()]");
  private static final Pattern CONNECTION_NON_ID_CHARS = Pattern.compile("[^a-zA-Z0-9_]");
  private static final int MAX_CONNECTION_ID_LENGTH = 50;

  enum ScopeMatch {
    DENIED,
    ALLOWED,
    ALLOWED_BY_WILDCARD
  }

  @SerializedName(FIELD_CONNECTIONS)
  private final Set<String> connections;

  @SerializedName(FIELD_OAUTH)
  private final Set<String> oauth;

  @SerializedName(FIELD_SECRETS)
  private final Set<String> secrets;

  @SerializedName(FIELD_HAS_EXTERNAL_DYNAMIC_SECRET)
  private final boolean hasExternalDynamicSecret;

  /**
   * Creates an immutable {@link RuntimeScopes} instance.
   *
   * @param connections allowed connection IDs (or {@code "*"})
   * @param oauth allowed OAuth entries formatted as {@code provider/credential} (each part a name or {@code "*"})
   * @param secrets allowed secret names or wildcard patterns containing {@code "*"}
   * @param hasExternalDynamicSecret whether the run may access dynamic secrets within its namespace
   */
  public RuntimeScopes(Collection<String> connections, Collection<String> oauth,
      Collection<String> secrets, boolean hasExternalDynamicSecret) {
    this.connections = ImmutableSet.copyOf(Objects.requireNonNull(connections, FIELD_CONNECTIONS));
    this.oauth = ImmutableSet.copyOf(Objects.requireNonNull(oauth, FIELD_OAUTH));
    this.secrets = ImmutableSet.copyOf(Objects.requireNonNull(secrets, FIELD_SECRETS));
    this.hasExternalDynamicSecret = hasExternalDynamicSecret;
  }

  public Set<String> getConnections() {
    return connections;
  }

  public Set<String> getOauth() {
    return oauth;
  }

  public Set<String> getSecrets() {
    return secrets;
  }

  public boolean hasExternalDynamicSecret() {
    return hasExternalDynamicSecret;
  }

  /**
   * Returns a wildcard {@link RuntimeScopes} allowing all connections, OAuth credentials, and
   * secrets within the run's namespace.
   */
  public static RuntimeScopes wildcardAll() {
    return WILDCARD_ALL;
  }

  /**
   * Computes the normalized connection ID for a connection name as written in a
   * {@code ${conn(...)}} macro. Returns {@link Optional#empty()} for {@code null}, {@code "."},
   * {@code ".."}, names containing {@code '/'}, or names rejected by the connection ID rule.
   * Never throws.
   */
  public static Optional<String> connectionIdFromMacroName(@Nullable String macroName) {
    if (macroName == null || ".".equals(macroName) || "..".equals(macroName)
        || macroName.indexOf('/') >= 0) {
      return Optional.empty();
    }
    // ConnectionMacroEvaluator URL-encodes the connection name, converting spaces to '+'.
    // Apply the same transformation before the ID rule so leading/trailing or space-only names
    // match the segment the runtime client sends.
    return Optional.ofNullable(toConnectionId(macroName.replace(' ', '+')));
  }

  /**
   * Returns {@code true} if the requested secret name is allowed by this scope set (either via an
   * exact name, a {@code *} wildcard pattern, or the dynamic-secret flag).
   */
  public boolean allowsSecret(@Nullable String secretName) {
    return checkSecret(secretName) != ScopeMatch.DENIED;
  }

  /**
   * Returns {@code true} if the requested connection path segment is allowed by this scope set.
   * Applies the connection ID normalization rule and denies segments rejected by that rule without
   * throwing.
   */
  public boolean allowsConnection(@Nullable String connectionSegment) {
    return checkConnection(connectionSegment) != ScopeMatch.DENIED;
  }

  /**
   * Returns {@code true} if the requested OAuth provider and credential pair is allowed by this
   * scope set (matching exact names or {@code *} per part).
   */
  public boolean allowsOauth(@Nullable String provider, @Nullable String credential) {
    return checkOauth(provider, credential) != ScopeMatch.DENIED;
  }

  ScopeMatch checkSecret(@Nullable String secretName) {
    if (Strings.isNullOrEmpty(secretName)) {
      return ScopeMatch.DENIED;
    }
    if (secretName.indexOf('*') < 0 && secrets.contains(secretName)) {
      return ScopeMatch.ALLOWED;
    }
    boolean matchedAllWildcard = false;
    for (String entry : secrets) {
      if (entry.indexOf('*') < 0) {
        continue;
      }
      if (isAllWildcards(entry)) {
        matchedAllWildcard = true;
      } else if (matchesWildcard(entry, secretName)) {
        return ScopeMatch.ALLOWED;
      }
    }
    return (hasExternalDynamicSecret || matchedAllWildcard)
        ? ScopeMatch.ALLOWED_BY_WILDCARD : ScopeMatch.DENIED;
  }

  ScopeMatch checkConnection(@Nullable String connectionSegment) {
    String connectionId = toConnectionId(connectionSegment);
    if (connectionId == null) {
      return ScopeMatch.DENIED;
    }
    if (connections.contains(connectionId)) {
      return ScopeMatch.ALLOWED;
    }
    return connections.contains(WILDCARD) ? ScopeMatch.ALLOWED_BY_WILDCARD : ScopeMatch.DENIED;
  }

  ScopeMatch checkOauth(@Nullable String provider, @Nullable String credential) {
    if (Strings.isNullOrEmpty(provider) || Strings.isNullOrEmpty(credential)
        || provider.indexOf('/') >= 0 || credential.indexOf('/') >= 0) {
      return ScopeMatch.DENIED;
    }
    if (!WILDCARD.equals(provider) && !WILDCARD.equals(credential)
        && oauth.contains(provider + "/" + credential)) {
      return ScopeMatch.ALLOWED;
    }
    for (String entry : oauth) {
      int slash = entry.indexOf('/');
      if (slash <= 0 || slash >= entry.length() - 1 || entry.indexOf('/', slash + 1) >= 0) {
        continue;
      }
      String entryProvider = entry.substring(0, slash);
      String entryCredential = entry.substring(slash + 1);
      boolean providerWildcard = WILDCARD.equals(entryProvider);
      boolean credentialWildcard = WILDCARD.equals(entryCredential);
      if ((providerWildcard || credentialWildcard)
          && (providerWildcard || entryProvider.equals(provider))
          && (credentialWildcard || entryCredential.equals(credential))) {
        return ScopeMatch.ALLOWED_BY_WILDCARD;
      }
    }
    return ScopeMatch.DENIED;
  }

  /**
   * Computes the normalized connection ID from a requested connection path segment.
   *
   * <p>Keep in sync with {@code io.cdap.cdap.etl.proto.connection.ConnectionId#getConnectionId}.
   * Returns {@code null} instead of throwing when the segment is invalid.
   */
  @Nullable
  private static String toConnectionId(@Nullable String segment) {
    if (segment == null || CONNECTION_MACRO_CHARS.matcher(segment).find()) {
      return null;
    }
    String normalized = CONNECTION_NON_ID_CHARS.matcher(segment.trim()).replaceAll("_");
    if (normalized.length() > MAX_CONNECTION_ID_LENGTH) {
      normalized = normalized.substring(0, MAX_CONNECTION_ID_LENGTH);
    }
    return normalized.isEmpty() ? null : normalized;
  }

  private static boolean isAllWildcards(String pattern) {
    if (pattern.isEmpty()) {
      return false;
    }
    for (int i = 0; i < pattern.length(); i++) {
      if (pattern.charAt(i) != '*') {
        return false;
      }
    }
    return true;
  }

  /**
   * Matches {@code value} against {@code pattern} where {@code '*'} matches any run of zero or
   * more characters and all other characters match literally.
   */
  private static boolean matchesWildcard(String pattern, String value) {
    int p = 0;
    int v = 0;
    int starIdx = -1;
    int matchIdx = 0;
    while (v < value.length()) {
      if (p < pattern.length() && pattern.charAt(p) == '*') {
        starIdx = p++;
        matchIdx = v;
      } else if (p < pattern.length() && pattern.charAt(p) == value.charAt(v)) {
        p++;
        v++;
      } else if (starIdx != -1) {
        p = starIdx + 1;
        v = ++matchIdx;
      } else {
        return false;
      }
    }
    while (p < pattern.length() && pattern.charAt(p) == '*') {
      p++;
    }
    return p == pattern.length();
  }

  /**
   * Serializes this scopes instance to JSON.
   */
  public String toJson() {
    return GSON.toJson(this);
  }

  /**
   * Deserializes a {@link RuntimeScopes} instance from JSON.
   *
   * @param json JSON representation produced by {@link #toJson()}
   * @return parsed immutable {@link RuntimeScopes}
   * @throws JsonParseException if the input is null, malformed, or has invalid field types
   */
  public static RuntimeScopes fromJson(String json) {
    if (json == null) {
      throw new JsonParseException("Runtime scopes JSON must not be null");
    }
    JsonElement element = new JsonParser().parse(json);
    if (!element.isJsonObject()) {
      throw new JsonParseException("Runtime scopes JSON must be a JSON object");
    }
    JsonObject obj = element.getAsJsonObject();
    return new RuntimeScopes(
        readStringSet(obj, FIELD_CONNECTIONS),
        readStringSet(obj, FIELD_OAUTH),
        readStringSet(obj, FIELD_SECRETS),
        readBoolean(obj, FIELD_HAS_EXTERNAL_DYNAMIC_SECRET));
  }

  private static Set<String> readStringSet(JsonObject obj, String field) {
    if (!obj.has(field) || obj.get(field).isJsonNull()) {
      return Collections.emptySet();
    }
    JsonElement value = obj.get(field);
    if (!value.isJsonArray()) {
      throw new JsonParseException("Field '" + field + "' must be a JSON array");
    }
    JsonArray array = value.getAsJsonArray();
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (JsonElement item : array) {
      if (!item.isJsonPrimitive() || !item.getAsJsonPrimitive().isString()) {
        throw new JsonParseException("Field '" + field + "' must contain only strings");
      }
      builder.add(item.getAsString());
    }
    return builder.build();
  }

  private static boolean readBoolean(JsonObject obj, String field) {
    if (!obj.has(field) || obj.get(field).isJsonNull()) {
      return false;
    }
    JsonElement value = obj.get(field);
    if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isBoolean()) {
      throw new JsonParseException("Field '" + field + "' must be a boolean");
    }
    return value.getAsBoolean();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RuntimeScopes)) {
      return false;
    }
    RuntimeScopes that = (RuntimeScopes) o;
    return hasExternalDynamicSecret == that.hasExternalDynamicSecret
        && connections.equals(that.connections)
        && oauth.equals(that.oauth)
        && secrets.equals(that.secrets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connections, oauth, secrets, hasExternalDynamicSecret);
  }

  @Override
  public String toString() {
    return "RuntimeScopes{"
        + "connections=" + connections
        + ", oauth=" + oauth
        + ", secrets=" + secrets
        + ", hasExternalDynamicSecret=" + hasExternalDynamicSecret
        + '}';
  }
}
