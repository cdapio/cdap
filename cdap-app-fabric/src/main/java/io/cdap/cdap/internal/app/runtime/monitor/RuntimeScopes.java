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
import java.util.Set;

/**
 * Immutable set of resource scopes allowed for a remote program run.
 */
public final class RuntimeScopes {

  private static final Gson GSON = new Gson();
  private static final String FIELD_CONNECTIONS = "connections";
  private static final String FIELD_OAUTH = "oauth";
  private static final String FIELD_SECRETS = "secrets";
  private static final String FIELD_HAS_EXTERNAL_DYNAMIC_SECRET = "hasExternalDynamicSecret";

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
