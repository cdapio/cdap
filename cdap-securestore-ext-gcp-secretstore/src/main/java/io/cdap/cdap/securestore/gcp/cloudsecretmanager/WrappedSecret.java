/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.securestore.gcp.cloudsecretmanager;

import com.google.cloud.secretmanager.v1.Replication;
import com.google.cloud.secretmanager.v1.Replication.Automatic;
import com.google.cloud.secretmanager.v1.Secret;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.protobuf.util.Timestamps;
import io.cdap.cdap.securestore.spi.secret.SecretMetadata;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;

/**
 * Wrapper around a {@link io.cdap.cdap.securestore.spi.secret.SecretMetadata} which allows it to
 * easily be converted a GCP SecretManager {@link com.google.cloud.secretmanager.v1.Secret} and
 * back.
 *
 * <p>Does not contain the underlying secret payload.
 */
public final class WrappedSecret {
  private static final Gson GSON = new Gson();
  private static final Type PROP_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final String namespace;
  private final SecretMetadata secretMetadata;

  private WrappedSecret(String namespace, SecretMetadata secretMetadata) {
    this.namespace = namespace;
    this.secretMetadata = secretMetadata;
  }

  /** Constructs a new WrappedSecret from a CDAP Secret. */
  public static WrappedSecret fromMetadata(String namespace, SecretMetadata metadata) {
    return new WrappedSecret(namespace, metadata);
  }

  /**
   * Constructs a new WrappedSecret from a GCP Secret Manager secret, throwing an
   * {@link InvalidSecretException} if the secret cannot be parsed.
   */
  public static WrappedSecret fromGcpSecret(Secret secret) throws InvalidSecretException {
    String namespace = getNamespace(secret);
    SecretMetadata metadata = toSecretMetadata(secret);
    return new WrappedSecret(namespace, metadata);
  }

  public String getNamespace() {
    return namespace;
  }

  public SecretMetadata getCdapSecretMetadata() {
    return secretMetadata;
  }

  /**
   * Returns a new GCP {@link Secret} representing the underlying SecretMetadata.
   *
   * @param resourceName Value to set for the "name" field needed for update operations.
   */
  public Secret getGcpSecret(String resourceName) {
    return Secret.newBuilder()
      // Set replication policy to automatic (as opposed to user-managed) and do not specify a
      // CMEK (use google-managed key).
      .setReplication(Replication.newBuilder().setAutomatic(Automatic.getDefaultInstance()))
      .setName(resourceName)
      .putAnnotations("cdap_namespace", namespace)
      .putAnnotations("cdap_secret_name", secretMetadata.getName())
      .putAnnotations("cdap_description", Optional.ofNullable(secretMetadata.getDescription()).orElse(""))
      .putAnnotations("cdap_props", serializeProps(secretMetadata.getProperties()))
      .build();
  }

  private static SecretMetadata toSecretMetadata(Secret secret) throws InvalidSecretException {
    return new SecretMetadata(
      secret.getAnnotationsOrDefault("cdap_secret_name", ""),
      secret.getAnnotationsOrDefault("cdap_description", ""),
      Timestamps.toMillis(secret.getCreateTime()),
      deserializeProps(secret.getAnnotationsOrDefault("cdap_props", "{}")));
  }

  private static String getNamespace(Secret secret) {
    return secret.getAnnotationsOrDefault("cdap_namespace", "");
  }

  private static String serializeProps(Map<String, String> input) {
    return GSON.toJson(input);
  }

  /**
   * Parses {@code input} as JSON and returns the corresponding map, throwing an {@link InvalidSecretException}
   * if parsing input fails.
   */
  private static Map<String, String> deserializeProps(String input) throws InvalidSecretException {
    try {
      return GSON.fromJson(input, PROP_MAP_TYPE);
    } catch (JsonSyntaxException e) {
      throw new InvalidSecretException("Failed to parse cdap_props", e);
    }
  }
}
