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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.secretmanager.v1.AddSecretVersionRequest;
import com.google.cloud.secretmanager.v1.CreateSecretRequest;
import com.google.cloud.secretmanager.v1.ListSecretsRequest;
import com.google.cloud.secretmanager.v1.Secret;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient.ListSecretsPage;
import com.google.cloud.secretmanager.v1.SecretManagerServiceSettings;
import com.google.cloud.secretmanager.v1.SecretPayload;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.FieldMask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Optional;

/** Client for <a href="https://cloud.google.com/secret-manager">Google Cloud Secret Manager</a> */
public class CloudSecretManagerClient {
  private static final Logger LOG = LoggerFactory.getLogger(CloudSecretManagerClient.class);
  /**
   * Property that, if set, overrides which GCP project to use rather than the default (see
   * https://github.com/googleapis/google-cloud-java#specifying-a-project-id for default behavior).
   */
  private static final String PROJECT_ID = "project.id";
  /**
   * Property that, if set, overrides which service account to authorize as rather than the default
   * (see https://github.com/googleapis/google-cloud-java#authentication for default behavior)
   */
  private static final String SERVICE_ACCOUNT_FILE = "service.account.file";

  private final SecretManagerServiceClient secretManager;
  // GCP project resource name e.g. "project/my-project-id".
  private final String projectResourceName;

  /** Throws IOException if the underlying {@link SecretManagerServiceClient} fails to be created. */
  public CloudSecretManagerClient(Map<String, String> properties) throws IOException {
    String projectId =
      properties.containsKey(PROJECT_ID)
        ? properties.get(PROJECT_ID)
        : ServiceOptions.getDefaultProjectId();
    this.secretManager = createClient(properties);
    this.projectResourceName = "projects/" + projectId;
  }

  /**
   * Creates a new secret, throwing if the secret already exists.
   *
   * @throws ApiException if Google API call fails.
   */
  public void createSecret(WrappedSecret wrappedSecret) {
    CreateSecretRequest request =
      CreateSecretRequest.newBuilder()
        .setParent(projectResourceName)
        .setSecretId(
          getSecretId(
            wrappedSecret.getNamespace(), wrappedSecret.getCdapSecretMetadata().getName()))
        .setSecret(wrappedSecret.getGcpSecret(getSecretResourceName(wrappedSecret)))
        .build();

    secretManager.createSecret(request);
  }

  /**
   * Adds a new secret payload to the provided secret.
   *
   * @throws ApiException if Google API call fails.
   */
  public void addSecretVersion(WrappedSecret wrappedSecret, byte[] data) {
    SecretPayload secretPayload =
      SecretPayload.newBuilder().setData(ByteString.copyFrom(data)).build();
    AddSecretVersionRequest addSecretReq =
      AddSecretVersionRequest.newBuilder()
        .setParent(getSecretResourceName(wrappedSecret))
        .setPayload(secretPayload)
        .build();

    secretManager.addSecretVersion(addSecretReq);
  }

  /**
   * Returns the secrets in the specified namespace. Does not fetch their secret payloads. Iterates
   * over all pages.
   *
   * @throws ApiException if Google API call fails.
   */
  public ImmutableList<WrappedSecret> listSecrets(String namespace) {
    ListSecretsRequest request =
      ListSecretsRequest.newBuilder()
        .setFilter("annotations.cdap_namespace=" + namespace)
        .setParent(projectResourceName)
        .build();

    ImmutableList.Builder<WrappedSecret> secrets = ImmutableList.builder();
    for (ListSecretsPage page : secretManager.listSecrets(request).iteratePages()) {
      for (Secret secret : page.getResponse().getSecretsList()) {
        try {
          secrets.add(WrappedSecret.fromGcpSecret(secret));
        } catch (InvalidSecretException e) {
          // Log and ignore parse failures.
          LOG.error("Failed to parse secret.", e);
        }
      }
    }
    return secrets.build();
  }

  /**
   * Fetches the latest secret payload associated with the specified secret.
   *
   * @throws ApiException if Google API call fails.
   */
  public byte[] getSecretData(WrappedSecret secret) {
    return secretManager
      .accessSecretVersion(String.format("%s/versions/latest", getSecretResourceName(secret)))
      .getPayload()
      .getData()
      .toByteArray();
  }

  /**
   * Returns the secret metadata for the specified secret. Does not fetch its secret payload.
   *
   * @throws ApiException if Google API call fails.
   * @throws InvalidSecretException if parsing the JSON-encoded annotations fails.
   */
  public WrappedSecret getSecret(String namespace, String name) throws InvalidSecretException {
    return WrappedSecret.fromGcpSecret(
      secretManager.getSecret(getSecretResourceName(namespace, name)));
  }

  /**
   * Computes the new annotations for {@link wrappedSecret} and performs an update operation on the
   * corresponding GCP secret to match the newly computed properties.
   *
   * @throws ApiException if Google API call fails.
   */
  public void updateSecret(WrappedSecret wrappedSecret) {
    // Update all fields.
    secretManager.updateSecret(
      wrappedSecret.getGcpSecret(getSecretResourceName(wrappedSecret)),
      FieldMask.newBuilder().addPaths("annotations").build());
  }

  /**
   * Deletes the specified secret.
   *
   * @throws ApiException if Google API call fails.
   */
  public void deleteSecret(String namespace, String name) {
    secretManager.deleteSecret(getSecretResourceName(namespace, name));
  }

  public void destroy() {
    secretManager.close();
  }

  /** Returns the GCP resource name for the specified WrappedSecret. */
  private String getSecretResourceName(WrappedSecret secret) {
    return getSecretResourceName(secret.getNamespace(), secret.getCdapSecretMetadata().getName());
  }

  private String getSecretResourceName(String namespace, String name) {
    return String.format("%s/secrets/%s", projectResourceName, getSecretId(namespace, name));
  }

  /**
   * Computes the secret ID, returning an ID guaranteed to be shorter than 255 characters (the
   * Secret Manager limit) and guaranteed to be prefixed with "cdap-" to allow the use of IAM
   * conditionals to widely grant or revoke access to secrets created by this client.
   */
  private static String getSecretId(String namespace, String name) {
    return String.format("cdap-%s-%s", hashLongName(namespace), hashLongName(name));
  }

  /**
   * Returns {@code input} unmodified if it is at most 120 characters long, otherwise returns the
   * first 56 characters and appends a hex-encoded sha256 hash of the entire name (120 characters
   * total).
   */
  private static String hashLongName(String input) {
    if (input.length() <= 120) {
      return input;
    }
    try {
      byte[] hash =
        MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8));
      StringBuilder hexString = new StringBuilder();
      for (byte b : hash) {
        hexString.append(String.format("%02X", b));
      }
      return input.substring(0, 30) + hexString.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("Unable to generate SHA-256 hash", e);
    }
  }

  private static SecretManagerServiceClient createClient(Map<String, String> properties) throws IOException {
    SecretManagerServiceSettings.Builder settings = SecretManagerServiceSettings.newBuilder();

    createCredentialsProvider(properties)
      .ifPresent(
        (credentials) -> {
          settings.setCredentialsProvider(credentials);
        });
    return SecretManagerServiceClient.create(settings.build());
  }

  /**
   * Returns a CredentialProvider if a service account file is configured in {@code properties},
   * otherwise returns empty.
   */
  private static Optional<CredentialsProvider> createCredentialsProvider(Map<String, String> properties)
      throws IOException {
    String serviceAccountContents = properties.getOrDefault(SERVICE_ACCOUNT_FILE, "");
    if (serviceAccountContents.isEmpty()) {
      return Optional.empty();
    }
    GoogleCredentials credentials =
      GoogleCredentials.fromStream(new ByteArrayInputStream(serviceAccountContents.getBytes()));

    return Optional.of(FixedCredentialsProvider.create(credentials));
  }
}
