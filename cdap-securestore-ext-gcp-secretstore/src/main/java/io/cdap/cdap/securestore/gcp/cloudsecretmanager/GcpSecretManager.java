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

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.securestore.spi.SecretManager;
import io.cdap.cdap.securestore.spi.SecretManagerContext;
import io.cdap.cdap.securestore.spi.SecretManagerInfo;
import io.cdap.cdap.securestore.spi.SecretNotFoundException;
import io.cdap.cdap.securestore.spi.secret.Secret;
import io.cdap.cdap.securestore.spi.secret.SecretMetadata;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SecretManager implementation backed by GCP's service also called "Secret Manager"
 * https://cloud.google.com/secret-manager
 *
 * <p>This implementation doesn't store any infromation within the CDAP database and instead stores
 * the credentials entirely inside GCP Secret Manager -- this allows credentials to be shared
 * between multiple CDAP instances without additional coordination.
 *
 * <p>Due to Secret Manager annotation <a
 * href="https://cloud.google.com/secret-manager/docs/reference/rest/v1/projects.secrets#Secret">limitations</a>
 * the total size of metadata associated with any given secret cannot exceed 16 KiB.
 */
public class GcpSecretManager implements SecretManager {
  private static final Logger LOG = LoggerFactory.getLogger(GcpSecretManager.class);
  private static final String PROVIDER_NAME = "gcp-secretmanager";

  private CloudSecretManagerClient client;

  private static final String ANNOTATION_LEASE_STATE = "lease_state";
  private static final String ANNOTATION_LEASE_ACQUIRED_TIME_MS = "lease_acquired_time_ms";
  private static final String ANNOTATION_LEASE_HOLDER = "lease_holder";

  private static final String STATE_IDLE = "idle";
  private static final String STATE_REFRESHING = "refreshing";

  private static final SecretManagerInfo SECRET_MANAGER_INFO =
      new SecretManagerInfo(EnumSet.of(SecretManagerInfo.Capability.SECRET_LEASING));

  @Override
  public String getName() {
    return PROVIDER_NAME;
  }

  @Override
  public void initialize(SecretManagerContext context) throws IOException {
    this.client = new CloudSecretManagerClient(context.getProperties());
  }

  @VisibleForTesting
  void initialize(CloudSecretManagerClient client) {
    this.client = client;
  }

  @Override
  public void store(String namespace, Secret secret) throws IOException {
    store(namespace, secret, 0L);
  }

  @Override
  public void store(String namespace, Secret secret, long ttlInSeconds) throws IOException {
    WrappedSecret wrappedSecret;
    try {
      WrappedSecret existingWrappedSecret = getWrappedSecret(namespace, secret.getMetadata().getName());
      byte[] existingData = getData(namespace, secret.getMetadata().getName());

      wrappedSecret = WrappedSecret.fromMetadata(
          namespace, secret.getMetadata(), existingWrappedSecret.getAnnotations());

      // 'update' only if 'get' request succeeds, otherwise 'create'.
      client.updateSecret(wrappedSecret, ttlInSeconds);

      // Add a new secret version only if the secret payload has changed.
      if (!Arrays.equals(existingData, secret.getData())) {
        client.addSecretVersion(wrappedSecret, secret.getData());
      }
    } catch (SecretNotFoundException unused) {
      wrappedSecret = WrappedSecret.fromMetadata(namespace, secret.getMetadata());
      try {
        client.createSecret(wrappedSecret, ttlInSeconds);
        client.addSecretVersion(wrappedSecret, secret.getData());
      } catch (ApiException e) {
        throw new IOException("Secret Manager create API call failed", e);
      }
    } catch (ApiException e) {
      // Note: 'get' already has a handler for ApiExceptions.
      throw new IOException("Secret Manager update API call failed", e);
    }
  }

  @Override
  public Secret get(String namespace, String name) throws SecretNotFoundException, IOException {
    return new Secret(getData(namespace, name), getMetadata(namespace, name));
  }

  @Override
  public byte[] getData(String namespace, String name) throws SecretNotFoundException, IOException {
    try {
      return client.getSecretData(namespace, name);
    } catch (ApiException e) {
      if (e.getStatusCode().getCode() == StatusCode.Code.NOT_FOUND) {
        throw new SecretNotFoundException(namespace, name);
      }
      throw new IOException("Secret Manager get API call failed", e);
    }
  }

  @Override
  public SecretMetadata getMetadata(String namespace, String name)
      throws SecretNotFoundException, IOException {
    return getWrappedSecret(namespace, name).getCdapSecretMetadata();
  }

  private WrappedSecret getWrappedSecret(String namespace, String name) throws SecretNotFoundException, IOException {
    try {
      return client.getSecret(namespace, name);
    } catch (ApiException e) {
      if (e.getStatusCode().getCode() == StatusCode.Code.NOT_FOUND) {
        throw new SecretNotFoundException(namespace, name);
      }
      throw new IOException("Secret Manager get API call failed", e);
    } catch (InvalidSecretException e) {
      throw new IOException("Failed to parse secret", e);
    }
  }

  @Override
  public Collection<SecretMetadata> list(String namespace) throws IOException {
    try {
      return client.listSecrets(namespace).stream()
        .map(WrappedSecret::getCdapSecretMetadata)
        .collect(Collectors.toList());
    } catch (ApiException e) {
      throw new IOException("Secret Manager list API call failed", e);
    }
  }

  @Override
  public void delete(String namespace, String name) throws SecretNotFoundException, IOException {
    try {
      client.deleteSecret(namespace, name);
    } catch (ApiException e) {
      throw new IOException("Secret Manager delete API call failed", e);
    }
  }

  @Override
  public void destroy(SecretManagerContext context) {
    client.destroy();
  }

  @Override
  public SecretManagerInfo getStoreInfo() {
    return SECRET_MANAGER_INFO;
  }

  @Override
  public boolean acquireLease(String namespace, String key, long timeoutMs, String leaseHolder) throws IOException {
    try {
      WrappedSecret refreshSecret = client.getSecret(namespace, key);
      String currentEtag = refreshSecret.getEtag() == null ? "" : refreshSecret.getEtag();

      String state = refreshSecret.getAnnotation(ANNOTATION_LEASE_STATE, STATE_IDLE);
      long leaseTimestamp = tryParseLong(refreshSecret.getAnnotation(ANNOTATION_LEASE_ACQUIRED_TIME_MS, "0"), 0L, key);

      String currentLeaseHolder = refreshSecret.getAnnotation(ANNOTATION_LEASE_HOLDER, "");
      long now = System.currentTimeMillis();
      boolean isExpired = (now - leaseTimestamp) > timeoutMs;

      if (STATE_REFRESHING.equalsIgnoreCase(state) && !isExpired && !leaseHolder.equals(currentLeaseHolder)) {
        return false;
      }

      Map<String, String> annotationsToUpdate = putLeaseAnnotations(
          refreshSecret.getAnnotations(), STATE_REFRESHING, String.valueOf(now), leaseHolder);
      client.updateSecretAnnotations(namespace, key, annotationsToUpdate, currentEtag);
      return true;

    } catch (ApiException e) {
      if (e.getStatusCode().getCode() == StatusCode.Code.FAILED_PRECONDITION) {
        LOG.debug("Lease acquire failure (ETag mismatch) for secret {} in namespace {}", key, namespace);
        return false;
      }
      throw new IOException("Failed to acquire lease on secret " + key, e);
    } catch (InvalidSecretException e) {
      throw new IOException("Failed to parse secret", e);
    }
  }

  @Override
  public boolean releaseLease(String namespace, String key, String leaseHolder) throws IOException {
    if (leaseHolder == null || leaseHolder.isEmpty()) {
      return false;
    }
    try {
      WrappedSecret refreshSecret = client.getSecret(namespace, key);
      String currentEtag = refreshSecret.getEtag() == null ? "" : refreshSecret.getEtag();
      Map<String, String> currentAnnotations = refreshSecret.getAnnotations();
      String currentLeaseHolder = currentAnnotations.get(ANNOTATION_LEASE_HOLDER);

      if (STATE_IDLE.equalsIgnoreCase(currentAnnotations.get(ANNOTATION_LEASE_STATE))
          || currentLeaseHolder == null || currentLeaseHolder.isEmpty()) {
        return false;
      }

      if (!leaseHolder.equals(currentLeaseHolder)) {
        return false;
      }

      Map<String, String> annotationsToUpdate = putLeaseAnnotations(
          currentAnnotations, STATE_IDLE, "0", "");
      client.updateSecretAnnotations(namespace, key, annotationsToUpdate, currentEtag);
      return true;
    } catch (ApiException | InvalidSecretException e) {
      throw new IOException("Failed to release lease on secret " + key, e);
    }
  }

  private long tryParseLong(String value, long defaultValue, String key) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      LOG.debug("Invalid lease timestamp found for secret {}, treating as expired.", key);
      return defaultValue;
    }
  }

  private Map<String, String> putLeaseAnnotations(Map<String, String> currentAnnotations,
                                               String state, String leaseTimestamp, String leaseHolder) {
    Map<String, String> updatedAnnotations = new HashMap<>(currentAnnotations);
    updatedAnnotations.put(ANNOTATION_LEASE_STATE, state);
    updatedAnnotations.put(ANNOTATION_LEASE_ACQUIRED_TIME_MS, leaseTimestamp);
    updatedAnnotations.put(ANNOTATION_LEASE_HOLDER, leaseHolder);
    return updatedAnnotations;
  }
}
