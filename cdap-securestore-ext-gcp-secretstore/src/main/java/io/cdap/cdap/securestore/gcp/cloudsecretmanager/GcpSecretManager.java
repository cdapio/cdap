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
import io.cdap.cdap.securestore.spi.SecretLease;
import io.cdap.cdap.securestore.spi.SecretManager;
import io.cdap.cdap.securestore.spi.SecretManagerContext;
import io.cdap.cdap.securestore.spi.SecretNotFoundException;
import io.cdap.cdap.securestore.spi.secret.Secret;
import io.cdap.cdap.securestore.spi.secret.SecretMetadata;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

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
  private static final String PROVIDER_NAME = "gcp-secretmanager";

  private CloudSecretManagerClient client;

  private static final String ANNOTATION_STATE = "state";
  private static final String ANNOTATION_LOCK_TIMESTAMP = "lock_timestamp";
  private static final String ANNOTATION_LOCK_HOLDER = "lock_holder";

  private static final String STATE_IDLE = "idle";
  private static final String STATE_REFRESHING = "refreshing";

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
    WrappedSecret wrappedSecret = WrappedSecret.fromMetadata(namespace, secret.getMetadata());

    try {
      Secret existingSecret = get(namespace, secret.getMetadata().getName());
      // 'update' only if 'get' request succeeds, otherwise 'create'.
      client.updateSecret(wrappedSecret);

      // Add a new secret version only if the secret payload has changed.
      if (!Arrays.equals(existingSecret.getData(), secret.getData())) {
        client.addSecretVersion(wrappedSecret, secret.getData());
      }
    } catch (SecretNotFoundException unused) {
      try {
        client.createSecret(wrappedSecret);
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
    try {
      return client.getSecret(namespace, name).getCdapSecretMetadata();
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
  public boolean isLeaseSupported() {
    return true;
  }

  @Override
  public SecretLease acquireLease(String namespace, String key, long timeoutMs, String lockHolder) throws IOException {
    long now = System.currentTimeMillis();

    try {
      WrappedSecret refreshSecret = client.getSecret(namespace, key);
      String currentEtag = refreshSecret.getEtag() == null ? "" : refreshSecret.getEtag();

      String state = refreshSecret.getAnnotation(ANNOTATION_STATE, STATE_IDLE);
      String lockTimestampStr = refreshSecret.getAnnotation(ANNOTATION_LOCK_TIMESTAMP, "0");
      long lockTimestamp = 0L;
      try {
        lockTimestamp = Long.parseLong(lockTimestampStr);
      } catch (NumberFormatException e) {
        lockTimestamp = 0L;
      }

      boolean isExpired = (now - lockTimestamp) > timeoutMs;
      boolean isLockedByAnother = STATE_REFRESHING.equalsIgnoreCase(state) && !isExpired;

      if (isLockedByAnother) {
        return SecretLease.failed();
      }

      Map<String, String> annotationsToUpdate = new HashMap<>(refreshSecret.getAnnotations());
      annotationsToUpdate.put(ANNOTATION_STATE, STATE_REFRESHING);
      annotationsToUpdate.put(ANNOTATION_LOCK_TIMESTAMP, String.valueOf(now));
      annotationsToUpdate.put(ANNOTATION_LOCK_HOLDER, lockHolder);

      boolean acquired = client.updateSecretWithEtag(namespace, key, annotationsToUpdate, currentEtag);

      if (acquired) {
        return SecretLease.acquired(String.valueOf(now), lockHolder);
      } else {
        return SecretLease.failed();
      }

    } catch (ApiException e) {
      if (e.getStatusCode().getCode() == StatusCode.Code.NOT_FOUND) {
        throw new IOException("Refresh Token Secret '" + key + "' not found in namespace '" + namespace + "'", e);
      }
      throw new IOException("Failed to acquire lease lock on Refresh Token Secret " + key, e);
    } catch (InvalidSecretException e) {
      throw new IOException("Failed to parse Refresh Token Secret metadata for " + key, e);
    } catch (Exception e) {
      throw new IOException("Unexpected error acquiring lease on Refresh Token Secret " + key, e);
    }
  }

  @Override
  public void releaseLease(String namespace, String key, SecretLease lease) throws IOException {
    if (lease == null || !lease.isAcquired()) {
      return;
    }
    try {
      WrappedSecret refreshSecret = client.getSecret(namespace, key);
      String currentEtag = refreshSecret.getEtag() == null ? "" : refreshSecret.getEtag();

      Map<String, String> currentAnnotations = refreshSecret.getAnnotations();
      if (!lease.getLockHolder().equals(currentAnnotations.get(ANNOTATION_LOCK_HOLDER))) {
        throw new IOException(String.format(
          "Cannot release lease for %s: lock is currently held by a different owner (%s).",
          key, currentAnnotations.get(ANNOTATION_LOCK_HOLDER)));
      }

      Map<String, String> annotationsToUpdate = new HashMap<>(currentAnnotations);
      annotationsToUpdate.put(ANNOTATION_STATE, STATE_IDLE);
      annotationsToUpdate.put(ANNOTATION_LOCK_TIMESTAMP, "0");
      annotationsToUpdate.put(ANNOTATION_LOCK_HOLDER, "");

      client.updateSecretWithEtag(namespace, key, annotationsToUpdate, currentEtag);
    } catch (Exception e) {
      // Ignore release failures
    }
  }
}
