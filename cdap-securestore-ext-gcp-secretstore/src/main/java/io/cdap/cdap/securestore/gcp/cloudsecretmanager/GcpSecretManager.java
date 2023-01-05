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
import io.cdap.cdap.securestore.spi.SecretNotFoundException;
import io.cdap.cdap.securestore.spi.secret.Secret;
import io.cdap.cdap.securestore.spi.secret.SecretMetadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
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
    try {
      WrappedSecret wrappedSecret = client.getSecret(namespace, name);
      byte[] secretData = client.getSecretData(wrappedSecret);
      return new Secret(secretData, wrappedSecret.getCdapSecretMetadata());
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
}
