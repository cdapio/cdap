/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.security.store.extension;

import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.SecureKeyNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.securestore.spi.SecretManager;
import co.cask.cdap.securestore.spi.SecretNotFoundException;
import co.cask.cdap.securestore.spi.secret.Secret;
import co.cask.cdap.securestore.spi.secret.SecretMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO Make it thread safe?
 * TODO Add unit tests for this service.
 *
 * Secure store service to initialize {@link SecretManager}.
 */
@Singleton
public class DefaultSecureStoreExtensionService extends AbstractIdleService implements SecureStoreService {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultSecureStoreExtensionService.class);

  private final String type;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final DefaultSecretManagerContext context;
  private final SecretManagerProvider secretManagerProvider;
  private SecretManager secretManager;

  @VisibleForTesting
  @Inject
  public DefaultSecureStoreExtensionService(CConfiguration cConf, NamespaceQueryAdmin namespaceQueryAdmin,
                                            SecretManagerProvider secretManagerProvider) {
    this.type = cConf.get(Constants.Security.Store.PROVIDER);
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.secretManagerProvider = secretManagerProvider;
    this.context = new DefaultSecretManagerContext();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}.", getClass().getSimpleName());

    // Depending on type of the secret manager, there will only be one secret manager
    this.secretManager = secretManagerProvider.loadSecretManager(type);
    if (this.secretManager == null) {
      LOG.error(String.format("Secure store extension %s can not be loaded. Make sure the name of " +
                                "the implementation matches %s property.", type,
                              Constants.Security.Store.PROVIDER));
      return;
    }

    try {
      this.secretManager.initialize(context);
      LOG.info("Initialized secure store of type {}", type);
    } catch (IOException e) {
      LOG.error(String.format("Error occurred while initializing secure store of type %s.", type), e);
      this.secretManager = null;
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}.", getClass().getSimpleName());

    try {
      if (this.secretManager != null) {
        this.secretManager.destroy(context);
      }
    } catch (Throwable e) {
      LOG.trace("Error occurred while stopping {}.", getClass().getSimpleName(), e);
    }
  }

  /**
   * List of all the entries in the secret manager belonging to the specified namespace.
   * @return A Map of objects representing the data and associated description
   * @param namespace The namespace this key belongs to
   * @throws NamespaceNotFoundException If the specified namespace does not exist
   * @throws IOException If there was a problem reading from the secret manager
   */
  @Override
  public Map<String, String> listSecureData(String namespace) throws Exception {
    if (secretManager == null) {
      throw new RuntimeException("Secret manager is either not initialized or not loaded. ");
    }
    checkNamespaceExists(namespace);
    Map<String, String> map = new HashMap<>();
    try {
      for (SecretMetadata metadata : secretManager.list(namespace)) {
        map.put(metadata.getName(), metadata.getDescription());
      }

      return map;
    } catch (Exception e) {
      throw new IOException("Failed to get the list of keys from the secure store.", e);
    }
  }

  /**
   * Returns the metadata for the secret identified by the given name.
   * @param name Name of the secret
   * @return An object representing the metadata associated with the secret
   * @throws SecureKeyNotFoundException If the key was not found in the store
   * @throws IOException If there was a problem in getting the key from the secret manager
   */
  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws Exception {
    if (secretManager == null) {
      throw new RuntimeException("Secret manager is either not initialized or not loaded. ");
    }
    checkNamespaceExists(namespace);
    try {
      Secret secret = secretManager.get(namespace, name);
      SecretMetadata metadata = secret.getMetadata();
      return new SecureStoreData(new SecureStoreMetadata(metadata.getName(), metadata.getDescription(),
                                                         metadata.getCreationTimeMs(), metadata.getProperties()),
                                 secret.getData());
    } catch (SecretNotFoundException e) {
      throw new SecureKeyNotFoundException(new SecureKeyId(namespace, name));
    }
  }

  /**
   * Stores a secret in the secure store.
   * @param namespace The namespace this key belongs to
   * @param name Name of the secret
   * @param data The data that needs to be securely stored
   * @param description User provided description of the entry
   * @param properties properties associated with the data
   * @throws NamespaceNotFoundException If the specified namespace does not exist
   * @throws IOException If there was a problem storing the key to the secret manager
   */
  @Override
  public void putSecureData(String namespace, String name, String data, String description,
                            Map<String, String> properties) throws Exception {
    if (secretManager == null) {
      throw new RuntimeException("Secret manager is either not initialized or not loaded. ");
    }
    checkNamespaceExists(namespace);
    try {
      secretManager.store(namespace, new Secret(data.getBytes(StandardCharsets.UTF_8),
                                                new SecretMetadata(name, description, System.currentTimeMillis(),
                                                                   ImmutableMap.copyOf(properties))));
    } catch (Exception e) {
      throw new IOException(String.format("Failed to store key %s under namespace %s.", name, namespace), e);
    }
  }

  /**
   * Deletes the secret with the given name.
   * @param namespace The namespace this key belongs to
   * @param name Name of the secret to be deleted
   * @throws NamespaceNotFoundException If the specified namespace does not exist
   * @throws SecureKeyNotFoundException If the key to be deleted is not found
   * @throws IOException If their was a problem during deleting the key from the secret manager
   */
  @Override
  public void deleteSecureData(String namespace, String name) throws Exception {
    if (secretManager == null) {
      throw new RuntimeException("Secret manager is either not initialized or not loaded. ");
    }
    checkNamespaceExists(namespace);
    try {
      secretManager.delete(namespace, name);
    } catch (SecretNotFoundException e) {
      throw new SecureKeyNotFoundException(new SecureKeyId(namespace, name));
    }
  }

  private void checkNamespaceExists(String namespace) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }
  }
}
