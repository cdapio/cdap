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

package co.cask.cdap.security.store.secretmanager;

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
import co.cask.cdap.securestore.spi.SecretManagerContext;
import co.cask.cdap.securestore.spi.SecretNotFoundException;
import co.cask.cdap.securestore.spi.secret.Secret;
import co.cask.cdap.securestore.spi.secret.SecretMetadata;
import co.cask.cdap.security.store.SecureStoreService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Secret manager secure store service to initialize {@link SecretManager}.
 */
public class SecretManagerSecureStoreService extends AbstractIdleService implements SecureStoreService {
  private static final Logger LOG = LoggerFactory.getLogger(SecretManagerSecureStoreService.class);

  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final SecretManagerContext context;
  private final String type;
  private SecretManager secretManager;

  @Inject
  public SecretManagerSecureStoreService(CConfiguration cConf, NamespaceQueryAdmin namespaceQueryAdmin) {
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.context = new DefaultSecretManagerContext(cConf);
    this.type = cConf.get(Constants.Security.Store.PROVIDER);
    this.secretManager = new SecretManagerExtensionLoader(cConf.get(Constants.Security.Store.EXTENSIONS_DIR)).get(type);
  }

  @VisibleForTesting
  public SecretManagerSecureStoreService(NamespaceQueryAdmin namespaceQueryAdmin, SecretManagerContext context,
                                         String type, SecretManager secretManager) {
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.context = context;
    this.type = type;
    this.secretManager = secretManager;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}.", getClass().getSimpleName());
    initializeSecretManager();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}.", getClass().getSimpleName());
    destroySecretManager();
  }

  /**
   * List of all the secrets in the secret manager belonging to the specified namespace.
   * @return A map of secret name to its description
   * @param namespace the namespace for list of secrets
   * @throws NamespaceNotFoundException if the specified namespace does not exist
   * @throws IOException if there was a problem while reading from the secret manager
   */
  @Override
  public Map<String, String> listSecureData(String namespace) throws Exception {
    validate(namespace);
    Map<String, String> map = new HashMap<>();
    for (SecretMetadata metadata : secretManager.list(namespace)) {
      map.put(metadata.getName(), metadata.getDescription());
    }

    return map;
  }

  /**
   * Returns the data for the secret identified by the given name.
   * @param name name of the secret
   * @return an secure store data identified by the given name
   * @throws SecureKeyNotFoundException if the secret was not found in the store
   * @throws IOException if there was a problem in getting the secret from the secret manager
   */
  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws Exception {
    validate(namespace);
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
   * @param namespace the namespace this secret belongs to
   * @param name name of the secret
   * @param data the data that needs to be securely stored
   * @param description user provided description of the secret
   * @param properties properties associated with the secret
   * @throws NamespaceNotFoundException if the specified namespace does not exist
   * @throws IOException if there was a problem storing the secret to the secret manager
   */
  @Override
  public void putSecureData(String namespace, String name, String data, String description,
                            Map<String, String> properties) throws Exception {
    validate(namespace);
    secretManager.store(namespace, new Secret(data.getBytes(StandardCharsets.UTF_8),
                                              new SecretMetadata(name, description, System.currentTimeMillis(),
                                                                 ImmutableMap.copyOf(properties))));
  }

  /**
   * Deletes the secret with the given name.
   * @param namespace the namespace this secret belongs to
   * @param name name of the secret to be deleted
   * @throws NamespaceNotFoundException if the specified namespace does not exist
   * @throws SecureKeyNotFoundException if the secret to be deleted is not found
   * @throws IOException if there was a problem during deleting the secret from the secret manager
   */
  @Override
  public void deleteSecureData(String namespace, String name) throws Exception {
    validate(namespace);
    try {
      secretManager.delete(namespace, name);
    } catch (SecretNotFoundException e) {
      throw new SecureKeyNotFoundException(new SecureKeyId(namespace, name));
    }
  }

  /**
   * Validates if namespace exists and secret mananger is loaded.
   */
  private void validate(String namespace) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }

    if (secretManager == null) {
      throw new RuntimeException("Secret manager is either not initialized or not loaded. ");
    }
  }

  private void initializeSecretManager() {
    if (this.secretManager == null) {
      LOG.error(String.format("Secure store extension %s was not loaded. Make sure the name of the " +
                                "implementation matches %s property.", type, Constants.Security.Store.PROVIDER));
      return;
    }

    try {
      this.secretManager.initialize(context);
      LOG.info("Initialized secure store of type {}.", type);
    } catch (IOException e) {
      LOG.error(String.format("Error occurred while initializing secure store of type %s.", type), e);
    }
  }

  private void destroySecretManager() {
    try {
      if (this.secretManager != null) {
        this.secretManager.destroy(context);
      }
    } catch (Throwable e) {
      LOG.warn("Error occurred while stopping {}.", getClass().getSimpleName(), e);
    }
  }
}
