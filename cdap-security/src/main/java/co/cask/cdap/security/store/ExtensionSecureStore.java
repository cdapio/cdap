/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.cdap.security.store;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.securestore.spi.SecretAlreadyExistsException;
import co.cask.cdap.securestore.spi.SecretManager;
import co.cask.cdap.securestore.spi.SecretNotFoundException;
import co.cask.cdap.securestore.spi.secret.Secret;
import co.cask.cdap.securestore.spi.secret.SecretMetadata;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Secure Store which reads and writes to extension secure store such as google cloud kms.
 *
 * TODO Make the class thread safe
 * TODO Add tests
 * TODO Failure cases
 */
@Singleton
public class ExtensionSecureStore implements SecureStore, SecureStoreManager, SecureStoreLifeCycle {
  private static final Logger LOG = LoggerFactory.getLogger(ExtensionSecureStore.class);
  private final String dir;
  private final String type;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private DefaultSecretManagercontext context;
  private SecretManager secretManager;

  @Inject
  public ExtensionSecureStore(CConfiguration cConf, NamespaceQueryAdmin namespaceQueryAdmin) {
    this.dir = cConf.get(Constants.Security.Store.EXTENSIONS_DIR);
    this.type = cConf.get(cConf.get(Constants.Security.Store.PROVIDER));
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  @Override
  public void initialize() throws Exception {
    SecureStoreExtensionLoader secureStoreExtensionLoader = new SecureStoreExtensionLoader(dir);
    // Depending on type of the secret manager, there will only be one secret manager
    this.secretManager = secureStoreExtensionLoader.get(type);
    if (this.secretManager == null) {
      throw new RuntimeException(String.format("Secure store extension %s can not be loaded. Make sure the name of " +
                                                 "the implementation matches %s property", type,
                                               Constants.Security.Store.PROVIDER));
    }
    this.context = new DefaultSecretManagercontext();
    this.secretManager.initialize(context);
  }

  @Override
  public void destroy() {
    try {
      this.secretManager.destroy(context);
    } catch (Exception e) {
      // do not throw any exception in destroy method
      LOG.warn("Error occurred while destroying secure store", e);
    }
  }

  /**
   *
   * @param namespace The namespace that this key belongs to.
   * @return
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws Exception
   */
  @Override
  public Map<String, String> listSecureData(String namespace) throws Exception {
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
   *
   * @param namespace The namespace that this key belongs to.
   * @param name Name of the data element.
   * @return
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws NotFoundException If the key is not found in the store.
   * @throws Exception
   */
  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws Exception {
    checkNamespaceExists(namespace);
    try {
      Secret secret = secretManager.get(namespace, name);
      return new SecureStoreData(new SecureStoreMetadata(secret.getMetadata().getName(),
                                                         secret.getMetadata().getDescription(),
                                                         secret.getMetadata().getCreationTimeMs(),
                                                         secret.getMetadata().getProperties()),
                                 secret.getData());
    } catch (SecretNotFoundException e) {
      throw new NotFoundException(String.format("Key name %s not found in the secure store.", name));
    }
  }

  /**
   *
   * @param namespace The namespace that this key belongs to.
   * @param name This is the identifier that will be used to retrieve this element.
   * @param data The sensitive data that has to be securely stored
   * @param description User provided description of the entry.
   * @param properties associated with this element.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws AlreadyExistsException If the key already exists in the namespace. Updating is not supported.
   * @throws IOException If there was a problem storing the key to the in memory keystore
   */
  @Override
  public void putSecureData(String namespace, String name, String data, String description,
                            Map<String, String> properties) throws Exception {
    checkNamespaceExists(namespace);
    try {
      secretManager.store(namespace, name, data.getBytes(), description, properties);
    } catch (SecretAlreadyExistsException e) {
      throw new AlreadyExistsException(new SecureKeyId(namespace, name));
    } catch (Exception e) {
      throw new IOException(String.format("Failed to store key %s under namespace %s", name,namespace), e);
    }
  }

  /**
   *
   * @param namespace The namespace that this key belongs to.
   * @param name of the element to delete.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws NotFoundException If the key is not found in the store.
   * @throws Exception
   */
  @Override
  public void deleteSecureData(String namespace, String name) throws Exception {
    checkNamespaceExists(namespace);
    try {
      secretManager.delete(namespace, name);
    } catch (SecretNotFoundException e) {
      throw new NotFoundException(String.format("Key name %s not found in the secure store.", name));
    }
  }

  private void checkNamespaceExists(String namespace) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }
  }
}
