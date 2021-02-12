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

package io.cdap.cdap.security.store.secretmanager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.SecureKeyNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.SecureKeyId;
import io.cdap.cdap.securestore.spi.SecretManager;
import io.cdap.cdap.securestore.spi.SecretManagerContext;
import io.cdap.cdap.securestore.spi.SecretNotFoundException;
import io.cdap.cdap.securestore.spi.SecretStore;
import io.cdap.cdap.securestore.spi.secret.Secret;
import io.cdap.cdap.securestore.spi.secret.SecretMetadata;
import io.cdap.cdap.security.store.SecureStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Secret manager based secure store service to call {@link SecretManager} methods.
 */
public class SecretManagerSecureStoreService extends AbstractIdleService implements SecureStoreService {
  private static final Logger LOG = LoggerFactory.getLogger(SecretManagerSecureStoreService.class);

  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final SecretManagerContext context;
  private final String type;
  private SecretManager secretManager;

  @Inject
  SecretManagerSecureStoreService(CConfiguration cConf, NamespaceQueryAdmin namespaceQueryAdmin, SecretStore store) {
    this(namespaceQueryAdmin,
         new DefaultSecretManagerContext(cConf, store),
         cConf.get(Constants.Security.Store.PROVIDER),
         new SecretManagerExtensionLoader(cConf.get(Constants.Security.Store.EXTENSIONS_DIR))
           .get(cConf.get(Constants.Security.Store.PROVIDER)));
  }

  @VisibleForTesting
  SecretManagerSecureStoreService(NamespaceQueryAdmin namespaceQueryAdmin, SecretManagerContext context,
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

  @Override
  public List<SecureStoreMetadata> list(String namespace) throws Exception {
    validate(namespace);
    List<SecureStoreMetadata> metadataList = new ArrayList<>();
    for (SecretMetadata metadata : secretManager.list(namespace)) {
      metadataList.add(new SecureStoreMetadata(metadata.getName(), metadata.getDescription(),
                                               metadata.getCreationTimeMs(), metadata.getProperties()));
    }
    return metadataList;
  }

  @Override
  public SecureStoreData get(String namespace, String name) throws Exception {
    validate(namespace);
    try {
      Secret secret = secretManager.get(namespace, name);
      SecretMetadata metadata = secret.getMetadata();
      return new SecureStoreData(new SecureStoreMetadata(metadata.getName(), metadata.getDescription(),
                                                         metadata.getCreationTimeMs(), metadata.getProperties()),
                                 secret.getData());
    } catch (SecretNotFoundException e) {
      throw new SecureKeyNotFoundException(new SecureKeyId(namespace, name), e);
    }
  }

  @Override
  public void put(String namespace, String name, String data, @Nullable String description,
                  Map<String, String> properties) throws Exception {
    validate(namespace);
    secretManager.store(namespace, new Secret(data.getBytes(StandardCharsets.UTF_8),
                                              new SecretMetadata(name, description, System.currentTimeMillis(),
                                                                 ImmutableMap.copyOf(properties))));
  }

  @Override
  public void delete(String namespace, String name) throws Exception {
    validate(namespace);
    try {
      secretManager.delete(namespace, name);
    } catch (SecretNotFoundException e) {
      throw new SecureKeyNotFoundException(new SecureKeyId(namespace, name), e);
    }
  }

  /**
   * Validates if secret manager is loaded and the provided namespace exists.
   */
  private void validate(String namespace) throws Exception {
    if (secretManager == null) {
      throw new RuntimeException("Secret manager is either not initialized or not loaded. ");
    }
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!NamespaceId.SYSTEM.equals(namespaceId) && !namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
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
      this.secretManager = null;
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
