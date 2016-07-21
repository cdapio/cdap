/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.SecureKeyCreateRequest;
import co.cask.cdap.proto.security.SecureKeyListEntry;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import com.google.common.base.Strings;
import com.google.inject.Inject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Service that manages access to the Secure Store,
 */
public class DefaultSecureStoreService implements SecureStoreService {
  private final AuthorizerInstantiator authorizerInstantiator;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;

  @Inject
  DefaultSecureStoreService(AuthorizerInstantiator authorizerInstantiator, SecureStore secureStore,
                            SecureStoreManager secureStoreManager) {
    this.authorizerInstantiator = authorizerInstantiator;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
  }

  @Override
  public List<SecureKeyListEntry> list(NamespaceId namespaceId) throws Exception {
    Principal principal = SecurityRequestContext.toPrincipal();
    authorizerInstantiator.get().enforce(namespaceId, principal, Action.READ);
    List<SecureStoreMetadata> metadatas = secureStore.list(namespaceId.getNamespace());
    List<SecureKeyListEntry> returnList = new ArrayList<>(metadatas.size());
    for (SecureStoreMetadata metadata : metadatas) {
      returnList.add(new SecureKeyListEntry(metadata.getName(), metadata.getDescription()));
    }
    return returnList;
  }

  @Override
  public SecureStoreData get(SecureKeyId secureKeyId) throws Exception {
    Principal principal = SecurityRequestContext.toPrincipal();
    authorizerInstantiator.get().enforce(secureKeyId, principal, Action.READ);
    return secureStore.get(secureKeyId.getNamespace(), secureKeyId.getName());
  }

  @Override
  public synchronized void put(SecureKeyCreateRequest secureKeyCreateRequest, SecureKeyId secureKeyId)
    throws Exception {
    Principal principal = SecurityRequestContext.toPrincipal();
    authorizerInstantiator.get().enforce(secureKeyId, principal, Action.WRITE);
    String description = secureKeyCreateRequest.getDescription();
    String value = secureKeyCreateRequest.getData();
    if (Strings.isNullOrEmpty(value)) {
      throw new BadRequestException("The data field should not be empty. This is the data that will be stored " +
                                      "securely.");
    }

    byte[] data = value.getBytes(StandardCharsets.UTF_8);
    secureStoreManager.put(secureKeyId.getNamespace(), secureKeyId.getName(), data, description,
                           secureKeyCreateRequest.getProperties());
  }

  @Override
  public void delete(SecureKeyId secureKeyId) throws Exception {
    Principal principal = SecurityRequestContext.toPrincipal();
    authorizerInstantiator.get().enforce(secureKeyId, principal, Action.ADMIN);
    secureStoreManager.delete(secureKeyId.getNamespace(), secureKeyId.getName());
  }
}
