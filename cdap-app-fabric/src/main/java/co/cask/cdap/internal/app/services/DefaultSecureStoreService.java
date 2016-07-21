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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation of the service that manages access to the Secure Store,
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

  /**
   * Lists all the secure keys in the given namespace that the user has access to.
   * @param namespaceId Id of the namespace we want the key list for.
   * @return A list of {@link SecureKeyListEntry} of all the keys visible to the user under the given namespace.
   * @throws Exception If either the caller does not have READ access to the namespace or if there was a problem
   * getting the list from the underlying provider.
   */
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

  /**
   * Checks if the user has access to read the secure key and returns the data associated with the key if they do.
   * @param secureKeyId Id of the key that the user is trying to read.
   * @return Data associated with the key if the user has read access.
   * @throws Exception If either the caller does not have READ access to the key or if there was a problem
   * getting the data from the underlying provider.
   */
  @Override
  public SecureStoreData get(SecureKeyId secureKeyId) throws Exception {
    Principal principal = SecurityRequestContext.toPrincipal();
    authorizerInstantiator.get().enforce(secureKeyId, principal, Action.READ);
    return secureStore.get(secureKeyId.getNamespace(), secureKeyId.getName());
  }

  /**
   * Puts the user provided data in the secure store, if the user has write access to the namespace.
   * @param secureKeyCreateRequest The request containing the data to be stored in the secure store.
   * @param secureKeyId The Id for the key that needs to be stored.
   * @throws Exception If either the caller does not have WRITE access to the namespace
   * or if there was a problem storing the data to the underlying provider
   * or if the request did not contain the value to be stored.
   */
  @Override
  public synchronized void put(SecureKeyCreateRequest secureKeyCreateRequest, SecureKeyId secureKeyId)
    throws Exception {
    Principal principal = SecurityRequestContext.toPrincipal();
    NamespaceId namespaceId = new NamespaceId(secureKeyId.getNamespace());
    authorizerInstantiator.get().enforce(namespaceId, principal, Action.WRITE);
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

  /**
   * Deletes the key if the user has ADMIN privileges to the key.
   * @param secureKeyId Id of the key to be deleted.
   * @throws Exception If either the caller does not have ADMIN access to the key or if there was a problem
   * deleting it from the underlying provider.
   */
  @Override
  public void delete(SecureKeyId secureKeyId) throws Exception {
    Principal principal = SecurityRequestContext.toPrincipal();
    authorizerInstantiator.get().enforce(secureKeyId, principal, Action.ADMIN);
    secureStoreManager.delete(secureKeyId.getNamespace(), secureKeyId.getName());
  }
}
