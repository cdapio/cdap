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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.SecureKeyCreateRequest;
import co.cask.cdap.proto.security.SecureKeyListEntry;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation of the service that manages access to the Secure Store,
 */
public class DefaultSecureStoreService implements SecureStoreService {
  private final Authorizer authorizer;
  private final AuthenticationContext authenticationContext;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;

  @Inject
  DefaultSecureStoreService(AuthorizerInstantiator authorizerInstantiator, AuthenticationContext authenticationContext,
                            SecureStore secureStore, SecureStoreManager secureStoreManager) {
    this.authorizer = authorizerInstantiator.get();
    this.authenticationContext = authenticationContext;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
  }

  /**
   * Lists all the secure keys in the given namespace that the user has access to. Returns an empty list if the user
   * does not have access to the namespace or any of the keys in the namespace.
   * @param namespaceId Id of the namespace we want the key list for.
   * @return A list of {@link SecureKeyListEntry} for all the keys visible to the user under the given namespace.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws IOException If there was a problem reading from the store.
   *
   */
  @Override
  public List<SecureKeyListEntry> list(NamespaceId namespaceId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    final Predicate<EntityId> filter;
    filter = authorizer.createFilter(principal);
    List<SecureStoreMetadata> metadatas = secureStore.listSecureData(namespaceId.getNamespace());
    List<SecureKeyListEntry> result = new ArrayList<>(metadatas.size());
    String namespace = namespaceId.getNamespace();
    for (SecureStoreMetadata metadata : metadatas) {
      String name = metadata.getName();
      if (filter.apply(new SecureKeyId(namespace, name))) {
        result.add(new SecureKeyListEntry(name, metadata.getDescription()));
      }
    }
    return result;
  }

  /**
   * Checks if the user has access to read the secure key and returns the {@link SecureStoreData} associated
   * with the key if they do.
   * @param secureKeyId Id of the key that the user is trying to read.
   * @return Data associated with the key if the user has read access.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws NotFoundException If the key is not found in the store.
   * @throws IOException If there was a problem reading from the store.
   * @throws UnauthorizedException If the user does not have READ permissions on the secure key.
   */
  @Override
  public SecureStoreData get(SecureKeyId secureKeyId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    final Predicate<EntityId> filter;
    filter = authorizer.createFilter(principal);

    if (filter.apply(secureKeyId)) {
      return secureStore.getSecureData(secureKeyId.getNamespace(), secureKeyId.getName());
    }
    throw new UnauthorizedException(principal, Action.READ, secureKeyId);
  }

  /**
   * Puts the user provided data in the secure store, if the user has write access to the namespace. Grants the user
   * all access to the newly created entity.
   * @param secureKeyId The Id for the key that needs to be stored.
   * @param secureKeyCreateRequest The request containing the data to be stored in the secure store.
   * @throws BadRequestException If the request does not contain the value to be stored.
   * @throws UnauthorizedException If the user does not have write permissions on the namespace.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws AlreadyExistsException If the key already exists in the namespace. Updating is not supported.
   * @throws IOException If there was a problem storing the key to underlying provider.
   */
  @Override
  public synchronized void put(SecureKeyId secureKeyId,
                               SecureKeyCreateRequest secureKeyCreateRequest) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    NamespaceId namespaceId = new NamespaceId(secureKeyId.getNamespace());
    authorizer.enforce(namespaceId, principal, Action.WRITE);

    String description = secureKeyCreateRequest.getDescription();
    String value = secureKeyCreateRequest.getData();
    if (Strings.isNullOrEmpty(value)) {
      throw new BadRequestException("The data field should not be empty. This is the data that will be stored " +
                                      "securely.");
    }

    secureStoreManager.putSecureData(secureKeyId.getNamespace(), secureKeyId.getName(), value, description,
                           secureKeyCreateRequest.getProperties());
    authorizer.grant(secureKeyId, principal, ImmutableSet.of(Action.ALL));
  }

  /**
   * Deletes the key if the user has ADMIN privileges to the key. Clears all the privileges associated with the key.
   * @param secureKeyId Id of the key to be deleted.
   * @throws UnauthorizedException If the user does not have admin privileges required to delete the secure key.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws NotFoundException If the key to be deleted is not found.
   * @throws IOException If there was a problem deleting it from the underlying provider.
   */
  @Override
  public void delete(SecureKeyId secureKeyId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    authorizer.enforce(secureKeyId, principal, Action.ADMIN);
    secureStoreManager.deleteSecureData(secureKeyId.getNamespace(), secureKeyId.getName());
    authorizer.revoke(secureKeyId);
  }
}
