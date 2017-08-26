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

package co.cask.cdap.security.store;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of the service that manages access to the Secure Store,
 */
public class DefaultSecureStoreService implements SecureStore, SecureStoreManager {
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;

  @Inject
  DefaultSecureStoreService(AuthorizationEnforcer authorizationEnforcer,
                            AuthenticationContext authenticationContext,
                            @Named(SecureStoreModules.DELEGATE_SECURE_STORE) SecureStore secureStore,
                            @Named(SecureStoreModules.DELEGATE_SECURE_STORE_MANAGER)
                              SecureStoreManager secureStoreManager) {
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
  }

  /**
   * Lists all the secure keys in the given namespace that the user has access to.
   * Returns an empty list if the user does not have access to any of the keys in the namespace.
   *
   * @return A map of key names accessible by the user and their descriptions.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws IOException If there was a problem reading from the store.
   *
   */
  @Override
  public final Map<String, String> listSecureData(final String namespace) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    Map<String, String> metadatas = new HashMap<>(secureStore.listSecureData(namespace));
    metadatas.keySet().retainAll(AuthorizationUtil.isVisible(metadatas.keySet(), authorizationEnforcer, principal,
                                                             new Function<String, EntityId>() {
                                                               @Override
                                                               public EntityId apply(String input) {
                                                                 return new SecureKeyId(namespace, input);
                                                               }
                                                             }, null));
    return metadatas;
  }

  /**
   * Checks if the user has access to read the secure key and returns the {@link SecureStoreData} associated
   * with the key if they do.
   *
   * @return Data associated with the key if the user has read access.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws NotFoundException If the key is not found in the store.
   * @throws IOException If there was a problem reading from the store.
   * @throws UnauthorizedException If the user does not have READ permissions on the secure key.
   */
  @Override
  public final SecureStoreData getSecureData(String namespace, String name) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    SecureKeyId secureKeyId = new SecureKeyId(namespace, name);
    authorizationEnforcer.enforce(secureKeyId, principal, Action.READ);
    return secureStore.getSecureData(namespace, name);
  }

  /**
   * Puts the user provided data in the secure store, if the user has admin access to the key.
   *
   * @throws BadRequestException If the request does not contain the value to be stored.
   * @throws UnauthorizedException If the user does not have write permissions on the namespace.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws AlreadyExistsException If the key already exists in the namespace. Updating is not supported.
   * @throws IOException If there was a problem storing the key to underlying provider.
   */
  @Override
  public final synchronized void putSecureData(String namespace, String name, String value, String description,
                                               Map<String, String> properties) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    NamespaceId namespaceId = new NamespaceId(namespace);
    SecureKeyId secureKeyId = namespaceId.secureKey(name);
    authorizationEnforcer.enforce(secureKeyId, principal, Action.ADMIN);

    if (Strings.isNullOrEmpty(value)) {
      throw new BadRequestException("The data field should not be empty. This is the data that will be stored " +
                                      "securely.");
    }

    secureStoreManager.putSecureData(namespace, name, value, description, properties);
  }

  /**
   * Deletes the key if the user has ADMIN privileges to the key. 
   *
   * @throws UnauthorizedException If the user does not have admin privileges required to delete the secure key.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws NotFoundException If the key to be deleted is not found.
   * @throws IOException If there was a problem deleting it from the underlying provider.
   */
  @Override
  public final void deleteSecureData(String namespace, String name) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    SecureKeyId secureKeyId = new SecureKeyId(namespace, name);
    authorizationEnforcer.enforce(secureKeyId, principal, Action.ADMIN);
    secureStoreManager.deleteSecureData(namespace, name);
  }
}
