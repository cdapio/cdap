/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.security.store;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.SecureKeyId;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of the service that manages access to the Secure Store,
 */
public class DefaultSecureStoreService extends AbstractIdleService implements SecureStoreService {
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final SecureStoreService secureStoreService;

  @Inject
  DefaultSecureStoreService(AccessEnforcer accessEnforcer,
                            AuthenticationContext authenticationContext,
                            @Named(SecureStoreServerModule.DELEGATE_SECURE_STORE_SERVICE)
                              SecureStoreService secureStoreService) {
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.secureStoreService = secureStoreService;
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
  public final List<SecureStoreMetadata> list(final String namespace) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    List<SecureStoreMetadata> metadatas = secureStoreService.list(namespace);
    return AuthorizationUtil.isVisible(metadatas, accessEnforcer, principal,
                                       input -> new SecureKeyId(namespace, input.getName()), null);
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
  public final SecureStoreData get(String namespace, String name) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    SecureKeyId secureKeyId = new SecureKeyId(namespace, name);
    accessEnforcer.enforce(secureKeyId, principal, StandardPermission.GET);
    return secureStoreService.get(namespace, name);
  }

  /**
   * Puts the user provided data in the secure store, if the user has admin access to the key.
   *
   * @throws UnauthorizedException If the user does not have write permissions on the namespace.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws IOException If there was a problem storing the key to underlying provider.
   */
  @Override
  public final synchronized void put(String namespace, String name, String value, @Nullable String description,
                                     Map<String, String> properties) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    NamespaceId namespaceId = new NamespaceId(namespace);
    SecureKeyId secureKeyId = namespaceId.secureKey(name);
    accessEnforcer.enforce(secureKeyId, principal, StandardPermission.UPDATE);
    secureStoreService.put(namespace, name, value, description, properties);
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
  public final void delete(String namespace, String name) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    SecureKeyId secureKeyId = new SecureKeyId(namespace, name);
    accessEnforcer.enforce(secureKeyId, principal, StandardPermission.DELETE);
    secureStoreService.delete(namespace, name);
  }

  @Override
  protected void startUp() throws Exception {
    secureStoreService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    secureStoreService.stopAndWait();
  }
}
