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

package co.cask.cdap.internal.app.namespace;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.store.NamespaceStore;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.util.List;

/**
 * Default implementation of {@link NamespaceQueryAdmin} to query namespace details.
 */
public class DefaultNamespaceQueryAdmin implements NamespaceQueryAdmin {

  protected final NamespaceStore nsStore;
  protected final AuthorizationEnforcer authorizationEnforcer;
  protected final AuthenticationContext authenticationContext;

  @Inject
  public DefaultNamespaceQueryAdmin(NamespaceStore nsStore,
                                    AuthorizationEnforcer authorizationEnforcer,
                                    AuthenticationContext authenticationContext) {
    this.nsStore = nsStore;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
  }

  /**
   * Lists all namespaces
   *
   * @return a list of {@link NamespaceMeta} for all namespaces
   */
  @Override
  public List<NamespaceMeta> list() throws Exception {
    List<NamespaceMeta> namespaces = nsStore.list();
    Principal principal = authenticationContext.getPrincipal();
    final Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    return Lists.newArrayList(
      Iterables.filter(namespaces, new com.google.common.base.Predicate<NamespaceMeta>() {
        @Override
        public boolean apply(NamespaceMeta namespaceMeta) {
          return filter.apply(new NamespaceId(namespaceMeta.getName()));
        }
      })
    );
  }

  /**
   * Gets details of a namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the requested namespace
   * @return the {@link NamespaceMeta} of the requested namespace
   * @throws NamespaceNotFoundException if the requested namespace is not found
   * @throws UnauthorizedException if the namespace is not authorized to the logged-user
   */
  @Override
  public NamespaceMeta get(Id.Namespace namespaceId) throws Exception {
    NamespaceMeta ns = nsStore.get(namespaceId);
    if (ns == null) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    Principal principal = authenticationContext.getPrincipal();
    Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    if (!Principal.SYSTEM.equals(principal) && !filter.apply(namespaceId.toEntityId())) {
      throw new UnauthorizedException(principal, namespaceId.toEntityId());
    }
    return ns;
  }

  /**
   * Checks if the specified namespace exists
   *
   * @param namespaceId the {@link Id.Namespace} to check for existence
   * @return true, if the specifed namespace exists, false otherwise
   */
  @Override
  public boolean exists(Id.Namespace namespaceId) throws Exception {
    try {
      get(namespaceId);
    } catch (NotFoundException e) {
      return false;
    }
    return true;
  }
}
