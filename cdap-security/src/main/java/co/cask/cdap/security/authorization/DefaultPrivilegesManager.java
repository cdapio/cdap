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

package co.cask.cdap.security.authorization;

import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.common.base.Predicate;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * A {@link PrivilegesManager} that also invalidates privileges caches when privileges are updated.
 */
public class DefaultPrivilegesManager implements PrivilegesManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultPrivilegesManager.class);

  private final Authorizer delegateAuthorizer;
  private final AuthorizationEnforcementService authorizationEnforcementService;
  private final PrivilegesFetcherProxyService privilegesFetcherProxyService;

  @Inject
  DefaultPrivilegesManager(AuthorizerInstantiator authorizerInstantiator,
                           AuthorizationEnforcementService authorizationEnforcementService,
                           PrivilegesFetcherProxyService privilegesFetcherProxyService) {
    this.privilegesFetcherProxyService = privilegesFetcherProxyService;
    this.delegateAuthorizer = authorizerInstantiator.get();
    this.authorizationEnforcementService = authorizationEnforcementService;
  }

  @Override
  public void grant(EntityId entity, final Principal principal, Set<Action> actions) throws Exception {
    delegateAuthorizer.grant(entity, principal, actions);
    Predicate<Principal> invalidationPredicate = createInvalidationPredicate(principal);
    authorizationEnforcementService.invalidate(invalidationPredicate);
    privilegesFetcherProxyService.invalidate(invalidationPredicate);

  }

  @Override
  public void revoke(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    delegateAuthorizer.revoke(entity, principal, actions);
    Predicate<Principal> invalidationPredicate = createInvalidationPredicate(principal);
    authorizationEnforcementService.invalidate(invalidationPredicate);
    privilegesFetcherProxyService.invalidate(invalidationPredicate);
  }

  @Override
  public void revoke(EntityId entity) throws Exception {
    delegateAuthorizer.revoke(entity);
    // no need to invalidate. This is called only when the entity is deleted. As a result, even if cache is not
    // invalidated, it should be ok because the entity will not exist.
  }

  private Predicate<Principal> createInvalidationPredicate(final Principal principal) {
    return new Predicate<Principal>() {
      @Override
      public boolean apply(Principal input) {
        if (Principal.PrincipalType.ROLE != principal.getType()) {
          return input.equals(principal);
        }
        Role role = new Role(principal.getName());
        try {
          return delegateAuthorizer.listRoles(input).contains(role);
        } catch (Exception e) {
          LOG.warn("Error while listing roles of principal {}. Cannot invalidate cache for role {}. Operations " +
                     "may fail until the next cache refresh.", input, role, e);
          return false;
        }
      }
    };
  }
}
