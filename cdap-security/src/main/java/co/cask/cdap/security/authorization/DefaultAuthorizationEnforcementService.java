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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.ParentedId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.PrivilegesFetcher;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link AuthorizationEnforcementService}.
 */
@Singleton
public class DefaultAuthorizationEnforcementService extends AbstractAuthorizationService
  implements AuthorizationEnforcementService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAuthorizationEnforcementService.class);
  private static final Predicate<EntityId> ALLOW_ALL = new Predicate<EntityId>() {
    @Override
    public boolean apply(EntityId entityId) {
      return true;
    }
  };

  @Inject
  DefaultAuthorizationEnforcementService(PrivilegesFetcher privilegesFetcher, CConfiguration cConf,
                                         AuthenticationContext authenticationContext) {
    super(cConf, privilegesFetcher, authenticationContext, "enforcement");
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Action action) throws Exception {
    enforce(entity, principal, Collections.singleton(action));
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }

    doEnforce(entity, principal, actions, true);
  }

  @Override
  public Predicate<EntityId> createFilter(Principal principal) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return ALLOW_ALL;
    }
    Map<EntityId, Set<Action>> privileges = getPrivileges(principal);
    final Set<EntityId> allowedEntities = privileges != null ? privileges.keySet() : Collections.<EntityId>emptySet();

    return new Predicate<EntityId>() {
      @Override
      public boolean apply(EntityId entityId) {
        boolean parentPassed = false;
        if (entityId instanceof ParentedId) {
          parentPassed = apply(((ParentedId) entityId).getParent());
        }
        return (parentPassed || allowedEntities.contains(entityId));
      }
    };
  }

  protected boolean isSecurityAuthorizationEnabled() {
    return securityEnabled && authorizationEnabled;
  }

  private boolean doEnforce(EntityId entity, Principal principal,
                            Set<Action> actions, boolean exceptionOnFailure) throws Exception {
    if (entity instanceof ParentedId) {
      if (doEnforce(((ParentedId) entity).getParent(), principal, actions, false)) {
        return true;
      }
    }

    Set<Action> allowedActions = getPrivileges(principal).get(entity);
    LOG.trace("Enforcing actions {} on {} for {}. Allowed actions are {}", actions, entity, principal, allowedActions);
    if (allowedActions == null) {
      if (exceptionOnFailure) {
        throw new UnauthorizedException(principal, actions, entity);
      }
      return false;
    }

    // Check for the specific actions requested
    if (allowedActions.containsAll(actions)) {
      return true;
    }
    if (exceptionOnFailure) {
      throw new UnauthorizedException(principal, Sets.difference(actions, allowedActions), entity);
    }
    return false;
  }

  @Override
  public void invalidate(com.google.common.base.Predicate<Principal> predicate) {
    doInvalidate(predicate);
  }
}
