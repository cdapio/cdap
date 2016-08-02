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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.PrivilegesFetcher;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

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

  private final Set<Principal> superUsers;

  @Inject
  DefaultAuthorizationEnforcementService(PrivilegesFetcher privilegesFetcher, CConfiguration cConf) {
    super(privilegesFetcher, cConf, "enforcement");
    this.superUsers = getSuperUsers(cConf.get(Constants.Security.Authorization.SUPERUSERS));
    Preconditions.checkArgument(
      !superUsers.isEmpty(), "No super users specified. Without this setting, it may be impossible to bootstrap CDAP " +
        "with authorization enabled. Please set %s to a comma separated list of superusers who can bypass " +
        "authorization policies in CDAP.", Constants.Security.Authorization.SUPERUSERS);
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
    // For accessing system datasets for internal operations like recording metadata, usage, lineage, etc
    if (Principal.SYSTEM.equals(principal)) {
      return;
    }
    // If the principal is a superuser, allow access
    if (isSuperUser(principal)) {
      return;
    }

    Set<Action> allowedActions = getPrivileges(principal).get(entity);
    LOG.trace("Enforcing actions {} on {} for {}. Allowed actions are {}", actions, entity, principal, allowedActions);
    if (allowedActions == null) {
      throw new UnauthorizedException(principal, actions, entity);
    }

    // If a principal has ALL privileges on the entity, authorization should succeed
    // Otherwise check for the specific actions requested
    if (!allowedActions.contains(Action.ALL) && !allowedActions.containsAll(actions)) {
      throw new UnauthorizedException(principal, Sets.difference(actions, allowedActions), entity);
    }
  }

  @Override
  public Predicate<EntityId> createFilter(Principal principal) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return ALLOW_ALL;
    }
    // For accessing system datasets for internal operations like recording metadata, usage, lineage, etc
    if (Principal.SYSTEM.equals(principal)) {
      return ALLOW_ALL;
    }
    // If the principal is a super user, do not filter
    if (isSuperUser(principal)) {
      return ALLOW_ALL;
    }
    Map<EntityId, Set<Action>> privileges = getPrivileges(principal);
    final Set<EntityId> allowedEntities = privileges != null ? privileges.keySet() : new HashSet<EntityId>();

    return new Predicate<EntityId>() {
      @Override
      public boolean apply(EntityId entityId) {
        return allowedEntities.contains(entityId);
      }
    };
  }

  private Set<Principal> getSuperUsers(@Nullable String superUsers) {
    ImmutableSet.Builder<Principal> result = new ImmutableSet.Builder<>();
    if (superUsers != null) {
      for (String curUser : Splitter.on(",").trimResults().split(superUsers)) {
        result.add(new Principal(curUser, Principal.PrincipalType.USER));
      }
    }
    return result.build();
  }

  private boolean isSuperUser(Principal principal) {
    return superUsers.contains(principal) || superUsers.contains(new Principal("*", Principal.PrincipalType.USER));
  }

  protected boolean isSecurityAuthorizationEnabled() {
    return securityEnabled && authorizationEnabled;
  }
}
