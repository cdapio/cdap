/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.security.authorization;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.security.AuthEnforceUtil;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.AuthorizationContext;
import io.cdap.cdap.security.spi.authorization.Authorizer;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Wraps an {@link Authorizer} and creates an {@link AccessController} out of it
 */
public class AuthorizerWrapper implements AccessController {
  private final Authorizer authorizer;

  public AuthorizerWrapper(Authorizer authorizer) {
    this.authorizer = authorizer;
  }

  @Override
  public void initialize(AuthorizationContext context) {
    try {
      authorizer.initialize(context);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void createRole(Role role) throws AccessException {
    try {
      authorizer.createRole(role);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public void dropRole(Role role) throws AccessException {
    try {
      authorizer.dropRole(role);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public void addRoleToPrincipal(Role role, Principal principal) throws AccessException {
    try {
      authorizer.addRoleToPrincipal(role, principal);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public void removeRoleFromPrincipal(Role role, Principal principal) throws AccessException {
    try {
      authorizer.removeRoleFromPrincipal(role, principal);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public Set<Role> listRoles(Principal principal) throws AccessException {
    try {
      return authorizer.listRoles(principal);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public Set<Role> listAllRoles() throws AccessException {
    try {
      return authorizer.listAllRoles();
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public void destroy() {
    try {
      authorizer.destroy();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Set<GrantedPermission> listGrants(Principal principal) throws AccessException {
    try {
      return authorizer.listPrivileges(principal).stream().map(
        p -> new GrantedPermission(p.getAuthorizable(), p.getAction().getPermission())
      ).collect(Collectors.toSet());
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public void grant(Authorizable authorizable, Principal principal, Set<? extends Permission> permissions)
    throws AccessException {
    try {
      authorizer.grant(authorizable, principal, getActionSet(permissions, authorizable.getEntityType()));
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public void revoke(Authorizable authorizable, Principal principal, Set<? extends Permission> permissions)
    throws AccessException {
    try {
      authorizer.revoke(authorizable, principal, getActionSet(permissions, authorizable.getEntityType()));
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public void revoke(Authorizable authorizable) throws AccessException {
    try {
      authorizer.revoke(authorizable);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Permission permission) throws AccessException {
    try {
      authorizer.enforce(entity, principal, getAction(permission, entity.getEntityType()));
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions)
    throws AccessException {
    try {
      authorizer.enforce(entity, principal, getActionSet(permissions, entity.getEntityType()));
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public void isVisible(EntityId entityId, Principal principal) throws AccessException {
    try {
      authorizer.isVisible(entityId, principal);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal)
    throws AccessException {
    try {
      return authorizer.isVisible(entityIds, principal);
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  private Set<Action> getActionSet(Set<? extends Permission> permissions, EntityType entityType) {
    return permissions.stream().map(p -> getAction(p, entityType))
      .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  static Action getAction(Permission permission, EntityType entityType) {
    if (StandardPermission.GET == permission
      && (entityType == EntityType.DATASET || entityType == EntityType.SECUREKEY)) {
      return Action.READ;
    } else if (StandardPermission.UPDATE == permission && entityType == EntityType.DATASET) {
      return Action.WRITE;
    } else if (ApplicationPermission.EXECUTE == permission) {
      return Action.EXECUTE;
    }
    return Action.ADMIN;
  }
}
