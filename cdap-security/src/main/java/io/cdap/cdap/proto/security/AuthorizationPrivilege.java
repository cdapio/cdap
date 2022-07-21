/*
 * Copyright © 2017-2021 Cask Data, Inc.
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

package io.cdap.cdap.proto.security;

import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Key for caching Privileges on containers. This represents a specific privilege on which authorization can be
 * enforced. The cache stores whether the enforce succeeded or failed.
 *
 * Action may be empty in the case of isVisible checks for single entities.
 */
public class AuthorizationPrivilege {
  private final EntityId entityId;
  private final Set<Permission> permissions;
  private final Principal principal;
  private final EntityType childEntityType;

  public AuthorizationPrivilege(Principal principal, EntityId entityId, Set<? extends Permission> permissions,
                                EntityType childEntityType) {
    this.entityId = entityId;
    this.permissions = Collections.unmodifiableSet(permissions);
    this.childEntityType = childEntityType;
    this.principal = principal;
  }

  public Principal getPrincipal() {
    return principal;
  }

  public EntityId getEntity() {
    return entityId;
  }

  public Set<Permission> getPermissions() {
    return permissions;
  }
  /**
   * @return child entity type for {@link AccessEnforcer#enforceOnParent(EntityType, EntityId, Principal, Permission)}
   * checks.
   */
  @Nullable
  public EntityType getChildEntityType() {
    return childEntityType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuthorizationPrivilege that = (AuthorizationPrivilege) o;
    return Objects.equals(entityId, that.entityId) && permissions.equals(that.permissions) &&
      Objects.equals(childEntityType, that.childEntityType) &&
      Objects.equals(principal, that.principal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityId, permissions, principal, childEntityType);
  }

  @Override
  public String toString() {
    return "AuthorizationPrivilege {" +
      "entityId=" + entityId +
      ", permissions=" + permissions +
      ", principal=" + principal +
      ", childEntityType=" + childEntityType +
      '}';
  }
}
