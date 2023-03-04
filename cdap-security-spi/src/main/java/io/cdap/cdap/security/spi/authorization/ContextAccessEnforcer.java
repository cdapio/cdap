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

package io.cdap.cdap.security.spi.authorization;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import java.util.Collections;
import java.util.Set;

/**
 * Enforces authorization for the current {@link SecurityRequestContext} {@link Principal} to
 * perform an {@link Permission} on an {@link EntityId}. @see AccessEnforcer for enforcer for any
 * principal
 */
@Beta
public interface ContextAccessEnforcer {

  /**
   * Enforces authorization for the specified {@link Principal} for the specified {@link Permission}
   * on the specified {@link EntityId}.
   *
   * @param entity the {@link EntityId} on which authorization is to be enforced
   * @param permission the {@link Permission} being performed
   * @throws UnauthorizedException if the current user is not authorized to perform the
   *     specified permission on the entity
   */
  default void enforce(EntityId entity, Permission permission) throws AccessException {
    enforce(entity, Collections.singleton(permission));
  }

  /**
   * Enforces authorization for the current user for the specified {@link Permission permissions} on
   * the specified {@link EntityId}.
   *
   * @param entity the {@link EntityId} on which authorization is to be enforced
   * @param permissions the {@link Permission permissions} being performed
   * @throws UnauthorizedException if the current user is not authorized to perform the
   *     specified permissions on t he entity
   */
  void enforce(EntityId entity, Set<? extends Permission> permissions) throws AccessException;

  /**
   * Enforces specific {@link Permission#isCheckedOnParent()} permission for {@link EntityType} on
   * it's parent {@link EntityId}. E.g. one can check if it's possible to {@link
   * StandardPermission#LIST} {@link EntityType#PROFILE} in specific {@link
   * io.cdap.cdap.proto.id.NamespaceId}.
   *
   * @param entityType the {@link EntityType} on which authorization is to be enforced
   * @param parentId the {@link EntityId} of parent entity on which authorization is to be
   *     enforced
   * @param permission the {@link Permission} being performed. Permission must return true on
   *     {@link Permission#isCheckedOnParent()}.
   * @throws UnauthorizedException if the principal is not authorized to perform the specified
   *     permission on the entity
   */
  void enforceOnParent(EntityType entityType, EntityId parentId,
      Permission permission) throws AccessException;

  /**
   * Checks whether the set of {@link EntityId}s are visible to the current user. An entity is
   * visible to a user if the user has {@link StandardPermission#GET} on the entity. However,
   * visibility check behavior can be overwritten at the authorization extension level.
   *
   * @param entityIds the entities on which the visibility check is to be performed
   * @return a set of entities that are visible to the principal
   */
  Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds) throws AccessException;
}
