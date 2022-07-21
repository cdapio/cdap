/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

import java.util.Collections;
import java.util.Set;

/**
 * Enforces authorization for a {@link Principal} to perform an {@link Permission} on an {@link EntityId}.
 */
@Beta
public interface AccessEnforcer {

  /**
   * Enforces authorization for the specified {@link Principal} for the specified {@link Permission} on the specified
   * {@link EntityId}.
   *
   * @param entity the {@link EntityId} on which authorization is to be enforced
   * @param principal the {@link Principal} that performs the permission
   * @param permission the {@link Permission} being performed
   * @throws UnauthorizedException if the principal is not authorized to perform the specified permission on the entity
   */
  default void enforce(EntityId entity, Principal principal, Permission permission) throws AccessException {
    enforce(entity, principal, Collections.singleton(permission));
  }

  /**
   * Enforces authorization for the specified {@link Principal} for the specified {@link Permission permissions} on the
   * specified {@link EntityId}.
   *
   * @param entity the {@link EntityId} on which authorization is to be enforced
   * @param principal the {@link Principal} that performs the permissions
   * @param permissions the {@link Permission permissions} being performed
   * @throws UnauthorizedException if the principal is not authorized to perform the specified permissions on the entity
   */
  void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions) throws AccessException;

  /**
   * Enforces specific {@link Permission#isCheckedOnParent()} permission for {@link EntityType} on it's parent
   * {@link EntityId}. E.g. one can check if it's possible to {@link StandardPermission#LIST}
   * {@link EntityType#PROFILE} in specific {@link io.cdap.cdap.proto.id.NamespaceId}.
   *
   * @param entityType the {@link EntityType} on which authorization is to be enforced
   * @param parentId the {@link EntityId} of parent entity on which authorization is to be enforced
   * @param principal the {@link Principal} that performs the permission
   * @param permission the {@link Permission} being performed. Permission must return true on
   * {@link Permission#isCheckedOnParent()}.
   * @throws UnauthorizedException if the principal is not authorized to perform the specified permission on the entity
   */
  void enforceOnParent(EntityType entityType, EntityId parentId, Principal principal,
                       Permission permission) throws AccessException;

  /**
   * Checks whether the set of {@link EntityId}s are visible to the specified {@link Principal}.
   * An entity is visible to a principal if the principal has {@link StandardPermission#GET} on the entity.
   * However, visibility check behavior can be overwritten at the authorization extension level.
   *
   * @param entityIds the entities on which the visibility check is to be performed
   * @param principal the principal to check the visibility for
   * @return a set of entities that are visible to the principal
   */
  Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal) throws AccessException;

}
