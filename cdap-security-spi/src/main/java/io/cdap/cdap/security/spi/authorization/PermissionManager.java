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

import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.AccessException;

import java.util.Set;

public interface PermissionManager {

  /**
   * Grants a {@link Principal} authorization to perform a set of {@link Permission permissions} on {@link EntityId}
   * represented by the {@link Authorizable}
   * Note: this grant is used to support wildcard privilege management
   * @param authorizable The {@link Authorizable} on which the {@link Permission} are to be granted
   * @param principal the {@link Principal} that performs the permissions. This could be a user, or role
   * @param permissions the set of {@link Permission permissions} to grant.
   */
  void grant(Authorizable authorizable, Principal principal, Set<? extends Permission> permissions)
    throws AccessException;

  /**
   * Revokes a {@link Principal} authorization to perform a set of {@link Permission permissions} on {@link EntityId}
   * represented by the {@link Authorizable}
   * Note: this revoke is used to support wildcard privilege management.
   * @param authorizable the {@link Authorizable} whose {@link Permission permissions} are to be revoked
   * @param principal the {@link Principal} that performs the permissions. This could be a user, group or role
   * @param permissions the set of {@link Permission permissions} to revoke
   */
  void revoke(Authorizable authorizable, Principal principal, Set<? extends Permission> permissions)
    throws AccessException;

  /**
   * Revokes all {@link Principal}s authorization to perform any set of {@link Permission permissions} on
   * {@link EntityId} represented by the {@link Authorizable}
   *
   * @param authorizable the {@link Authorizable} on which all {@link Permission permissions} are to be revoked
   */
  void revoke(Authorizable authorizable) throws AccessException;

  /**
   * Returns all the {@link GrantedPermission} for the specified {@link Principal}.
   *
   * @param principal the {@link Principal} for which to return privileges
   * @return a {@link Set} of {@link GrantedPermission} for the specified principal
   */
  Set<GrantedPermission> listGrants(Principal principal) throws AccessException;
}
