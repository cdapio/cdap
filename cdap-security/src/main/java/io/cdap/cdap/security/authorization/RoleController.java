/*
 * Copyright © 2023 Cask Data, Inc.
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

import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.AlreadyExistsException;
import io.cdap.cdap.security.spi.authorization.GrantFetcher;
import java.util.Set;

/**
 * This interface is responsible to manage role related actions and it's relation with {@link Principal}
 * This logic was previously used in {@link AccessController}
 * This is an API .
 */
public interface RoleController {

  /**
   * Create a role.
   *
   * @param role the {@link Role} to create
   * @throws AlreadyExistsException if the the role to be created already exists
   */
  void createRole(Role role) throws AccessException;

  /**
   * Drop a role.
   *
   * @param role the {@link Role} to drop
   * @throws AlreadyExistsException if the role to be dropped is not found
   */
  void dropRole(Role role) throws AccessException;

  /**
   * Add a role to the specified {@link Principal}.
   *
   * @param role the {@link Role} to add to the specified group
   * @param principal the {@link Principal} to add the role to
   * @throws AlreadyExistsException if the role to be added to the principals is not found
   */
  void addRoleToPrincipal(Role role, Principal principal) throws AccessException;

  /**
   * Delete a role from the specified {@link Principal}.
   *
   * @param role the {@link Role} to remove from the specified group
   * @param principal the {@link Principal} to remove the role from
   * @throws AlreadyExistsException if the role to be removed to the principals is not found
   */
  void removeRoleFromPrincipal(Role role, Principal principal) throws AccessException;

  /**
   * Returns a set of all {@link Role roles} for the specified {@link Principal}.
   *
   * @param principal the {@link Principal} to look up roles for
   * @return Set of {@link Role} for the specified {@link Principal}
   */
  Set<Role> listRoles(Principal principal) throws AccessException;

  /**
   * Returns all available {@link Role}. Only a super user can perform this operation.
   *
   * @return a set of all available {@link Role} in the system.
   */
  Set<Role> listAllRoles() throws AccessException;


  /**
   * Copied from {@link GrantFetcher} to avoid complex extends
   * Returns all the {@link GrantedPermission} for the specified {@link Principal}.
   *
   * @param principal the {@link Principal} for which to return privileges
   * @return a {@link Set} of {@link GrantedPermission} for the specified principal
   */
  Set<GrantedPermission> listGrants(Principal principal) throws AccessException;

}
