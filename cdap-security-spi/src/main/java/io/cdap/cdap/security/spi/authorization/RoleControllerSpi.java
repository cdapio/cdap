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

package io.cdap.cdap.security.spi.authorization;

import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import java.util.Set;

/**
 * This interface is responsible to manage role related actions and it's relation with {@link Principal}
 * This logic was previously used in {@link AccessController}
 * This is a SPI .
 */
public interface RoleControllerSpi {

  /**
   * Create a role.
   *
   * @param caller , the principal who is performing this call.
   * @param role the {@link Role} to create
   * @throws AlreadyExistsException if the the role to be created already exists
   */
  AuthorizationResponse createRole(Principal caller, Role role) throws AccessException;

  /**
   * Drop a role.
   *
   * @param caller , the {@link Principal} who is performing this call.
   * @param role the {@link Role} to drop
   * @throws AlreadyExistsException if the role to be dropped is not found
   */
  AuthorizationResponse dropRole(Principal caller, Role role) throws AccessException;

  /**
   * Add a role to the specified {@link Principal}.
   *
   * @param caller , the {@link Principal} who is performing this call.
   * @param role the {@link Role} to add to the specified group
   * @param principal the {@link Principal} to add the role to
   * @throws AlreadyExistsException if the role to be added to the principals is not found
   */
  AuthorizationResponse addRoleToPrincipal(Principal caller, Role role, Principal principal) throws AccessException;

  /**
   * Delete a role from the specified {@link Principal}.
   *
   * @param caller , the {@link Principal} who is performing this call.
   * @param role the {@link Role} to remove from the specified group
   * @param principal the {@link Principal} to remove the role from
   * @throws AlreadyExistsException if the role to be removed to the principals is not found
   */
  AuthorizationResponse removeRoleFromPrincipal(Principal caller, Role role, Principal principal)
    throws AccessException;

  /**
   * Returns a set of all {@link Role roles} for the specified {@link Principal}.
   *
   * @param caller , the {@link Principal} who is performing this call.
   * @param principal the {@link Principal} to look up roles for
   * @return a AuthorizationResult which consists of ( setof {@link Role} for the specified principal)
   *          and {@link AuthorizationResponse}
   */
  AuthorizedResult<Set<Role>> listRoles(Principal caller, Principal principal) throws AccessException;

  /**
   * Returns all available {@link Role}. Only a super user can perform this operation.
   *
   * @param caller , the {@link Principal} who is performing this call.
   * @return a AuthorizationResult which consists of ( setof {@link Role} for the specified principal)
   *          and {@link AuthorizationResponse}
   */
  AuthorizedResult<Set<Role>> listAllRoles(Principal caller) throws AccessException;

  /**
   * Returns all the {@link GrantedPermission}  with it's {@link AuthorizationResponse } for the
   * specified {@link Principal}.
   *
   * @param caller , the {@link Principal} who is performing this call.
   * @param principal the {@link Principal} for which to return privileges
   * @return a AuthorizationResult which consists of ( setof {@link GrantedPermission} for the specified principal)
   *          and {@link AuthorizationResponse}
   */
  AuthorizedResult<Set<GrantedPermission>> listGrants(Principal caller, Principal principal)
    throws AccessException;
}
