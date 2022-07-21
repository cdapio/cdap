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

package io.cdap.cdap.security.spi.authorization;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;

import java.util.Properties;
import java.util.Set;
import java.util.jar.Attributes;

/**
 * Interface for managing {@link Principal principals'} authorization for {@link Action actions} on
 * {@link EntityId CDAP entities}. Authorization extensions must implement this interface to delegate authorization
 * to appropriate authorization back-ends. The contract with Authorization extensions is as below:
 *
 * <ul>
 *   <li>Authorization is enabled setting the parameter {@code security.authorization.enabled} to true in
 *   {@code cdap-site.xml}.</li>
 *   <li>The path to the extension jar bundled with all its dependencies must be specified by
 *   {@code security.authorization.extension.jar.path} in cdap-site.xml</li>
 *   <li>The extension jar must contain a class that implements {@link Authorizer}. This class must be
 *   specified as the {@link Attributes.Name#MAIN_CLASS} in the extension jar's manifest file.</li>
 *   <li>The contract with the class that implements {@link Authorizer} is that it must have a default
 *   constructor.</li>
 *   <li>{@link Authorizer} also provides lifecycle methods for extensions. {@link #initialize(AuthorizationContext)}
 *   can be used to perform initialization tasks. This method provides an {@link AuthorizationContext} which gives
 *   extensions access to CDAP entities for operations like creating and accessing datasets, accessing datasets in
 *   transactions, etc. It also provides access to {@link Properties extension properties} via the
 *   {@link AuthorizationContext#getExtensionProperties()} method. The {@link Properties} object returned form this
 *   method is populated with all configuration settings from {@code cdap-site.xml} that have
 *   keys with the prefix {@code security.authorization.extension.config}.</li>
 *   <li>The {@link #destroy()} method can be used to perform cleanup tasks.</li>
 * </ul>
 * @deprecated Use {@link AccessController}
 */
@Beta @Deprecated
public interface Authorizer extends PrivilegesFetcher, PrivilegesManager, AuthorizationEnforcer {
  /**
   * Initialize the {@link Authorizer}. Authorization extensions can use this method to access an
   * {@link AuthorizationContext} that allows them to interact with CDAP for operations such as creating and accessing
   * datasets, executing dataset operations in transactions, etc.
   *
   * @param context the {@link AuthorizationContext} that can be used to interact with CDAP
   */
  void initialize(AuthorizationContext context) throws Exception;

  /**
   * Create a role.
   *
   * @param role the {@link Role} to create
   * @throws AlreadyExistsException if the the role to be created already exists
   */
  void createRole(Role role) throws Exception;

  /**
   * Drop a role.
   *
   * @param role the {@link Role} to drop
   * @throws AlreadyExistsException if the role to be dropped is not found
   */
  void dropRole(Role role) throws Exception;

  /**
   * Add a role to the specified {@link Principal}.
   *
   * @param role the {@link Role} to add to the specified group
   * @param principal the {@link Principal} to add the role to
   * @throws AlreadyExistsException if the role to be added to the principals is not found
   */
  void addRoleToPrincipal(Role role, Principal principal) throws Exception;

  /**
   * Delete a role from the specified {@link Principal}.
   *
   * @param role the {@link Role} to remove from the specified group
   * @param principal the {@link Principal} to remove the role from
   * @throws AlreadyExistsException if the role to be removed to the principals is not found
   */
  void removeRoleFromPrincipal(Role role, Principal principal) throws Exception;

  /**
   * Returns a set of all {@link Role roles} for the specified {@link Principal}.
   *
   * @param principal the {@link Principal} to look up roles for
   * @return Set of {@link Role} for the specified {@link Principal}
   */
  Set<Role> listRoles(Principal principal) throws Exception;

  /**
   * Returns all available {@link Role}. Only a super user can perform this operation.
   *
   * @return a set of all available {@link Role} in the system.
   */
  Set<Role> listAllRoles() throws Exception;

  /**
   * Destroys an {@link Authorizer}. Authorization extensions can use this method to write any cleanup code.
   */
  void destroy() throws Exception;
}
