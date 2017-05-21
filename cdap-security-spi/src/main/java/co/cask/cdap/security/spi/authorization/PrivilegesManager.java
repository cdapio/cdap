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

package co.cask.cdap.security.spi.authorization;

import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;

import java.util.Set;

/**
 * Interface for managing privileges on {@link EntityId entities}.
 */
public interface PrivilegesManager {
  /**
   * Grants a {@link Principal} authorization to perform a set of {@link Action actions} on an {@link EntityId}.
   *
   * @param entity the {@link EntityId} to whom {@link Action actions} are to be granted
   * @param principal the {@link Principal} that performs the actions. This could be a user, or role
   * @param actions the set of {@link Action actions} to grant.
   */
  void grant(EntityId entity, Principal principal, Set<Action> actions) throws Exception;

  /**
   * Revokes a {@link Principal principal's} authorization to perform a set of {@link Action actions} on
   * an {@link EntityId}.
   *
   * @param entity the {@link EntityId} whose {@link Action actions} are to be revoked
   * @param principal the {@link Principal} that performs the actions. This could be a user, group or role
   * @param actions the set of {@link Action actions} to revoke
   */
  void revoke(EntityId entity, Principal principal, Set<Action> actions) throws Exception;

  /**
   * Revokes all {@link Principal principals'} authorization to perform any {@link Action} on the given
   * {@link EntityId}.
   *
   * @param entity the {@link EntityId} on which all {@link Action actions} are to be revoked
   */
  void revoke(EntityId entity) throws Exception;

  /**
   * Returns all the {@link Privilege} for the specified {@link Principal}.
   *
   * @param principal the {@link Principal} for which to return privileges
   * @return a {@link Set} of {@link Privilege} for the specified principal
   */
  Set<Privilege> listPrivileges(Principal principal) throws Exception;
}
