/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;

import java.util.Set;

/**
 * Authorization plugin interface.
 */
public interface AuthorizationPlugin {
  /**
   * Checks if a user is allowed to perform a set of actions on an entity.
   *
   * @param entity the entity
   * @param user the user
   * @param actions the actions
   * @return true if the user is allowed to perform all of the actions on the entity
   */
  boolean authorized(EntityId entity, String user, Set<Action> actions);

  /**
   * Grants a user permission to perform a set of actions on an entity.
   *
   * @param entity the entity
   * @param user the user
   * @param actions the actions
   */
  void grant(EntityId entity, String user, Set<Action> actions);

  /**
   * Grants a user permission to perform all actions on an entity.
   *
   * @param entity the entity
   * @param user the user
   */
  void grant(EntityId entity, String user);

  /**
   * Revokes a user's permission to perform a set of actions on an entity.
   *
   * @param entity the entity
   * @param user the user
   * @param actions the actions
   */
  void revoke(EntityId entity, String user, Set<Action> actions);

  /**
   * Revokes a user's permission on an entity.
   *
   * @param entity the entity
   * @param user the user
   */
  void revoke(EntityId entity, String user);

  /**
   * Revokes all user's permissions on an entity.
   *
   * @param entity the entity
   */
  void revoke(EntityId entity);
}
