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

import java.util.Properties;
import java.util.Set;
import java.util.jar.Attributes;

/**
 * Interface to grant/revoke {@link Principal principals} authorization for {@link Action actions} on
 * {@link EntityId CDAP entities}. Authorization extensions must implement this interface to delegate authorization
 * to appropriate authorization backends. The contract with Authorization extensions is as below:
 *
 * <ul>
 *   <li>Authorization is enabled setting the parameter {@code security.authorization.enabled} to true in
 *   {@code cdap-site.xml}.</li>
 *   <li>The path to the extension jar bundled with all its dependencies must be specified by
 *   {@code security.authorization.extension.jar.path} in cdap-site.xml</li>
 *   <li>The extension jar must contain a class that implements {@link Authorizer}. This class must be specified as
 *   the {@link Attributes.Name#MAIN_CLASS} in the extension jar's manifest file.</li>
 *   <li>The contract with the class that implements {@link Authorizer} is that it must have a public constructor that
 *   accepts a single {@link Properties} object as parameter. This constructor is invoked with a {@link Properties}
 *   object that is populated with all configuration settings from {@code cdap-site.xml} that have keys with the prefix
 *   {@code security.authorization.extension.config}.</li>
 * </ul>
 */
public interface Authorizer {
  /**
   * Enforces authorization for the specified {@link Principal} for the specified {@link Action} on the specified
   * {@link EntityId}.
   *
   * @param entity the {@link EntityId} on which authorization is to be enforced
   * @param principal the {@link Principal} that performs the actions
   * @param action the {@link Action} being performed
   * @throws UnauthorizedException if the principal is not authorized to perform action on the entity
   */
  void enforce(EntityId entity, Principal principal, Action action) throws UnauthorizedException;

  /**
   * Grants a {@link Principal} authorization to perform a set of {@link Action actions} on an {@link EntityId}.
   *
   * @param entity the {@link EntityId} to whom {@link Action actions} are to be granted
   * @param principal the {@link Principal} that performs the actions. This could be a user, group or a role
   * @param actions the set of {@link Action actions} to grant.
   */
  void grant(EntityId entity, Principal principal, Set<Action> actions);

  /**
   * Revokes a {@link Principal principal's} authorization to perform a set of {@link Action actions} on
   * an {@link EntityId}.
   *
   * @param entity the {@link EntityId} whose {@link Action actions} are to be revoked
   * @param principal the {@link Principal} that performs the actions. This could be a user, group or a role
   * @param actions the set of {@link Action actions} to revoke
   */
  void revoke(EntityId entity, Principal principal, Set<Action> actions);

  /**
   * Revokes all {@link Principal principals'} authorization to perform any {@link Action} on the given
   * {@link EntityId}.
   *
   * @param entity the {@link EntityId} on which all {@link Action actions} are to be revoked
   */
  void revoke(EntityId entity);
}
