/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.common.kerberos;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespacedEntityId;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Admin class to manage owner information of entities
 */
public interface OwnerAdmin {

  /**
   * Store the owner's kerberosPrincipalId for the given {@link EntityId}
   *
   * @param entityId The {@link EntityId} whose owner kerberosPrincipalId needs to be stored
   * @param kerberosPrincipalId the {@link KerberosPrincipalId} of the {@link EntityId} owner
   * @throws IOException if failed to get the store
   * @throws AlreadyExistsException if the given entity already has an owner
   * @throws IllegalArgumentException if the given KerberosPrincipalId is not valid or the entity is not of
   * supported type.
   */
  void add(NamespacedEntityId entityId, KerberosPrincipalId kerberosPrincipalId)
    throws IOException, AlreadyExistsException;

  /**
   * Retrieves the owner information for the given {@link EntityId}
   *
   * @param entityId the {@link EntityId} whose owner principal information needs to be retrieved
   * @return {@link KerberosPrincipalId} of the {@link EntityId} owner
   * @throws IOException if failed to get the store
   * @throws IllegalArgumentException if the given entity is not of supported type.
   */
  @Nullable
  KerberosPrincipalId getOwner(NamespacedEntityId entityId) throws IOException;

  /**
   * <p>
   * Retrieves the owner information for the given {@link EntityId} by tracing the entity hierarchy.
   * </p>
   * <p>
   * If an owner is present for this entity id then returns the {@link KerberosPrincipalId} of that immediate owner.
   * If a direct owner is not present then the namespace owner {@link KerberosPrincipalId} will be returned if
   * one is present else returns null.
   * <p>
   *
   * @param entityId the {@link EntityId} whose owner principal information needs to be retrieved
   * @return {@link KerberosPrincipalId} of the effective owner for the given entity
   * @throws IOException if  failed to get the store
   * @throws IllegalArgumentException if the given entity is not of supported type.
   */
  @Nullable
  KerberosPrincipalId getEffectiveOwner(NamespacedEntityId entityId) throws IOException;

  /**
   * Checks if owner information exists or not
   *
   * @param entityId the {@link EntityId} for which the check needs to be performed
   * @return a boolean true: owner principal exists, false: no owner principal exists
   * @throws IOException if failed to get the store
   * @throws IllegalArgumentException if the given entity is not of supported type.
   */
  boolean exists(NamespacedEntityId entityId) throws IOException;


  /**
   * Deletes the owner principal for the given {@link EntityId}
   *
   * @param entityId the entity whose owner principal needs to be deleted
   * @throws IOException if failed to get the owner store
   * @throws IllegalArgumentException if the given entity is not of supported type.
   */
  void delete(NamespacedEntityId entityId) throws IOException;
}
