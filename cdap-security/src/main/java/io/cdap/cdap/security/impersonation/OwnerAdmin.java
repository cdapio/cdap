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

package io.cdap.cdap.security.impersonation;

import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespacedEntityId;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
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
   * <p>Retrieves the owner information for the given {@link EntityId}</p>
   * <p>Note: a null return value does not indicate presence or absence of the given entity in the system.
   * It only means that no explicit owner principal was specified during entity creation and it's owned by the system
   * if its present</p>
   *
   * @param entityId the {@link EntityId} whose owner principal information needs to be retrieved
   * @return {@link KerberosPrincipalId} of the {@link EntityId} owner if one was explicitly provided during entity
   * creation or null if
   * <ol>
   * <li>the entity does not exists in the system</li>
   * <li>entity exists in the system but no explicit owner principal was specified during creation</li>
   * </ol>
   * @throws IOException if failed to get the store
   * @throws IllegalArgumentException if the given entity is not of supported type.
   */
  @Nullable
  KerberosPrincipalId getOwner(NamespacedEntityId entityId) throws IOException;

  /**
   * <p>Delegates to {@link #getOwner(NamespacedEntityId)} to retrieve the owner information for the given
   * {@link EntityId} and then return the principal of the entity as {@link String} by
   * calling {@link KerberosPrincipalId#getPrincipal()} on the owner's principal if one exists.</p>
   * <p>Note: a null return value does not indicate presence or absence of the given entity in the system.
   * It only means that no explicit owner principal was specified during entity creation and it's owned by the system
   * if its present</p>
   *
   * @param entityId the {@link EntityId} whose owner principal information needs to be retrieved
   * @return {@link String} the principal of the {@link EntityId} owner if one was explicitly provided during entity
   * creation or null if
   * <ol>
   * <li>the entity does not exists in the system</li>
   * <li>entity exists in the system but no explicit owner principal was specified during creation</li>
   * </ol>
   * @throws IOException if failed to get the store
   * @throws IllegalArgumentException if the given entity is not of supported type.
   */
  @Nullable
  String getOwnerPrincipal(NamespacedEntityId entityId) throws IOException;

  /**
   * Batch version of the {@link #getOwnerPrincipal(NamespacedEntityId)} method for retrieving principals of multiple
   * {@link NamespacedEntityId}.
   *
   * @param ids the set of {@link NamespacedEntityId} for getting the owners.
   * @param <T> type of the entity
   * @return a {@link Map} from the entity id to the owner if one was explicitly provided during entity
   * creation or no entry if
   * <ol>
   * <li>the entity does not exists in the system</li>
   * <li>entity exists in the system but no explicit owner principal was specified during creation</li>
   * </ol>
   * @throws IOException if failed to get the information
   * @throws IllegalArgumentException if any of the given entities is not of supported type.
   */
  <T extends NamespacedEntityId> Map<T, String> getOwnerPrincipals(Set<T> ids) throws IOException;

  /**
   * <p>
   * Retrieves the impersonation information for the given {@link EntityId} by tracing the entity hierarchy.
   * </p>
   * <p>
   * If an owner is present for this entity id then returns the information of that immediate owner.
   * If a direct owner is not present then the namespace owner information will be returned if
   * one is present else returns null.
   * </p>
   * @param entityId the {@link EntityId} whose owner principal information needs to be retrieved
   * @return {@link ImpersonationInfo} of the effective owner for the given entity.
   * @throws AccessException if  failed to get the store
   * @throws IllegalArgumentException if the given entity is not of supported type.
   */
  @Nullable
  ImpersonationInfo getImpersonationInfo(NamespacedEntityId entityId) throws AccessException;

  /**
   * <p>Delegates to {@link #getImpersonationInfo(NamespacedEntityId)}} to retrieve the owner information by tracing
   * the entity hierarchy for the given {@link EntityId} and then return the principal of the entity as {@link String}
   * by calling {@link ImpersonationInfo#getPrincipal()}.</p>
   * <p>
   * If an owner is present for this entity id then returns the information of that immediate owner.
   * If a direct owner is not present then the namespace owner information will be returned if
   * one is present else returns null.</p>
   * <p>Note: a null return value does not indicate presence or absence of the given entity in the system.
   * It only means that no explicit owner principal was specified during entity creation and it's owned by the system
   * if its present</p>
   *
   * @param entityId the {@link EntityId} whose owner principal information needs to be retrieved
   * @return {@link String} the principal of the {@link EntityId} owner if one was explicitly provided during entity
   * creation or null if
   * <ol>
   * <li>the entity does not exists in the system</li>
   * <li>entity exists in the system but no explicit owner principal was specified during creation</li>
   * </ol>
   * @throws IOException if failed to get the store
   * @throws IllegalArgumentException if the given entity is not of supported type.
   */
  @Nullable
  String getImpersonationPrincipal(NamespacedEntityId entityId) throws IOException;

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
   * <p>Deletes the owner principal for the given {@link EntityId} </p>
   * <p>This call does not throw {@link io.cdap.cdap.common.NotFoundException} if the entity does not
   * exists in the store so it's safe to call without checking its existence</p>
   *
   * @param entityId the entity whose owner principal needs to be deleted
   * @throws IOException if failed to get the owner store
   * @throws IllegalArgumentException if the given entity is not of supported type.
   */
  void delete(NamespacedEntityId entityId) throws IOException;
}
