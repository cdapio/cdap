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

package co.cask.cdap.security.impersonation;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Class to manage owner's principal information of CDAP entities.
 * <p>
 * Currently: Owner information is stored for the following entities:
 * <ul>
 * <li>{@link co.cask.cdap.api.data.stream.Stream}</li>
 * <li>{@link co.cask.cdap.api.dataset.Dataset}</li>
 * <li>{@link co.cask.cdap.api.app.Application}</li>
 * <li>{@link co.cask.cdap.common.conf.Constants.Namespace}</li>
 * <p>
 * </ul>
 * </p>
 * <p>
 * It is the responsibility of the creator of the supported entities to add an entry in the store for the
 * associated owner's principal. Note: An absence of an entry in this table for an {@link EntityId} does not
 * signifies that the entity does not exists. The owner information is only stored if an owner was provided during
 * creation time else the owner information is non-existent which signifies that the entity owner is default CDAP owner.
 * </p>
 * <p>
 */
public abstract class OwnerStore {

  private static final Set<EntityType> SUPPORTED_ENTITY_TYPES = Sets.immutableEnumSet(EntityType.NAMESPACE,
                                                                                      EntityType.APPLICATION,
                                                                                      EntityType.DATASET,
                                                                                      EntityType.STREAM,
                                                                                      EntityType.ARTIFACT);

  /**
   * Validates the given {@link NamespacedEntityId} to be supported by the {@link OwnerStore}
   * i.e. the entity can be associated with an owner.
   * Validated the given {@link KerberosPrincipalId} to be valid i.e. it can be used to create a
   * {@link org.apache.hadoop.security.authentication.util.KerberosName}.
   * See {@link SecurityUtil#validateKerberosPrincipal(KerberosPrincipalId)}
   *
   * @param entityId {@link NamespacedEntityId} to be validated
   * @param principalId {@link KerberosPrincipalId} to be validated
   */
  protected final void validate(NamespacedEntityId entityId, KerberosPrincipalId principalId) {
    validate(entityId);
    SecurityUtil.validateKerberosPrincipal(principalId);
  }

  /**
   * Validates the given {@link NamespacedEntityId} to be supported by the {@link OwnerStore}
   * i.e. the entity can be associated with an owner.
   *
   * @param entityId {@link NamespacedEntityId} to be validated
   */
  protected final void validate(NamespacedEntityId entityId) {
    if (!SUPPORTED_ENTITY_TYPES.contains(entityId.getEntityType())) {
      throw new IllegalArgumentException(String.format("The given entity '%s' is of unsupported types '%s'. " +
                                                         "Entity ownership is only supported for '%s'.",
                                                       entityId.getEntityName(), entityId.getEntityType(),
                                                       SUPPORTED_ENTITY_TYPES));
    }
  }

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
  public abstract void add(NamespacedEntityId entityId,
                           KerberosPrincipalId kerberosPrincipalId) throws IOException, AlreadyExistsException;

  /**
   * Retrieves the owner information for the given {@link EntityId}
   *
   * @param entityId the {@link EntityId} whose owner principal information needs to be retrieved
   * @return {@link KerberosPrincipalId} of the {@link EntityId} owner
   * @throws IOException if failed to get the store
   * @throws IllegalArgumentException if the given entity is not of supported type.
   */
  @Nullable
  public abstract KerberosPrincipalId getOwner(NamespacedEntityId entityId) throws IOException;

  /**
   * Checks if owner information exists or not
   *
   * @param entityId the {@link EntityId} for which the check needs to be performed
   * @return a boolean true: owner principal exists, false: no owner principal exists
   * @throws IOException if failed to get the store
   * @throws IllegalArgumentException if the given entity is not of supported type.
   */
  public abstract boolean exists(NamespacedEntityId entityId) throws IOException;

  /**
   * Deletes the owner principal for the given {@link EntityId}
   *
   * @param entityId the entity whose owner principal needs to be deleted
   * @throws IOException if failed to get the owner store
   * @throws IllegalArgumentException if the given entity is not of supported type.
   */
  public abstract void delete(NamespacedEntityId entityId) throws IOException;
}
