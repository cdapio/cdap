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

package co.cask.cdap.store;

import co.cask.cdap.common.kerberos.OwnerStore;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Abstract implementation of {@link OwnerStore} which has some shared helper function for input validation
 */
abstract class AbstractOwnerStore implements OwnerStore {

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
  final void validate(NamespacedEntityId entityId, KerberosPrincipalId principalId) {
    validate(entityId);
    SecurityUtil.validateKerberosPrincipal(principalId);
  }

  /**
   * Validates the given {@link NamespacedEntityId} to be supported by the {@link OwnerStore}
   * i.e. the entity can be associated with an owner.
   *
   * @param entityId {@link NamespacedEntityId} to be validated
   */
  final void validate(NamespacedEntityId entityId) {
    if (!SUPPORTED_ENTITY_TYPES.contains(entityId.getEntityType())) {
      throw new IllegalArgumentException(String.format("The given entity '%s' is of unsupported types '%s'. " +
                                                         "Entity ownership is only supported for '%s'.",
                                                       entityId.getEntityName(), entityId.getEntityType(),
                                                       SUPPORTED_ENTITY_TYPES));
    }
  }
}
