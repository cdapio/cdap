/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.common.metadata;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.proto.id.EntityId;

import java.util.Set;

/**
 * Common utilities for metadata, can be used by all implementations of the Metadata SPI.
 */
public final class MetadataUtil {

  private static final Set<String> VERSIONED_ENTITY_TYPES = ImmutableSet.of(
    MetadataEntity.APPLICATION,
    MetadataEntity.SCHEDULE,
    MetadataEntity.PROGRAM,
    MetadataEntity.PROGRAM_RUN);

  /**
   * @return whether an entity of the given type requires a version
   */
  public static boolean isVersionedEntityType(String type) {
    return VERSIONED_ENTITY_TYPES.contains(type);
  }

  /**
   * Add the default version to an entity if it is a versioned entity type.
   */
  public static MetadataEntity addVersionIfNeeded(MetadataEntity entity) {
    // EntityId.fromMetadataEntity already has the logic to add version information
    // in the correct place depending on the type of the entity so use that
    if (MetadataUtil.isVersionedEntityType(entity.getType())) {
      return EntityId.fromMetadataEntity(entity).toMetadataEntity();
    }
    return entity;
  }
}
