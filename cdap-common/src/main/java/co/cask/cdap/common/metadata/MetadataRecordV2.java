/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.common.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespacedEntityId;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the complete metadata of a {@link MetadataEntity} including its properties, tags in a given
 * {@link MetadataScope}
 * this class was in cdap-api earlier and has been moved from cdap-common as its used only internally
 */
public class MetadataRecordV2 {
  private final MetadataEntity metadataEntity;
  private final MetadataScope scope;
  private final Map<String, String> properties;
  private final Set<String> tags;

  /**
   * Returns an empty {@link MetadataRecordV2} in the specified {@link MetadataScope}.
   */
  public MetadataRecordV2(NamespacedEntityId entityId, MetadataScope scope) {
    this(entityId.toMetadataEntity(), scope);
  }

  /**
   * Returns an empty {@link MetadataRecordV2} in the specified {@link MetadataScope}.
   */
  public MetadataRecordV2(MetadataEntity metadataEntity, MetadataScope scope) {
    this(metadataEntity, scope, Collections.emptyMap(), Collections.emptySet());
  }

  /**
   * Returns a new {@link MetadataRecordV2} from the specified existing {@link MetadataRecordV2}.
   */
  public MetadataRecordV2(MetadataRecordV2 other) {
    this(other.getMetadataEntity(), other.getScope(), other.getProperties(), other.getTags());
  }

  /**
   * Returns an empty {@link MetadataRecordV2} in the specified {@link MetadataScope}.
   */
  public MetadataRecordV2(NamespacedEntityId entityId, MetadataScope scope, Map<String, String> properties,
                          Set<String> tags) {
    this(entityId.toMetadataEntity(), scope, properties, tags);
  }

  public MetadataRecordV2(MetadataEntity metadataEntity, MetadataScope scope, Map<String, String> properties,
                          Set<String> tags) {
    this.metadataEntity = metadataEntity;
    this.scope = scope;
    this.properties = properties;
    this.tags = tags;
  }

  public MetadataEntity getMetadataEntity() {
    return metadataEntity;
  }

  public NamespacedEntityId getEntityId() {
    return EntityId.fromMetadataEntity(metadataEntity);
  }

  public MetadataScope getScope() {
    return scope;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Set<String> getTags() {
    return tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MetadataRecordV2 that = (MetadataRecordV2) o;

    return Objects.equals(metadataEntity, that.metadataEntity) &&
      scope == that.scope &&
      Objects.equals(properties, that.properties) &&
      Objects.equals(tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metadataEntity, scope, properties, tags);
  }

  @Override
  public String toString() {
    return "MetadataRecordV2{" +
      "metadataEntity=" + metadataEntity +
      ", scope=" + scope +
      ", properties=" + properties +
      ", tags=" + tags +
      '}';
  }

  /**
   * @return the deprecated {@link MetadataRecord} for this {@link MetadataRecordV2}.
   */
  public MetadataRecord getMetadataRecord() {
    return new MetadataRecord(EntityId.fromMetadataEntity(metadataEntity), scope, properties, tags);
  }
}
