/*
 * Copyright 2015 Cask Data, Inc.
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
package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespacedEntityId;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents a single Metadata entry for a CDAP Entity.
 */
public class MetadataEntry {
  private final MetadataEntity metadataEntity;
  private final String key;
  private final String value;
  private final String schema;

  public MetadataEntry(NamespacedEntityId targetId, String key, String value) {
    this(targetId, key, value, null);
  }

  public MetadataEntry(MetadataEntity metadataEntity, String key, String value, @Nullable String schema) {
    this.metadataEntity = metadataEntity;
    this.key = key;
    this.value = value;
    this.schema = schema;
  }

  public MetadataEntry(MetadataEntity metadataEntity, String key, String value) {
    this(metadataEntity, key, value, null);
  }

  //TODO: Remove this constructor when it is finalized that schema need not be stored separately
  public MetadataEntry(NamespacedEntityId targetId, String key, String value,
                       @Nullable String schema) {
    this(targetId.toMetadataEntity(), key, value, schema);
  }

  public MetadataEntity getMetadataEntity() {
    return metadataEntity;
  }

  /**
   * @return {@link NamespacedEntityId} to which the {@link MetadataEntry} belongs if it is a known cdap entity type,
   * for example datasets, applications etc. Custom resources likes fields etc cannot be converted into cdap
   * {@link NamespacedEntityId} and calling this for {@link MetadataEntry} associated with such resources will fail
   * with a {@link IllegalArgumentException}.
   * @throws IllegalArgumentException if the {@link MetadataEntry} belong to a custom cdap resource and not a known cdap
   * entity.
   */
  public NamespacedEntityId getTargetId() {
    return EntityId.fromMetadataEntity(metadataEntity);
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public String getSchema() {
    return schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MetadataEntry that = (MetadataEntry) o;

    return Objects.equals(metadataEntity, that.metadataEntity) &&
      Objects.equals(key, that.key) &&
      Objects.equals(value, that.value) &&
      Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metadataEntity, key, value, schema);
  }

  @Override
  public String toString() {
    return "MetadataEntry{" +
      "metadataEntity=" + metadataEntity +
      ", key='" + key + '\'' +
      ", value='" + value + '\'' +
      ", schema='" + schema + '\'' +
      '}';
  }
}
