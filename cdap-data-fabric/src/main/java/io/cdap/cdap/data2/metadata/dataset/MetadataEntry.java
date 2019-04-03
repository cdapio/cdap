/*
 * Copyright 2015-2018 Cask Data, Inc.
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
package io.cdap.cdap.data2.metadata.dataset;

import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.proto.id.NamespacedEntityId;

import java.util.Objects;

/**
 * Represents a single Metadata entry.
 */
public class MetadataEntry {
  private final MetadataEntity metadataEntity;
  private final String key;
  private final String value;

  public MetadataEntry(NamespacedEntityId targetId, String key, String value) {
    this(targetId.toMetadataEntity(), key, value);
  }

  public MetadataEntry(MetadataEntity metadataEntity, String key, String value) {
    this.metadataEntity = metadataEntity;
    this.key = key;
    this.value = value;
  }

  /**
   * @return {@link MetadataEntity} to which the {@link MetadataEntry} belongs
   */
  public MetadataEntity getMetadataEntity() {
    return metadataEntity;
  }

  /**
   * @return the key for the metadata
   */
  public String getKey() {
    return key;
  }

  /**
   * @return the value for the metadata
   */
  public String getValue() {
    return value;
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
      Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metadataEntity, key, value);
  }

  @Override
  public String toString() {
    return "MetadataEntry{" +
      "metadataEntity=" + metadataEntity +
      ", key='" + key + '\'' +
      ", value='" + value + '\'' +
      '}';
  }
}
