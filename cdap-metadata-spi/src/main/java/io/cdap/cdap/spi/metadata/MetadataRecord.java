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

package io.cdap.cdap.spi.metadata;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.metadata.MetadataEntity;

import java.util.Objects;

/**
 * An entity with its metadata.
 */
@Beta
public class MetadataRecord {
  private final MetadataEntity entity;
  private final Metadata metadata;

  public MetadataRecord(MetadataEntity entity, Metadata metadata) {
    this.entity = entity;
    this.metadata = metadata;
  }

  public MetadataEntity getEntity() {
    return entity;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetadataRecord that = (MetadataRecord) o;
    return Objects.equals(entity, that.entity) &&
      Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entity, metadata);
  }

  @Override
  public String toString() {
    return "MetadataRecord{" +
      "entity=" + entity +
      ", metadata=" + metadata +
      '}';
  }
}
