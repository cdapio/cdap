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
 * The change effected by an operation is represented by
 * the metadata before and after the operation.
 */
@Beta
public class MetadataChange {
  private final MetadataEntity entity;
  private final Metadata before;
  private final Metadata after;

  public MetadataChange(MetadataEntity entity, Metadata before, Metadata after) {
    this.entity = entity;
    this.before = before;
    this.after = after;
  }

  public MetadataEntity getEntity() {
    return entity;
  }

  public Metadata getBefore() {
    return before;
  }

  public Metadata getAfter() {
    return after;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetadataChange that = (MetadataChange) o;
    return Objects.equals(entity, that.entity) &&
      Objects.equals(before, that.before) &&
      Objects.equals(after, that.after);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entity, before, after);
  }

  @Override
  public String toString() {
    return "MetadataChange{" +
      "entity=" + entity +
      ", before=" + before +
      ", after=" + after +
      '}';
  }
}
