/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the complete metadata of a {@link Id.NamespacedId} including its properties and tags.
 */
public class Metadata {
  private final Id.NamespacedId entityId;
  private final Map<String, String> properties;
  private final Set<String> tags;

  /**
   * Returns an empty {@link Metadata}
   */
  public Metadata(Id.NamespacedId entityId) {
    this(entityId, ImmutableMap.<String, String>of(), ImmutableSet.<String>of());
  }

  public Metadata(Id.NamespacedId entityId, Map<String, String> properties, Set<String> tags) {
    this.entityId = entityId;
    this.properties = properties;
    this.tags = tags;
  }

  public Id.NamespacedId getEntityId() {
    return entityId;
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

    Metadata that = (Metadata) o;

    return Objects.equals(entityId, that.entityId) &&
      Objects.equals(properties, that.properties) &&
      Objects.equals(tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityId, properties, tags);
  }

  @Override
  public String toString() {
    return "Metadata{" +
      "entityId=" + entityId +
      ", properties=" + properties +
      ", tags=" + tags +
      '}';
  }
}
