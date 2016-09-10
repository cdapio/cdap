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

import co.cask.cdap.proto.id.NamespacedEntityId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the complete metadata of a {@link NamespacedEntityId} including its properties and tags.
 */
public class Metadata {
  private final NamespacedEntityId namespacedEntityId;
  private final Map<String, String> properties;
  private final Set<String> tags;

  /**
   * Returns an empty {@link Metadata}
   */
  public Metadata(NamespacedEntityId namespacedEntityId) {
    this(namespacedEntityId, ImmutableMap.<String, String>of(), ImmutableSet.<String>of());
  }

  public Metadata(NamespacedEntityId namespacedEntityId, Map<String, String> properties, Set<String> tags) {
    this.namespacedEntityId = namespacedEntityId;
    this.properties = properties;
    this.tags = tags;
  }

  public NamespacedEntityId getEntityId() {
    return namespacedEntityId;
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

    return Objects.equals(namespacedEntityId, that.namespacedEntityId) &&
      Objects.equals(properties, that.properties) &&
      Objects.equals(tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespacedEntityId, properties, tags);
  }

  @Override
  public String toString() {
    return "Metadata{" +
      "namespacedEntityId=" + namespacedEntityId +
      ", properties=" + properties +
      ", tags=" + tags +
      '}';
  }
}
