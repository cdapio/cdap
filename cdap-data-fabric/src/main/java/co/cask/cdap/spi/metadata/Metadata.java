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

package co.cask.cdap.spi.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The metadata for an entity, consisting of tags and properties.
 */
public final class Metadata {

  private final Set<ScopedName> tags;
  private final Map<ScopedName, String> properties;

  public Metadata(Set<ScopedName> tags, Map<ScopedName, String> properties) {
    this.tags = ImmutableSet.copyOf(tags);
    this.properties = ImmutableMap.copyOf(properties);
  }

  public Set<ScopedName> getTags() {
    return tags;
  }

  public Map<ScopedName, String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Metadata metadata = (Metadata) o;
    return Objects.equals(tags, metadata.tags) &&
      Objects.equals(properties, metadata.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tags, properties);
  }

  @Override
  public String toString() {
    return "Metadata{" +
      "tags=" + tags +
      ", properties=" + properties +
      '}';
  }
}
