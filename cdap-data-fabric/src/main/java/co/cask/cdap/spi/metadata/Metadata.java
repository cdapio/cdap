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

import co.cask.cdap.api.metadata.MetadataScope;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The metadata for an entity, consisting of tags and properties.
 */
public final class Metadata {

  public static final Metadata EMPTY = new Metadata(Collections.emptySet(), Collections.emptyMap());

  private final Set<ScopedName> tags;
  private final Map<ScopedName, String> properties;

  /**
   * Constructor from all tags and properties.
   *
   * @param tags the tags, qualified with their scopes
   * @param properties the property names, qualified with their scopes, and values
   */
  public Metadata(Set<ScopedName> tags, Map<ScopedName, String> properties) {
    this.tags = ImmutableSet.copyOf(tags);
    this.properties = ImmutableMap.copyOf(properties);
  }

  /**
   * Convenience constructor for a single scope.
   *
   * @param scope the scope for all metadata
   * @param tags the names of the tags for this scope
   * @param properties the property names and values for this scope
   */
  public Metadata(MetadataScope scope, Set<String> tags, Map<String, String> properties) {
    this.tags = tags.stream().map(tag -> new ScopedName(scope, tag)).collect(Collectors.toSet());
    this.properties = properties.entrySet().stream().collect(Collectors.toMap(
      entry -> new ScopedName(scope, entry.getKey()), Map.Entry::getValue));
  }

  /**
   * Convenience constructor for a single scope with properties only.
   *
   * @param scope the scope for all properties
   * @param properties the property names and values for this scope
   */
  public Metadata(MetadataScope scope, Map<String, String> properties) {
    this(scope, Collections.emptySet(), properties);
  }

  /**
   * Convenience constructor for a single scope with tags only.
   *
   * @param scope the scope for all tags
   * @param tags the tag names for this scope
   */
  public Metadata(MetadataScope scope, Set<String> tags) {
    this(scope, tags, Collections.emptyMap());
  }

  /**
   * @return all tags
   */
  public Set<ScopedName> getTags() {
    return tags;
  }

  /**
   * @return all tags for the given scope
   */
  public Set<String> getTags(MetadataScope scope) {
    return tags.stream()
      .filter(tag -> scope.equals(tag.getScope()))
      .map(ScopedName::getName)
      .collect(Collectors.toSet());
  }

  /**
   * @return all properties
   */
  public Map<ScopedName, String> getProperties() {
    return properties;
  }

  /**
   * @return all properties for a given scope
   */
  public Map<String, String> getProperties(MetadataScope scope) {
    return properties.entrySet().stream()
      .filter(entry -> scope.equals(entry.getKey().getScope()))
      .collect(Collectors.toMap(entry -> entry.getKey().getName(), Map.Entry::getValue));
  }

  /**
   * @return whether this metadata has any tags or properties
   */
  public boolean isEmpty() {
    return tags.isEmpty() && properties.isEmpty();
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
