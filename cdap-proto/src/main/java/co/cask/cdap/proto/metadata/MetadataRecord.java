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

package co.cask.cdap.proto.metadata;

import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the complete metadata of a {@link Id.NamespacedId} including its properties and tags.
 */
public class MetadataRecord {
  private final Id.NamespacedId targetId;
  private final MetadataScope scope;
  private final Map<String, String> properties;
  private final Set<String> tags;

  /**
   * Returns an empty {@link MetadataRecord}
   */
  public MetadataRecord(Id.NamespacedId targetId) {
    this(targetId, ImmutableMap.<String, String>of(), ImmutableSet.<String>of());
  }

  public MetadataRecord(MetadataRecord other) {
    this(other.getTargetId(), other.getProperties(), other.getTags());
  }

  public MetadataRecord(Id.NamespacedId targetId, Map<String, String> properties, Set<String> tags) {
    this(targetId, MetadataScope.USER, properties, tags);
  }

  public MetadataRecord(Id.NamespacedId targetId, MetadataScope scope, Map<String, String> properties,
                        Set<String> tags) {
    this.targetId = targetId;
    this.scope = scope;
    this.properties = properties;
    this.tags = tags;
  }

  public Id.NamespacedId getTargetId() {
    return targetId;
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

    MetadataRecord that = (MetadataRecord) o;

    return Objects.equals(targetId, that.targetId) &&
      scope == that.scope &&
      Objects.equals(properties, that.properties) &&
      Objects.equals(tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetId, scope, properties, tags);
  }

  @Override
  public String toString() {
    return "MetadataRecord{" +
      "targetId=" + targetId +
      ", scope=" + scope +
      ", properties=" + properties +
      ", tags=" + tags +
      '}';
  }
}
