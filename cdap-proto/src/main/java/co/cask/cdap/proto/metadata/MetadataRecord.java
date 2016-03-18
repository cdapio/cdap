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

package co.cask.cdap.proto.metadata;

import co.cask.cdap.proto.Id;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the complete metadata of a {@link Id.NamespacedId} including its properties, tags in a given
 * {@link MetadataScope}
 */
@Deprecated
public class MetadataRecord {
  private final Id.NamespacedId entityId;
  private final MetadataScope scope;
  private final Map<String, String> properties;
  private final Set<String> tags;

  /**
   * Returns an empty {@link MetadataRecord} in the specified {@link MetadataScope}.
   */
  public MetadataRecord(Id.NamespacedId entityId, MetadataScope scope) {
    this(entityId, scope, Collections.<String, String>emptyMap(), Collections.<String>emptySet());
  }

  /**
   * Returns a new {@link MetadataRecord} from the specified existing {@link MetadataRecord}.
   */
  public MetadataRecord(MetadataRecord other) {
    this(other.getEntityId(), other.getScope(), other.getProperties(), other.getTags());
  }

  /**
   * Returns a new {@link MetadataRecord} in {@link MetadataScope#USER}.
   * Deprecated as of 3.3.1. Please use {@link #MetadataRecord(Id.NamespacedId, MetadataScope, Map, Set)} instead.
   * TODO: CDAP-4866 - Remove in 3.5
   */
  @Deprecated
  public MetadataRecord(Id.NamespacedId entityId, Map<String, String> properties, Set<String> tags) {
    this(entityId, MetadataScope.USER, properties, tags);
  }

  public MetadataRecord(Id.NamespacedId entityId, MetadataScope scope, Map<String, String> properties,
                        Set<String> tags) {
    this.entityId = entityId;
    this.scope = scope;
    this.properties = properties;
    this.tags = tags;
  }

  public Id.NamespacedId getEntityId() {
    return entityId;
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

    return Objects.equals(entityId, that.entityId) &&
      scope == that.scope &&
      Objects.equals(properties, that.properties) &&
      Objects.equals(tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityId, scope, properties, tags);
  }

  @Override
  public String toString() {
    return "MetadataRecord{" +
      "entityId=" + entityId +
      ", scope=" + scope +
      ", properties=" + properties +
      ", tags=" + tags +
      '}';
  }
}
