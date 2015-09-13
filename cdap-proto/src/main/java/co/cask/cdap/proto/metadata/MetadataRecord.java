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

import java.util.List;
import java.util.Map;

/**
 * Represents the complete metadata of a {@link Id.NamespacedId} including its properties and tags.
 */
public class MetadataRecord {
  private final Id.NamespacedId targetId;
  private final MetadataScope scope;
  private final Map<String, String> properties;
  private final List<String> tags;

  public MetadataRecord(Map<String, String> properties, List<String> tags, Id.NamespacedId targetId) {
    this(targetId, MetadataScope.USER, properties, tags);
  }

  public MetadataRecord(Id.NamespacedId targetId, MetadataScope scope, Map<String, String> properties,
                        List<String> tags) {
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

  public List<String> getTags() {
    return tags;
  }
}
