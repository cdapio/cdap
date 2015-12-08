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

package co.cask.cdap.data2.metadata.system;

import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataScope;

import java.util.Map;

/**
 * A class to write {@link MetadataScope#SYSTEM} metadata for an {@link Id.NamespacedId entity}.
 */
public abstract class AbstractSystemMetadataWriter {

  public static final char CTRL_A = '\001';
  public static final String SCHEMA_FIELD_PROPERTY_PREFIX = "schema";

  private final MetadataStore metadataStore;
  private final Id.NamespacedId entityId;

  public AbstractSystemMetadataWriter(MetadataStore metadataStore, Id.NamespacedId entityId) {
    this.metadataStore = metadataStore;
    this.entityId = entityId;
  }

  /**
   * Define the {@link MetadataScope#SYSTEM system} metadata properties to add for this entity.
   *
   * @return A {@link Map} of properties to add to this {@link Id.NamespacedId entity} in {@link MetadataScope#SYSTEM}
   */
  abstract Map<String, String> getSystemPropertiesToAdd();

  /**
   * Define the {@link MetadataScope#SYSTEM system} tags to add for this entity.
   *
   * @return an array of tags to add to this {@link Id.NamespacedId entity} in {@link MetadataScope#SYSTEM}
   */
  abstract String[] getSystemTagsToAdd();

  /**
   * Updates the {@link MetadataScope#SYSTEM} metadata for this {@link Id.NamespacedId entity}.
   */
  public void write() {
    Map<String, String> properties = getSystemPropertiesToAdd();
    if (properties.size() > 0) {
      metadataStore.setProperties(MetadataScope.SYSTEM, entityId, properties);
    }
    String[] tags = getSystemTagsToAdd();
    if (tags.length > 0) {
      metadataStore.addTags(MetadataScope.SYSTEM, entityId, tags);
    }
  }
}
