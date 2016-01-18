/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.indexer.SchemaIndexer;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.MetadataScope;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * A class to write {@link MetadataScope#SYSTEM} metadata for an {@link Id.NamespacedId entity}.
 */
public abstract class AbstractSystemMetadataWriter {

  private static final String SCHEMA_FIELD_PROPERTY_PREFIX = "schema";
  private static final String PLUGIN_KEY_PREFIX = "plugin";
  private static final String PLUGIN_VERSION_KEY_PREFIX = "pluginversion";
  protected static final String TTL_KEY = "ttl";

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
   * Define the {@link MetadataScope#SYSTEM system} schema to add for this entity.
   *
   * @return the schema as a {@link String}
   */
  @Nullable
  String getSchemaToAdd() {
    return null;
  }

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
    // if there is schema property then set that while providing schema indexer
    if (!Strings.isNullOrEmpty(getSchemaToAdd())) {
      metadataStore.setProperties(MetadataScope.SYSTEM, entityId, ImmutableMap.of(SCHEMA_FIELD_PROPERTY_PREFIX,
                                                                                  getSchemaToAdd()),
                                  new SchemaIndexer());
    }
  }

  protected void addPlugin(PluginClass pluginClass, @Nullable String version,
                         ImmutableMap.Builder<String, String> properties) {
    String name = pluginClass.getName();
    String type = pluginClass.getType();
    // Need both name and type in the key because two plugins of different types could have the same name.
    // However, the composite of name + type is guaranteed to be unique
    properties.put(
      PLUGIN_KEY_PREFIX + MetadataDataset.KEYVALUE_SEPARATOR + name + MetadataDataset.KEYVALUE_SEPARATOR + type,
      name + MetadataDataset.KEYVALUE_SEPARATOR + type
    );
    if (version != null) {
      properties.put(
        PLUGIN_VERSION_KEY_PREFIX + MetadataDataset.KEYVALUE_SEPARATOR + name +
          MetadataDataset.KEYVALUE_SEPARATOR + type,
        name + MetadataDataset.KEYVALUE_SEPARATOR + version
      );
    }
  }
}
