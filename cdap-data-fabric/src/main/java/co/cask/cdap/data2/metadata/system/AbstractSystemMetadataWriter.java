/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.metadata.MetadataScope;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A class to write {@link MetadataScope#SYSTEM} metadata for an {@link NamespacedEntityId entity}.
 */
public abstract class AbstractSystemMetadataWriter implements SystemMetadataWriter {

  @VisibleForTesting
  public static final String SCHEMA_KEY = "schema";
  @VisibleForTesting
  public static final String TTL_KEY = "ttl";
  @VisibleForTesting
  public static final String DESCRIPTION_KEY = "description";
  @VisibleForTesting
  public static final String ENTITY_NAME_KEY = "entity-name";
  @VisibleForTesting
  public static final String CREATION_TIME_KEY = "creation-time";
  @VisibleForTesting
  public static final String VERSION_KEY = "version";
  @VisibleForTesting
  public static final String EXPLORE_TAG = "explore";

  // The following system properties should not get removed on metadata update
  // since they are not part of entity properties
  private static final Set<String> PRESERVE_PROPERTIES = ImmutableSet.of(CREATION_TIME_KEY, DESCRIPTION_KEY);
  private static final String PLUGIN_KEY_PREFIX = "plugin";
  private static final String PLUGIN_VERSION_KEY_PREFIX = "plugin-version";

  private final MetadataStore metadataStore;
  private final NamespacedEntityId entityId;

  AbstractSystemMetadataWriter(MetadataStore metadataStore, NamespacedEntityId entityId) {
    this.metadataStore = metadataStore;
    this.entityId = entityId;
  }

  /**
   * Define the {@link MetadataScope#SYSTEM system} metadata properties to add for this entity.
   *
   * @return A {@link Map} of properties to add to this {@link NamespacedEntityId entity} in
   * {@link MetadataScope#SYSTEM}
   */
  protected abstract Map<String, String> getSystemPropertiesToAdd();

  /**
   * Define the {@link MetadataScope#SYSTEM system} tags to add for this entity.
   *
   * @return an array of tags to add to this {@link NamespacedEntityId entity} in {@link MetadataScope#SYSTEM}
   */
  protected abstract String[] getSystemTagsToAdd();

  /**
   * Define the {@link MetadataScope#SYSTEM system} schema to add for this entity.
   *
   * @return the schema as a {@link String}
   */
  @Nullable
  protected String getSchemaToAdd() {
    return null;
  }

  /**
   * Updates the {@link MetadataScope#SYSTEM} metadata for this {@link NamespacedEntityId entity}.
   */
  @Override
  public void write() {
    // Delete existing system metadata before writing new metadata
    Set<String> existingProperties = metadataStore.getProperties(MetadataScope.SYSTEM, entityId).keySet();
    Sets.SetView<String> removeProperties = Sets.difference(existingProperties, PRESERVE_PROPERTIES);
    if (!removeProperties.isEmpty()) {
      String[] propertiesArray = removeProperties.toArray(new String[removeProperties.size()]);
      metadataStore.removeProperties(MetadataScope.SYSTEM, entityId, propertiesArray);
    }
    metadataStore.removeTags(MetadataScope.SYSTEM, entityId);

    // Now add the new metadata. The properties that were preserved need to be provided to setProperties() so that
    // they also get indexed. 
    // First add any preserved properties that were not removed
    Map<String, String> allProperties =
      getPreserverdProperties(metadataStore.getProperties(MetadataScope.SYSTEM, entityId));
    // Add all the properties that need to be added
    allProperties.putAll(getSystemPropertiesToAdd());
    if (allProperties.size() > 0) {
      metadataStore.setProperties(MetadataScope.SYSTEM, entityId, allProperties);
    }
    String[] tags = getSystemTagsToAdd();
    if (tags.length > 0) {
      metadataStore.addTags(MetadataScope.SYSTEM, entityId, tags);
    }
    // store additional properties that we want to index separately
    // if there is schema property then set that while providing schema indexer
    String schema = getSchemaToAdd();
    if (!Strings.isNullOrEmpty(schema)) {
      metadataStore.setProperty(MetadataScope.SYSTEM, entityId, SCHEMA_KEY, schema);
    }
  }

  private Map<String, String> getPreserverdProperties(Map<String, String> existingProperties) {
    Map<String, String> allProperties = new HashMap<>();
    for (String preservedProperty : PRESERVE_PROPERTIES) {
      if (existingProperties.get(preservedProperty) != null) {
        allProperties.put(preservedProperty, existingProperties.get(preservedProperty));
      }
    }
    return allProperties;
  }

  void addPlugin(PluginClass pluginClass, @Nullable String version,
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
