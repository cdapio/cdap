/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.system;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.spi.metadata.MetadataConstants;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Provides metadata at system scope for a {@link MetadataEntity}.
 */
public interface SystemMetadataProvider {

  String VERSION_KEY = "version";

  String PLUGIN_KEY_PREFIX = "plugin";
  String PLUGIN_VERSION_KEY_PREFIX = "plugin-version";

  /**
   * Define the {@link MetadataScope#SYSTEM system} metadata properties to add for this entity.
   */
  Map<String, String> getSystemPropertiesToAdd();

  /**
   * Define the {@link MetadataScope#SYSTEM system} metadata tags to add for this entity.
   */
  default Set<String> getSystemTagsToAdd() {
    return Collections.emptySet();
  }

  /**
   * Define the {@link MetadataScope#SYSTEM system} schema to add for this entity.
   */
  @Nullable
  default String getSchemaToAdd() {
    return null;
  }

  static void addPlugin(PluginClass pluginClass, @Nullable String version,
                        ImmutableMap.Builder<String, String> properties) {
    String name = pluginClass.getName();
    String type = pluginClass.getType();
    // Need both name and type in the key because two plugins of different types could have the same name.
    // However, the composite of name + type is guaranteed to be unique
    properties.put(
      PLUGIN_KEY_PREFIX + MetadataConstants.KEYVALUE_SEPARATOR + name + MetadataConstants.KEYVALUE_SEPARATOR + type,
      name + MetadataConstants.KEYVALUE_SEPARATOR + type
    );
    if (version != null) {
      properties.put(
        PLUGIN_VERSION_KEY_PREFIX + MetadataConstants.KEYVALUE_SEPARATOR + name +
          MetadataConstants.KEYVALUE_SEPARATOR + type,
        name + MetadataConstants.KEYVALUE_SEPARATOR + version
      );
    }
  }
}
