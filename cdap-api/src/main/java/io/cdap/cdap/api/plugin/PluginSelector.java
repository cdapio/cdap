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

package io.cdap.cdap.api.plugin;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.artifact.ArtifactId;

import java.util.Map;
import java.util.SortedMap;
import javax.annotation.Nullable;

/**
 * Represents a class for selecting a plugin.
 */
@Beta
public class PluginSelector {

  /**
   * Selects a plugin. The default implementation returns the last entry in the given map.
   *
   * @param plugins the set of available plugins. The {@link ArtifactId} is sorted in ascending order of plugin JAR
   *        name followed by the plugin version.
   * @return a {@link java.util.Map.Entry} for the selected plugin, or null if nothing is selected
   */
  @Nullable
  public Map.Entry<ArtifactId, PluginClass> select(SortedMap<ArtifactId, PluginClass> plugins) {
    return plugins.tailMap(plugins.lastKey()).entrySet().iterator().next();
  }
}
