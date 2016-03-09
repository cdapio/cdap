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

package co.cask.cdap.api.plugin;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.artifact.ArtifactId;

import java.util.Map;
import java.util.SortedMap;

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
   * @return a {@link java.util.Map.Entry} for the selected plugin
   */
  public Map.Entry<ArtifactId, PluginClass> select(SortedMap<ArtifactId, PluginClass> plugins) {
    return plugins.tailMap(plugins.lastKey()).entrySet().iterator().next();
  }
}
