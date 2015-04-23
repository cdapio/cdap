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

package co.cask.cdap.api.templates.plugins;

import co.cask.cdap.api.annotation.Beta;

import java.util.Map;
import java.util.SortedMap;

/**
 * Represents class for selecting plugin.
 */
@Beta
public class PluginSelector {

  /**
   * Selects a plugin. The default implementation returns the last entry in the given map.
   *
   * @param plugins set of available plugins. The {@link PluginInfo} is sorted in ascending order of plugin jar
   *                name followed by plugin version.
   * @return a {@link java.util.Map.Entry} for the selected plugin.
   */
  public Map.Entry<PluginInfo, PluginClass> select(SortedMap<PluginInfo, PluginClass> plugins) {
    return plugins.tailMap(plugins.lastKey()).entrySet().iterator().next();
  }
}
