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

package co.cask.cdap.api.plugin;

import co.cask.cdap.api.annotation.Beta;

import javax.annotation.Nullable;

/**
 * Provides access to load plugin class from plugin endpoint methods.
 */
@Beta
public interface EndpointPluginContext {
  /**
   * loads and returns a plugin class.
   *
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param pluginProperties properties for the plugin.
   *                         The same set of properties will be used to instantiate the plugin
   *                         instance at execution time
   * @param <T> type of the plugin class
   * @return A {@link Class} for the plugin class or {@code null} if no plugin was found
   */
  @Nullable <T> Class<T> loadPluginClass(String pluginType, String pluginName,
                                         PluginProperties pluginProperties);

  /**
   * loads and returns a plugin class.
   *
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param pluginProperties properties for the plugin.
   *                         The same set of properties will be used to instantiate the plugin
   *                         instance at execution time
   * @param pluginSelector class for selecting plugin
   * @param <T> type of the plugin class
   * @return A {@link Class} for the plugin class or {@code null} if no plugin was found
   */
  @Nullable <T> Class<T> loadPluginClass(String pluginType, String pluginName,
                                         PluginProperties pluginProperties, PluginSelector pluginSelector);

  /**
   * loads and returns a plugin class.
   *
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param <T> type of the plugin class
   * @return A {@link Class} for the plugin class or {@code null} if no plugin was found
   */
  @Nullable <T> Class<T> loadPluginClass(String pluginType, String pluginName);
}
