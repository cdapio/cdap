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

package co.cask.cdap.api.templates;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.api.templates.plugins.PluginSelector;

import javax.annotation.Nullable;

/**
 * This interface provides method to register plugin usage for an adapter. The registered plugins
 * will be available at runtime of the adapter.
 */
@Beta
public interface AdapterPluginRegistry {

  /**
   * Adds a Plugin usage to the Adapter and create a new instance.
   * The Plugin will be accessible at execution time via the {@link AdapterContext}.
   *
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param pluginId an unique identifier for this usage. The same id is used to get the plugin at execution time.
   * @param properties properties for the plugin. The same set of properties will be used to instantiate the plugin
   *                   instance at execution time
   * @param <T> type of the plugin class
   * @return A new instance of the plugin class or {@code null} if no plugin was found
   */
  @Nullable
  <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties);

  /**
   * Adds a Plugin usage to the Adapter and create a new instance.
   * The Plugin will be accessible at execution time via the {@link AdapterContext}.
   *
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param pluginId an unique identifier for this usage. The same id is used to get the plugin at execution time.
   * @param properties properties for the plugin. The same set of properties will be used to instantiate the plugin
   *                   instance at execution time
   * @param selector for selecting which plugin to use
   * @param <T> type of the plugin class
   * @return A new instance of the plugin class or {@code null} if no plugin was found
   */
  @Nullable
  <T> T usePlugin(String pluginType, String pluginName,
                  String pluginId, PluginProperties properties, PluginSelector selector);

  /**
   * Adds a Plugin usage to the Adapter.
   * The Plugin will be accessible at execution time via the {@link AdapterContext}.
   *
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param pluginId an unique identifier for this usage. The same id is used to get the plugin at execution time.
   * @param properties properties for the plugin. The same set of properties will be used to instantiate the plugin
   *                   instance at execution time
   * @param <T> type of the plugin class
   * @return A {@link Class} for the plugin class or {@code null} if no plugin was found
   */
  @Nullable
  <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties);

  /**
   * Adds a Plugin usage to the Adapter.
   * The Plugin will be accessible at execution time via the {@link AdapterContext}.
   *
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param pluginId an unique identifier for this usage. The same id is used to get the plugin at execution time.
   * @param properties properties for the plugin. The same set of properties will be used to instantiate the plugin
   *                   instance at execution time
   * @param selector for selecting which plugin to use
   * @param <T> type of the plugin class
   * @return A {@link Class} for the plugin class or {@code null} if no plugin was found
   */
  @Nullable
  <T> Class<T> usePluginClass(String pluginType, String pluginName,
                              String pluginId, PluginProperties properties, PluginSelector selector);
}
