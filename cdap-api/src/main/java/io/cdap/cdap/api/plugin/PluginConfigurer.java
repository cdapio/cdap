/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * This interface provides methods to register plugin usage in a CDAP Program. The registered plugins will be
 * available at the runtime of the Program. Additionally, it provides methods to evaluate the plugin properties if
 * it contains macro.
 */
public interface PluginConfigurer {
  /**
   * Adds a Plugin usage to the Application and create a new instance.
   * The Plugin will be accessible at execution time via the {@link PluginContext}.
   *
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param pluginId an unique identifier for this usage. The same id is used to get the plugin at execution time.
   * @param properties properties for the plugin. The same set of properties will be used to instantiate the plugin
   *                   instance at execution time
   * @param <T> type of the plugin class
   * @return A new instance of the plugin class or {@code null} if no plugin was found
   * @throws InvalidPluginConfigException if the plugin config could not be created from the given properties
   */
  @Nullable
  default <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties) {
    return usePlugin(pluginType, pluginName, pluginId, properties, new PluginSelector());
  }

  /**
   * Adds a Plugin usage to the Application and create a new instance.
   * The Plugin will be accessible at execution time via the {@link PluginContext}.
   *
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param pluginId an unique identifier for this usage. The same id is used to get the plugin at execution time.
   * @param properties properties for the plugin. The same set of properties will be used to instantiate the plugin
   *                   instance at execution time
   * @param selector for selecting which plugin to use
   * @param <T> type of the plugin class
   * @return A new instance of the plugin class or {@code null} if no plugin was found
   * @throws InvalidPluginConfigException if the plugin config could not be created from the given properties
   */
  @Nullable
  <T> T usePlugin(String pluginType, String pluginName,
                  String pluginId, PluginProperties properties, PluginSelector selector);

  /**
   * Adds a Plugin usage to the Application.
   * The Plugin will be accessible at execution time via the {@link PluginContext}.
   *
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param pluginId an unique identifier for this usage. The same id is used to get the plugin at execution time.
   * @param properties properties for the plugin. The same set of properties will be used to instantiate the plugin
   *                   instance at execution time
   * @param <T> type of the plugin class
   * @return A {@link Class} for the plugin class or {@code null} if no plugin was found
   * @throws InvalidPluginConfigException if the plugin config could not be created from the given properties
   */
  @Nullable
  default <T> Class<T> usePluginClass(String pluginType, String pluginName,
                                      String pluginId, PluginProperties properties) {
    return usePluginClass(pluginType, pluginName, pluginId, properties, new PluginSelector());
  }

  /**
   * Adds a Plugin usage to the Application.
   * The Plugin will be accessible at execution time via the {@link PluginContext}.
   *
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param pluginId an unique identifier for this usage. The same id is used to get the plugin at execution time.
   * @param properties properties for the plugin. The same set of properties will be used to instantiate the plugin
   *                   instance at execution time
   * @param selector for selecting which plugin to use
   * @param <T> type of the plugin class
   * @return A {@link Class} for the plugin class or {@code null} if no plugin was found
   * @throws InvalidPluginConfigException if the plugin config could not be created from the given properties
   */
  @Nullable
  <T> Class<T> usePluginClass(String pluginType, String pluginName,
                              String pluginId, PluginProperties properties, PluginSelector selector);

  /**
   * Evaluates lookup macros and macro functions using provided macro evaluator.
   *
   * @param properties key-value map of properties to evaluate
   * @param evaluator  macro evaluator to be used to evaluate macros
   * @return map of evaluated macros
   * @throws InvalidMacroException indicates that there is an invalid macro
   */
  default Map<String, String> evaluateMacros(Map<String, String> properties,
                                             MacroEvaluator evaluator) throws InvalidMacroException {
    return evaluateMacros(properties, evaluator, MacroParserOptions.DEFAULT);
  }

  /**
   * Evaluates macros using provided macro evaluator with the provided parsing options.
   *
   * @param properties key-value map of properties to evaluate
   * @param evaluator  macro evaluator to be used to evaluate macros
   * @param options    macro parsing options
   * @return map of evaluated macros
   * @throws InvalidMacroException indicates that there is an invalid macro
   */
  default Map<String, String> evaluateMacros(
    Map<String, String> properties, MacroEvaluator evaluator, MacroParserOptions options) throws InvalidMacroException {
    throw new UnsupportedOperationException("Evaluating macros is not supported.");
  }
}
