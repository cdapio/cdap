/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.api.service.http;

import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import javax.annotation.Nullable;

/**
 * Plugin configurer specifically for service. It provides additional functionality such as
 * instantiate the plugin with provided macro evaluator.
 */
public interface ServicePluginConfigurer extends PluginConfigurer {

  /**
   * Adds a Plugin usage to the Application and create a new instance. The Plugin will be accessible
   * at execution time via the {@link PluginContext}.
   *
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param pluginId an unique identifier for this usage. The same id is used to get the plugin
   *     at execution time.
   * @param properties properties for the plugin. The same set of properties will be used to
   *     instantiate the plugin instance at execution time
   * @param selector for selecting which plugin to use
   * @param macroEvaluator macro evaluator to be used to evaluate macros
   * @param options macro parsing options
   * @param <T> type of the plugin class
   * @return A new instance of the plugin class or {@code null} if no plugin was found
   * @throws InvalidPluginConfigException if the plugin config could not be created from the
   *     given properties
   */
  @Nullable
  <T> T usePlugin(String pluginType, String pluginName, String pluginId,
      PluginProperties properties,
      PluginSelector selector, MacroEvaluator macroEvaluator, MacroParserOptions options);

  /**
   * Creates a new instance of {@link ClassLoader} that contains this program classloader, all the
   * plugins export-package classloader instantiated by this configurer and system classloader in
   * that loading order. Currently this method is only supported in services.
   *
   * @return a combined classloader
   */
  default ClassLoader createClassLoader() {
    throw new UnsupportedOperationException(
        "Creating classloader for all plugins is not supported.");
  }
}
