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

package io.cdap.cdap.api.plugin;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Provides access to plugin context when a program is executing.
 */
@Beta
public interface PluginContext {

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
  default <T> Class<T> loadClass(String pluginType, String pluginName, String pluginId,
                          PluginProperties properties) throws IOException, ClassNotFoundException {
    throw new UnsupportedOperationException("");
  }

  /**
   * Gets the {@link PluginProperties} associated with the given plugin id.
   *
   * @param pluginId the unique identifier provide when declaring plugin usage in the program.
   * @return the {@link PluginProperties}.
   * @throws IllegalArgumentException if pluginId is not found
   * @throws UnsupportedOperationException if the program does not support plugin
   * @throws InvalidPluginConfigException if the plugin config could not be created from the given properties
   */
  PluginProperties getPluginProperties(String pluginId);

  /**
   * Gets the {@link PluginProperties} associated with the given plugin id. If a plugin field has a macro,
   * the parameter evaluator is used to evaluate the macro and the evaluated value is used for the plugin field.
   *
   * @param pluginId the unique identifier provide when declaring plugin usage in the program.
   * @param evaluator the macro evaluator that's used to evaluate macro for plugin field
   *                  if macro is supported on those fields.
   * @return the macro evaluated {@link PluginProperties}.
   * @throws IllegalArgumentException if pluginId is not found
   * @throws UnsupportedOperationException if the program does not support plugin
   * @throws InvalidMacroException if there is an exception during macro evaluation
   * @throws InvalidPluginConfigException if the plugin config could not be created from the given properties
   */
  PluginProperties getPluginProperties(String pluginId, MacroEvaluator evaluator) throws InvalidMacroException;

  /**
   * Loads and returns a plugin class as specified by the given plugin id.
   *
   * @param pluginId the unique identifier provide when declaring plugin usage in the program.
   * @param <T> the class type of the plugin
   * @return the resulting plugin {@link Class}.
   * @throws IllegalArgumentException if pluginId is not found
   * @throws UnsupportedOperationException if the program does not support plugin
   * @throws InvalidPluginConfigException if the plugin config could not be created from the given properties
   */
  <T> Class<T> loadPluginClass(String pluginId);

  /**
   * Creates a new instance of a plugin. The instance returned will have the {@link PluginConfig} setup with
   * {@link PluginProperties} provided at the time when the
   * {@link PluginConfigurer#usePlugin(String, String, String, PluginProperties)} was called during the
   * program configuration time.
   *
   * @param pluginId the unique identifier provide when declaring plugin usage in the program.
   * @param <T> the class type of the plugin
   * @return A new instance of the plugin being specified by the arguments
   *
   * @throws InstantiationException if failed create a new instance
   * @throws IllegalArgumentException if pluginId is not found
   * @throws UnsupportedOperationException if the program does not support plugin
   * @throws InvalidPluginConfigException if the plugin config could not be created from the given properties
   */
  <T> T newPluginInstance(String pluginId) throws InstantiationException;

  /**
   * Creates a new instance of a plugin. The instance returned will have the {@link PluginConfig} setup with
   * {@link PluginProperties} provided at the time when the
   * {@link PluginConfigurer#usePlugin(String, String, String, PluginProperties)} was called during the
   * program configuration time. If a plugin field has a macro,
   * the parameter evaluator is used to evaluate the macro and the evaluated value is used for the plugin field.
   *
   * @param pluginId the unique identifier provide when declaring plugin usage in the program.
   * @param evaluator the macro evaluator that's used to evaluate macro for plugin field
   *                  if macro is supported on those fields.
   * @param <T> the class type of the plugin
   * @return A new instance of the plugin being specified by the arguments
   *
   * @throws InstantiationException if failed create a new instance
   * @throws IllegalArgumentException if pluginId is not found
   * @throws UnsupportedOperationException if the program does not support plugin
   * @throws InvalidMacroException if there is an exception during macro evaluation
   * @throws InvalidPluginConfigException if the plugin config could not be created from the given properties
   */
  <T> T newPluginInstance(String pluginId, MacroEvaluator evaluator) throws InstantiationException,
    InvalidMacroException;
}
