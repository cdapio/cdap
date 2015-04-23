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
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.api.templates.plugins.PluginProperties;

import javax.annotation.Nullable;

/**
 * Provides access to template adapter context when a program is executing as an adapter.
 */
@Beta
public interface AdapterContext {

  /**
   * Returns the {@link AdapterSpecification} containing information about the adapter or {@code null} if the
   * program is not running under the adapter context.
   */
  @Nullable
  AdapterSpecification getAdapterSpecification();

  /**
   * Gets the {@link PluginProperties} associated with the given plugin type and name in the adapter context.
   *
   * @param pluginId the unique identifier provide when declaring plugin usage in {@link AdapterConfigurer}.
   * @return the {@link PluginProperties}.
   * @throws IllegalArgumentException if no plugin for the given type and name
   * @throws UnsupportedOperationException if the program is not running under the adapter context
   */
  PluginProperties getPluginProperties(String pluginId);

  /**
   * Loads and returns a plugin class as specified by the given type and name.
   *
   * @param pluginId the unique identifier provide when declaring plugin usage in {@link AdapterConfigurer}.
   * @param <T> the class type of the plugin
   * @return the resulting plugin {@link Class}.
   * @throws IllegalArgumentException if no plugin for the given type and name
   * @throws UnsupportedOperationException if the program is not running under the adapter context
   */
  <T> Class<T> loadPluginClass(String pluginId);

  /**
   * Creates a new instance of a plugin. The instance returned will have the {@link PluginConfig} setup with
   * {@link PluginProperties} provided at the time when the
   * {@link AdapterConfigurer#usePlugin(String, String, String, PluginProperties)} was called during the
   * {@link ApplicationTemplate#configureAdapter(String, Object, AdapterConfigurer)} time.
   *
   * @param pluginId the unique identifier provide when declaring plugin usage in {@link AdapterConfigurer}.
   * @param <T> the class type of the plugin
   * @return A new instance of the plugin being specified by the arguments
   *
   * @throws InstantiationException if failed create a new instance.
   * @throws UnsupportedOperationException if the program is not running under the adapter context
   * @throws IllegalArgumentException if no plugin for the given type and name
   */
  <T> T newPluginInstance(String pluginId) throws InstantiationException;
}
