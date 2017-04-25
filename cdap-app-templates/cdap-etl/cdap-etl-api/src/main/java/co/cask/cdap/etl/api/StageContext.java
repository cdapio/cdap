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

package co.cask.cdap.etl.api;

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.api.plugin.PluginProperties;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Context for a pipeline stage, providing access to information about the stage, metrics, and plugins.
 */
@Beta
public interface StageContext extends ServiceDiscoverer {

  /**
   * Gets the unique stage name of the transform, useful for setting the context of logging in transforms.
   *
   * @return stage name
   */
  String getStageName();

  /**
   * Get an instance of {@link StageMetrics}, used to collect metrics for this stage. Metrics emitted from one stage
   * are independent from metrics emitted in another.
   *
   * @return {@link StageMetrics} for collecting metrics
   */
  StageMetrics getMetrics();

  /**
   * Gets the {@link PluginProperties} associated with the stage.
   *
   * @return the {@link PluginProperties}.
   */
  PluginProperties getPluginProperties();

  /**
   * Gets the {@link PluginProperties} associated with the given plugin id.
   *
   * @param pluginId the unique identifier provide when declaring plugin usage in the program.
   * @return the {@link PluginProperties}.
   * @throws IllegalArgumentException if pluginId is not found
   * @throws UnsupportedOperationException if the program does not support plugin
   */
  PluginProperties getPluginProperties(String pluginId);

  /**
   * Loads and returns a plugin class as specified by the given plugin id.
   *
   * @param pluginId the unique identifier provide when declaring plugin usage in the program.
   * @param <T> the class type of the plugin
   * @return the resulting plugin {@link Class}.
   * @throws IllegalArgumentException if pluginId is not found
   * @throws UnsupportedOperationException if the program does not support plugin
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
   */
  <T> T newPluginInstance(String pluginId) throws InstantiationException;

  /**
   * Return the input schema for the stage. A null input schema indicates that the previous stages did not set a
   * concrete output schema when the pipeline was deployed. This can either mean the input schema is unknown, or
   * it can mean the schema is not constant.
   *
   * @return the input schema for the stage
   */
  @Nullable
  Schema getInputSchema();

  /**
   * Return the input schemas for the stage. The map key is the stage name and the map value is the schema from
   * that stage. A null input schema indicates that the stage did not set a concrete output schema when the pipeline
   * was deployed. This can either mean the input schema is unknown, or it can mean the schema is not constant.
   *
   * @return the map of input stage names to their schema
   */
  Map<String, Schema> getInputSchemas();

  /**
   * Return the output schema of the stage, as set by this stage when the pipeline was deployed. If none was set,
   * null will be returned. A null schema indicates that the schema is not known, or that the output schema is not
   * constant.
   *
   * @return the output schema of the stage
   */
  @Nullable
  Schema getOutputSchema();

  /**
   * Return the pipeline arguments for this run.
   *
   * @return the pipeline arguments for this run
   */
  Arguments getArguments();
}
