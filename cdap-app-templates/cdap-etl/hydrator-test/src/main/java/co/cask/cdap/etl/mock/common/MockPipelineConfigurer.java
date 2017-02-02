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

package co.cask.cdap.etl.mock.common;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Mock test configurer used to test configure pipeline's validation of input schema and setting of output shcema.
 */
public class MockPipelineConfigurer implements PipelineConfigurer {
  private final Schema inputSchema;
  Schema outputSchema;
  private Map<String, Object> plugins;

  /**
   * Creates a mock pipeline configurer with a map of plugins to use for validation.
   * @param inputSchema The input schema to use for validation
   * @param plugins A map from plugin ID strings to plugin objects to use
   */
  public MockPipelineConfigurer(Schema inputSchema, Map<String, Object> plugins) {
    this.inputSchema = inputSchema;
    this.plugins = plugins;
  }

  public MockPipelineConfigurer(Schema inputSchema) {
    this(inputSchema, Collections.<String, Object>emptyMap());
  }

  @Nullable
  public Schema getOutputSchema() {
    return outputSchema;
  }

  @Override
  public StageConfigurer getStageConfigurer() {
    return new StageConfigurer() {
      @Nullable
      @Override
      public Schema getInputSchema() {
        return inputSchema;
      }

      @Override
      public void setOutputSchema(@Nullable Schema schema) {
        outputSchema = schema;
      }

      @Override
      public void setErrorSchema(@Nullable Schema errorSchema) {

      }
    };
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties pluginProperties) {
    return (T) plugins.get(pluginId);
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId,
                         PluginProperties pluginProperties, PluginSelector pluginSelector) {
    return (T) plugins.get(pluginId);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties pluginProperties) {
    return (Class<T>) plugins.get(pluginId).getClass();
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties pluginProperties, PluginSelector pluginSelector) {
    return (Class<T>) plugins.get(pluginId).getClass();
  }

  @Override
  public void addStream(Stream stream) {

  }

  @Override
  public void addStream(String s) {

  }

  @Override
  public void addDatasetModule(String s, Class<? extends DatasetModule> aClass) {

  }

  @Override
  public void addDatasetType(Class<? extends Dataset> aClass) {

  }

  @Override
  public void createDataset(String s, String s1, DatasetProperties datasetProperties) {

  }

  @Override
  public void createDataset(String s, String s1) {

  }

  @Override
  public void createDataset(String s, Class<? extends Dataset> aClass, DatasetProperties datasetProperties) {

  }

  @Override
  public void createDataset(String s, Class<? extends Dataset> aClass) {

  }
}

