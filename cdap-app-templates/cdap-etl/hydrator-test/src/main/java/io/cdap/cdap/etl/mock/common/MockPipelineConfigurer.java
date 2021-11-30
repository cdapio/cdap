/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.mock.common;

import io.cdap.cdap.api.DatasetConfigurer;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Mock test configurer used to test configure pipeline's validation of input schema and setting of output shcema.
 */
public class MockPipelineConfigurer implements PipelineConfigurer, DatasetConfigurer {
  private final Schema inputSchema;
  Schema outputSchema;
  private Map<String, Object> plugins;
  private final FailureCollector collector;

  /**
   * Creates a mock pipeline configurer with a map of plugins to use for validation.
   * @param inputSchema The input schema to use for validation
   * @param plugins A map from plugin ID strings to plugin objects to use
   */
  public MockPipelineConfigurer(Schema inputSchema, Map<String, Object> plugins) {
    this.inputSchema = inputSchema;
    this.plugins = plugins;
    this.collector = new MockFailureCollector();
  }

  public MockPipelineConfigurer(Schema inputSchema) {
    this(inputSchema, Collections.emptyMap());
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
        // no-op
      }

      public FailureCollector getFailureCollector() {
        return collector;
      }
    };
  }

  @Override
  public Engine getEngine() {
    return Engine.SPARK;
  }

  @Override
  public void setPipelineProperties(Map<String, String> properties) {
    // no-op
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
                                     PluginProperties pluginProperties, PluginSelector pluginSelector) {
    return (Class<T>) plugins.get(pluginId).getClass();
  }

  @Override
  public Map<String, String> evaluateMacros(Map<String, String> properties, MacroEvaluator evaluator,
                                            MacroParserOptions options) throws InvalidMacroException {
    return properties;
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

  @Override
  public Map<String, String> getFeatureFlags() {
    return Collections.emptyMap();
  }
}

