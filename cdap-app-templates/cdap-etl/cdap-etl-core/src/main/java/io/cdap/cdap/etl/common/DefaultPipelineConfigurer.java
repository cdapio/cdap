/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.DatasetConfigurer;
import io.cdap.cdap.api.FeatureFlagsProvider;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.MultiInputPipelineConfigurer;
import io.cdap.cdap.etl.api.MultiInputStageConfigurer;
import io.cdap.cdap.etl.api.MultiOutputPipelineConfigurer;
import io.cdap.cdap.etl.api.MultiOutputStageConfigurer;
import io.cdap.cdap.etl.api.PipelineConfigurer;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Configurer for a pipeline, that delegates all operations to a PluginConfigurer, except it prefixes plugin ids
 * to provide isolation for each etl stage. For example, a source can use a plugin with id 'jdbcdriver' and
 * a sink can also use a plugin with id 'jdbcdriver' without clobbering each other.
 */
public class DefaultPipelineConfigurer implements PipelineConfigurer, MultiInputPipelineConfigurer,
  MultiOutputPipelineConfigurer {
  private final Engine engine;
  private final PluginConfigurer pluginConfigurer;
  private final DatasetConfigurer datasetConfigurer;
  private final String stageName;
  private final DefaultStageConfigurer stageConfigurer;
  private final Map<String, String> properties;
  private final Map<String, String> featureFlags;

  public <C extends PluginConfigurer & DatasetConfigurer & FeatureFlagsProvider> DefaultPipelineConfigurer(
    C configurer, String stageName, Engine engine) {
    this(configurer, stageName, engine, new DefaultStageConfigurer(stageName));
  }

  public <C extends PluginConfigurer & DatasetConfigurer & FeatureFlagsProvider> DefaultPipelineConfigurer(
    C configurer, String stageName, Engine engine, DefaultStageConfigurer stageConfigurer) {
    this(configurer, configurer, stageName, engine, stageConfigurer, configurer);
  }

  public DefaultPipelineConfigurer(PluginConfigurer pluginConfigurer, DatasetConfigurer datasetConfigurer,
                                   String stageName, Engine engine, DefaultStageConfigurer stageConfigurer,
                                   FeatureFlagsProvider  featureFlagsProvider) {
    this.pluginConfigurer = pluginConfigurer;
    this.datasetConfigurer = datasetConfigurer;
    this.stageName = stageName;
    this.stageConfigurer = stageConfigurer;
    this.engine = engine;
    this.properties = new HashMap<>();
    this.featureFlags = featureFlagsProvider.getFeatureFlags();
  }

  @Override
  public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    datasetConfigurer.addDatasetModule(moduleName, moduleClass);
  }

  @Override
  public void addDatasetType(Class<? extends Dataset> datasetClass) {
    datasetConfigurer.addDatasetType(datasetClass);
  }

  @Override
  public void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    datasetConfigurer.createDataset(datasetName, typeName, properties);
  }

  @Override
  public void createDataset(String datasetName, String typeName) {
    datasetConfigurer.createDataset(datasetName, typeName);
  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props) {
    datasetConfigurer.createDataset(datasetName, datasetClass, props);
  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass) {
    datasetConfigurer.createDataset(datasetName, datasetClass);
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties) {
    return pluginConfigurer.usePlugin(pluginType, pluginName, getPluginId(pluginId), properties);
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                         PluginSelector selector) {
    return pluginConfigurer.usePlugin(pluginType, pluginName, getPluginId(pluginId), properties, selector);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties properties) {
    return pluginConfigurer.usePluginClass(pluginType, pluginName, getPluginId(pluginId), properties);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                                     PluginSelector selector) {
    return pluginConfigurer.usePluginClass(pluginType, pluginName, getPluginId(pluginId), properties, selector);
  }

  @Override
  public Map<String, String> evaluateMacros(Map<String, String> properties, MacroEvaluator evaluator,
                                            MacroParserOptions options) throws InvalidMacroException {
    return pluginConfigurer.evaluateMacros(properties, evaluator, options);
  }

  private String getPluginId(String childPluginId) {
    return String.format("%s%s%s", stageName, Constants.ID_SEPARATOR, childPluginId);
  }

  @Override
  public DefaultStageConfigurer getStageConfigurer() {
    return stageConfigurer;
  }

  @Override
  public Engine getEngine() {
    return engine;
  }

  @Override
  public void setPipelineProperties(Map<String, String> properties) {
    this.properties.clear();
    this.properties.putAll(properties);
  }

  @Override
  public MultiInputStageConfigurer getMultiInputStageConfigurer() {
    return stageConfigurer;
  }

  @Override
  public MultiOutputStageConfigurer getMultiOutputStageConfigurer() {
    return stageConfigurer;
  }

  public Map<String, String> getPipelineProperties() {
    return properties;
  }

  public Map<String, String> getFeatureFlags() {
    return featureFlags;
  }
}
