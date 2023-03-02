/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.validation;

import io.cdap.cdap.api.DatasetConfigurer;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A configurer that is used for validation purposes. It can still instantiate plugins, but dataset
 * operations are ignored.
 */
public class ValidatingConfigurer implements PluginConfigurer, DatasetConfigurer,
    FeatureFlagsProvider {

  private final PluginConfigurer delegate;
  private final FeatureFlagsProvider featureFlagsProvider;

  public ValidatingConfigurer(PluginConfigurer delegate,
      FeatureFlagsProvider featureFlagsProvider) {
    this.delegate = delegate;
    this.featureFlagsProvider = featureFlagsProvider;
  }

  @Override
  public boolean isFeatureEnabled(String name) {
    return featureFlagsProvider.isFeatureEnabled(name);
  }

  @Override
  public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    // no-op
  }

  @Override
  public void addDatasetType(Class<? extends Dataset> datasetClass) {
    // no-op
  }

  @Override
  public void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    // no-op
  }

  @Override
  public void createDataset(String datasetName, String typeName) {
    // no-op
  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass,
      DatasetProperties props) {
    // no-op
  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass) {
    // no-op
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId,
      PluginProperties properties,
      PluginSelector selector) {
    return delegate.usePlugin(pluginType, pluginName, pluginId, properties, selector);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
      PluginProperties properties,
      PluginSelector selector) {
    return delegate.usePluginClass(pluginType, pluginName, pluginId, properties, selector);
  }

  @Override
  public Map<String, String> evaluateMacros(Map<String, String> properties,
      MacroEvaluator evaluator,
      MacroParserOptions options) throws InvalidMacroException {
    return delegate.evaluateMacros(properties, evaluator, options);
  }
}
