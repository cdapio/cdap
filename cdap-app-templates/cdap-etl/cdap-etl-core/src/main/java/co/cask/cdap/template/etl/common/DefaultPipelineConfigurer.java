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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.api.templates.plugins.PluginSelector;
import co.cask.cdap.template.etl.api.PipelineConfigurer;

import javax.annotation.Nullable;

/**
 * Configurer for a pipeline, that delegates all operations to an AdapterConfigurer.
 */
public class DefaultPipelineConfigurer implements PipelineConfigurer {
  private final AdapterConfigurer adapterConfigurer;
  private final String pluginPrefix;

  public DefaultPipelineConfigurer(AdapterConfigurer adapterConfigurer, String pluginPrefix) {
    this.adapterConfigurer = adapterConfigurer;
    this.pluginPrefix = pluginPrefix;
  }

  @Override
  public void addStream(Stream stream) {
    adapterConfigurer.addStream(stream);
  }

  @Override
  public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    adapterConfigurer.addDatasetModule(moduleName, moduleClass);
  }

  @Override
  public void addDatasetType(Class<? extends Dataset> datasetClass) {
    adapterConfigurer.addDatasetType(datasetClass);
  }

  @Override
  public void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    adapterConfigurer.createDataset(datasetName, typeName, properties);
  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props) {
    adapterConfigurer.createDataset(datasetName, datasetClass, props);
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties) {
    return adapterConfigurer.usePlugin(pluginType, pluginName, getPluginId(pluginId), properties);
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                         PluginSelector selector) {
    return adapterConfigurer.usePlugin(pluginType, pluginName, getPluginId(pluginId), properties, selector);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties properties) {
    return adapterConfigurer.usePluginClass(pluginType, pluginName, getPluginId(pluginId), properties);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                                     PluginSelector selector) {
    return adapterConfigurer.usePluginClass(pluginType, pluginName, getPluginId(pluginId), properties, selector);
  }
  
  private String getPluginId(String childPluginId) {
    return String.format("%s%s%s", pluginPrefix, Constants.ID_SEPARATOR, childPluginId);
  }
}
