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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.etl.api.PipelineConfigurer;

import javax.annotation.Nullable;

/**
 * Configurer for a pipeline, that delegates all operations to a PluginConfigurer, except it prefixes plugin ids
 * to provide isolation for each etl stage. For example, a source can use a plugin with id 'jdbcdriver' and
 * a sink can also use a plugin with id 'jdbcdriver' without clobbering each other.
 */
public class DefaultPipelineConfigurer implements PipelineConfigurer {
  private final PluginConfigurer configurer;
  private final String pluginPrefix;

  public DefaultPipelineConfigurer(PluginConfigurer configurer, String pluginPrefix) {
    this.configurer = configurer;
    this.pluginPrefix = pluginPrefix;
  }

  @Override
  public void addStream(Stream stream) {
    configurer.addStream(stream);
  }

  @Override
  public void addStream(String streamName) {
    configurer.addStream(streamName);
  }

  @Override
  public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    configurer.addDatasetModule(moduleName, moduleClass);
  }

  @Override
  public void addDatasetType(Class<? extends Dataset> datasetClass) {
    configurer.addDatasetType(datasetClass);
  }

  @Override
  public void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    configurer.createDataset(datasetName, typeName, properties);
  }

  @Override
  public void createDataset(String datasetName, String typeName) {
    configurer.createDataset(datasetName, typeName);
  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props) {
    configurer.createDataset(datasetName, datasetClass, props);
  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass) {
    configurer.createDataset(datasetName, datasetClass);
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties) {
    return configurer.usePlugin(pluginType, pluginName, getPluginId(pluginId), properties);
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                         PluginSelector selector) {
    return configurer.usePlugin(pluginType, pluginName, pluginId, properties, selector);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties properties) {
    return configurer.usePluginClass(pluginType, pluginName, getPluginId(pluginId), properties);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                                     PluginSelector selector) {
    return configurer.usePluginClass(pluginType, pluginName, pluginId, properties, selector);
  }
  
  private String getPluginId(String childPluginId) {
    return String.format("%s%s%s", pluginPrefix, Constants.ID_SEPARATOR, childPluginId);
  }
}
