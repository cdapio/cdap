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

package co.cask.cdap.dq;

import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.mapreduce.MapReduceConfigurer;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.etl.api.PipelineConfigurer;

import javax.annotation.Nullable;

/**
 * Implementation of {@link PipelineConfigurer} that delegates to {@link MapReduceConfigurer}
 */
public class MapReducePipelineConfigurer implements PipelineConfigurer {
  private final MapReduceConfigurer mrConfigurer;
  private final String prefixId;

  public MapReducePipelineConfigurer(MapReduceConfigurer configurer, String prefixId) {
    this.mrConfigurer = configurer;
    this.prefixId = prefixId;
  }

  @Override
  public void addStream(Stream stream) {
    mrConfigurer.addStream(stream);
  }

  @Override
  public void addStream(String streamName) {
    mrConfigurer.addStream(streamName);
  }

  @Override
  public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    mrConfigurer.addDatasetModule(moduleName, moduleClass);
  }

  @Override
  public void addDatasetType(Class<? extends Dataset> datasetClass) {
    mrConfigurer.addDatasetType(datasetClass);
  }

  @Override
  public void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    mrConfigurer.createDataset(datasetName, typeName, properties);
  }

  @Override
  public void createDataset(String datasetName, String typeName) {
    mrConfigurer.createDataset(datasetName, typeName);
  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props) {
    mrConfigurer.createDataset(datasetName, datasetClass, props);
  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass) {
    mrConfigurer.createDataset(datasetName, datasetClass);
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties) {
    return mrConfigurer.usePlugin(pluginType, pluginName, prefixId + pluginId, properties);
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                         PluginSelector selector) {
    return mrConfigurer.usePlugin(pluginType, pluginName, prefixId + pluginId, properties, selector);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties properties) {
    return mrConfigurer.usePluginClass(pluginType, pluginName, prefixId + pluginId, properties);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                                     PluginSelector selector) {
    return mrConfigurer.usePluginClass(pluginType, pluginName, prefixId + pluginId, properties, selector);
  }
}
