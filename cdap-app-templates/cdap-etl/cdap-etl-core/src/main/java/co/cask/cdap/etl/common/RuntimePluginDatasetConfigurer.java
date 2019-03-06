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
 */

package co.cask.cdap.etl.common;


import co.cask.cdap.api.Admin;
import co.cask.cdap.api.DatasetConfigurer;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginSelector;

import javax.annotation.Nullable;

/**
 * A PluginConfigurer and DatasetConfigurer that is implemented using runtime contexts.
 * This is used when a pipeline run starts in order to validate and propagate schema in the pipeline
 * now that macros have been evaluated. Assumes that any plugins "registered" here were registered when the pipeline
 * was originally configured.
 * This should be given to plugins when they run their configurePipeline() methods. This is because
 * plugins used by a plugin get scoped by the stage name to prevent clashes with each other.
 * See {@link DefaultPipelineConfigurer}.
 */
public class RuntimePluginDatasetConfigurer implements DatasetConfigurer, PluginConfigurer {
  private final String stageName;
  private final Admin admin;
  private final PluginContext pluginContext;
  private final MacroEvaluator macroEvaluator;

  public RuntimePluginDatasetConfigurer(String stageName, Admin admin, PluginContext pluginContext,
                                        MacroEvaluator macroEvaluator) {
    this.stageName = stageName;
    this.admin = admin;
    this.pluginContext = pluginContext;
    this.macroEvaluator = macroEvaluator;
  }

  @Override
  public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    throw new UnsupportedOperationException("Dataset modules cannot be added in pipelines.");
  }

  @Override
  public void addDatasetType(Class<? extends Dataset> datasetClass) {
    throw new UnsupportedOperationException("Dataset types cannot be added in pipelines.");
  }

  @Override
  public void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    try {
      if (!admin.datasetExists(datasetName)) {
        admin.createDataset(datasetName, typeName, properties);
      }
    } catch (InstanceConflictException e) {
      // if it already exists, treat it like we were able to create it
      // this can happen if multiple pipelines try to create the same dataset at the same time
    } catch (DatasetManagementException e) {
      throw new RuntimeException(
        String.format("Unable to create dataset '%s' of type '%s'.", datasetName, typeName), e);
    }
  }

  @Override
  public void createDataset(String datasetName, String typeName) {
    createDataset(datasetName, typeName, DatasetProperties.EMPTY);
  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props) {
    createDataset(datasetName, datasetClass.getName(), props);
  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass) {
    createDataset(datasetName, datasetClass.getName(), DatasetProperties.EMPTY);
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                         PluginSelector selector) {
    try {
      return pluginContext.newPluginInstance(pluginId, macroEvaluator);
    } catch (IllegalArgumentException e) {
      // we expect this to have been registered with an actual PluginConfigurer at deploy time.
      // this can happen if somebody macro enables a plugin of a plugin or something like that.
      throw new IllegalArgumentException(
        String.format("Unable to create plugin '%s' in stage '%s'. Please ensure that all plugins of plugins are "
                        + "registered during pipeline deployment and not macro enabled.", pluginId, stageName), e);
    } catch (InstantiationException e) {
      throw new RuntimeException(String.format("Unable to create plugin for stage '%s'", stageName), e);
    }
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                                     PluginSelector selector) {
    try {
      return pluginContext.loadPluginClass(pluginId);
    } catch (IllegalArgumentException e) {
      // we expect this to have been registered with an actual PluginConfigurer at deploy time.
      // this can happen if somebody macro enables a plugin of a plugin or something like that.
      throw new IllegalArgumentException(
        String.format("Unable to create plugin '%s' in stage '%s'. Please ensure that all plugins of plugins are "
                        + "registered during pipeline deployment and not macro enabled.", pluginId, stageName), e);
    }
  }
}
