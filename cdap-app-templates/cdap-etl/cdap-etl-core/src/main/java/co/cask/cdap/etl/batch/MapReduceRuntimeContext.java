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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.common.PluginID;
import co.cask.cdap.etl.common.ScopedPluginContext;
import co.cask.cdap.etl.common.StageMetrics;

import java.util.Map;

/**
 * Batch runtime context that delegates most operations to MapReduceTaskContext. It also extends
 * {@link ScopedPluginContext} in order to provide plugin isolation between pipeline plugins. This means sources,
 * transforms, and sinks don't need to worry that plugins they use conflict with plugins other sources, transforms,
 * or sinks use.
 */
public class MapReduceRuntimeContext extends ScopedPluginContext implements BatchRuntimeContext {
  private final MapReduceTaskContext context;
  private final Metrics metrics;

  public MapReduceRuntimeContext(MapReduceTaskContext context, Metrics metrics, String stageId) {
    super(stageId);
    this.context = context;
    this.metrics = metrics;
  }

  @Override
  public Metrics getMetrics() {
    return new StageMetrics(metrics, PluginID.from(stageId));
  }

  @Override
  public int getStageId() {
    return PluginID.from(stageId).getStage();
  }

  @Override
  protected <T> T newScopedPluginInstance(String scopedPluginId) throws InstantiationException {
    return context.newPluginInstance(scopedPluginId);
  }

  @Override
  protected <T> Class<T> loadScopedPluginClass(String scopedPluginId) {
    return context.loadPluginClass(scopedPluginId);
  }

  @Override
  public PluginProperties getPluginProperties() {
    return context.getPluginProperties(stageId);
  }

  @Override
  public PluginProperties getScopedPluginProperties(String scopedPluginId) {
    return context.getPluginProperties(scopedPluginId);
  }

  @Override
  public long getLogicalStartTime() {
    return context.getLogicalStartTime();
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return context.getRuntimeArguments();
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return context.getDataset(name);
  }

  @Override
  public <T extends Dataset> T getDataset(String name,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    return context.getDataset(name, arguments);
  }
}
