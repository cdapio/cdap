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

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.templates.AdapterSpecification;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.templates.etl.api.StageContext;
import co.cask.cdap.templates.etl.common.Constants;

import javax.annotation.Nullable;

/**
 * Context for the Transform Stage.
 */
public class BatchStageContext implements StageContext {
  private final MapReduceContext context;
  private final Metrics metrics;
  private final String pluginPrefix;

  public BatchStageContext(MapReduceContext context, Metrics metrics, String pluginPrefix) {
    this.context = context;
    this.metrics = metrics;
    this.pluginPrefix = pluginPrefix;
  }

  @Override
  public Metrics getMetrics() {
    return metrics;
  }

  @Override
  public PluginProperties getPluginProperties() {
    return context.getPluginProperties(pluginPrefix);
  }

  @Nullable
  @Override
  public AdapterSpecification getAdapterSpecification() {
    return context.getAdapterSpecification();
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return context.getPluginProperties(getPluginId(pluginId));
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return context.loadPluginClass(getPluginId(pluginId));
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return context.newPluginInstance(getPluginId(pluginId));
  }

  private String getPluginId(String childPluginId) {
    return String.format("%s%s%s", pluginPrefix, Constants.ID_SEPARATOR, childPluginId);
  }
}
