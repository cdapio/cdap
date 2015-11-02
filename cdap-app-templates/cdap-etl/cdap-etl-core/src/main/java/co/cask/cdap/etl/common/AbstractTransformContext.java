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

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.TransformContext;

/**
 * Base implementation of {@link TransformContext} for common functionality.
 * This context scopes plugin ids by the id of the stage. This allows multiple transforms to use plugins with
 * the same id without clobbering each other.
 */
public abstract class AbstractTransformContext implements TransformContext {

  private final PluginContext pluginContext;
  private final String stageId;
  private final StageMetrics metrics;

  public AbstractTransformContext(PluginContext pluginContext, Metrics metrics, String stageId) {
    this.pluginContext = pluginContext;
    this.stageId = stageId;
    this.metrics = new DefaultStageMetrics(metrics, PluginID.from(stageId));
  }

  @Override
  public final PluginProperties getPluginProperties(String pluginId) {
    return pluginContext.getPluginProperties(scopePluginId(pluginId));
  }

  @Override
  public final <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return pluginContext.newPluginInstance(scopePluginId(pluginId));
  }

  @Override
  public final <T> Class<T> loadPluginClass(String pluginId) {
    return pluginContext.loadPluginClass(scopePluginId(pluginId));
  }

  @Override
  public final PluginProperties getPluginProperties() {
    return pluginContext.getPluginProperties(stageId);
  }

  @Override
  public final int getStageId() {
    return PluginID.from(stageId).getStage();
  }

  @Override
  public final StageMetrics getMetrics() {
    return metrics;
  }

  private String scopePluginId(String childPluginId) {
    return String.format("%s%s%s", stageId, Constants.ID_SEPARATOR, childPluginId);
  }
}
