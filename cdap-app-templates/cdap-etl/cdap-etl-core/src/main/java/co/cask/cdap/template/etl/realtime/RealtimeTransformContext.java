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

package co.cask.cdap.template.etl.realtime;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.template.etl.api.TransformContext;
import co.cask.cdap.template.etl.common.ScopedPluginContext;

/**
 * Context for the Transform Stage.
 */
public class RealtimeTransformContext extends ScopedPluginContext implements TransformContext {
  private final WorkerContext context;
  private final Metrics metrics;

  public RealtimeTransformContext(WorkerContext context, Metrics metrics, String stageId) {
    super(stageId);
    this.context = context;
    this.metrics = metrics;
  }

  @Override
  public Metrics getMetrics() {
    return metrics;
  }

  @Override
  protected <T> T newScopedPluginInstance(String scopedPluginId) throws InstantiationException {
    // temporary hack to let it support both templates and applications. Will be removed when templates are removed.
    try {
      return context.newPluginInstance(scopedPluginId);
    } catch (UnsupportedOperationException e) {
      return context.newInstance(scopedPluginId);
    }
  }

  @Override
  protected <T> Class<T> loadScopedPluginClass(String scopedPluginId) {
    // temporary hack until templates are removed, and to let this work for both apps and templates
    try {
      return context.loadPluginClass(scopedPluginId);
    } catch (UnsupportedOperationException e) {
      return context.loadClass(scopedPluginId);
    }
  }

  @Override
  public PluginProperties getPluginProperties() {
    // temporary hack to let it support both templates and applications. Will be removed when templates are removed.
    try {
      return context.getPluginProperties(stageId);
    } catch (UnsupportedOperationException e) {
      return context.getPluginProps(stageId);
    }
  }

  @Override
  public PluginProperties getScopedPluginProperties(String scopedPluginId) {
    // temporary hack to let it support both templates and applications. Will be removed when templates are removed.
    try {
      return context.getPluginProperties(scopedPluginId);
    } catch (UnsupportedOperationException e) {
      return context.getPluginProps(scopedPluginId);
    }
  }
}
