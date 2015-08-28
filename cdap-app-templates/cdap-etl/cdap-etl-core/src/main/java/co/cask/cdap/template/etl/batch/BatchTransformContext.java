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

package co.cask.cdap.template.etl.batch;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.template.etl.api.TransformContext;

/**
 * Context for the Transform Stage.
 */
public class BatchTransformContext implements TransformContext {
  private final MapReduceContext context;
  private final Metrics metrics;

  protected final String pluginId;

  public BatchTransformContext(MapReduceContext context, Metrics metrics, String pluginId) {
    this.context = context;
    this.metrics = metrics;
    this.pluginId = pluginId;
  }

  @Override
  public Metrics getMetrics() {
    return metrics;
  }

  @Override
  public PluginProperties getPluginProperties() {
    // temporary hack to let it support both templates and applications. Will be removed when templates are removed.
    try {
      return context.getPluginProperties(pluginId);
    } catch (UnsupportedOperationException e) {
      return context.getPluginProps(pluginId);
    }
  }
}
