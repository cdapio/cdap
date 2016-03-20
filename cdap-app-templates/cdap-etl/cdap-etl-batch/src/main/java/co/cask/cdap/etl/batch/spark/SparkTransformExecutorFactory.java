/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.spark;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.batch.TransformExecutorFactory;

import java.util.Map;

/**
 * Creates transform executors for spark programs.
 *
 * @param <T> the type of input for the created transform executors
 */
public class SparkTransformExecutorFactory<T> extends TransformExecutorFactory<T> {
  private final PluginContext pluginContext;
  private final long logicalStartTime;
  private final Map<String, String> runtimeArgs;

  public SparkTransformExecutorFactory(PluginContext pluginContext,
                                       PipelinePluginInstantiator pluginInstantiator,
                                       Metrics metrics, long logicalStartTime,
                                       Map<String, String> runtimeArgs) {
    super(pluginInstantiator, metrics);
    this.pluginContext = pluginContext;
    this.logicalStartTime = logicalStartTime;
    this.runtimeArgs = runtimeArgs;
  }

  @Override
  protected BatchRuntimeContext createRuntimeContext(String stageName) {
    return new SparkBatchRuntimeContext(pluginContext, metrics, logicalStartTime, runtimeArgs, stageName);
  }
}
