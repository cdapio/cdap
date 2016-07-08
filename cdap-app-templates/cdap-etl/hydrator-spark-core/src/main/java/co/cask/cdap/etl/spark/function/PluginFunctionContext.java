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

package co.cask.cdap.etl.spark.function;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.cdap.etl.spark.batch.SparkBatchRuntimeContext;

import java.io.Serializable;
import java.util.Map;

/**
 * Serializable collection of objects that can be used in Spark closures to instantiate plugins.
 */
public class PluginFunctionContext implements Serializable {

  private static final long serialVersionUID = -8131461628444037900L;

  private final String stageName;
  private final PluginContext pluginContext;
  private final Metrics metrics;
  private final long logicalStartTime;
  private final Map<String, String> runtimeArgs;

  public PluginFunctionContext(String stageName, JavaSparkExecutionContext sec) {
    this.stageName = stageName;
    this.pluginContext = sec.getPluginContext();
    this.metrics = sec.getMetrics();
    this.logicalStartTime = sec.getLogicalStartTime();
    this.runtimeArgs = sec.getRuntimeArguments();
  }

  public <T> T createPlugin() throws Exception {
    return pluginContext.newPluginInstance(stageName);
  }

  public StageMetrics createStageMetrics() {
    return new DefaultStageMetrics(metrics, stageName);
  }

  public BatchRuntimeContext createBatchRuntimeContext() {
    return new SparkBatchRuntimeContext(pluginContext, metrics, logicalStartTime, runtimeArgs, stageName);
  }
}
