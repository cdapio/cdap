/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark;

import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.Transformation;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.batch.PipelinePluginInstantiator;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.NoopStageStatisticsCollector;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.common.TransformExecutor;
import io.cdap.cdap.etl.exec.PipeStage;
import io.cdap.cdap.etl.exec.TransformExecutorFactory;
import io.cdap.cdap.etl.exec.UnwrapPipeStage;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.batch.SparkBatchRuntimeContext;

import java.util.Map;

/**
 * Helps create {@link TransformExecutor TransformExecutors}.
 *
 * @param <T> the type of input for the created transform executors
 */
public class SparkTransformExecutorFactory<T> extends TransformExecutorFactory<T> {
  private final Map<String, StageStatisticsCollector> collectors;
  private final JavaSparkExecutionContext sec;
  private final BasicArguments arguments;
  private final Emitter<KeyValue<Object, Object>> sinkEmitter;

  public SparkTransformExecutorFactory(PipelinePluginInstantiator pluginInstantiator, MacroEvaluator macroEvaluator,
                                       Metrics metrics, String sourceStageName, boolean collectStageStatistics,
                                       Map<String, StageStatisticsCollector> collectors,
                                       JavaSparkExecutionContext sec, BasicArguments arguments,
                                       Emitter<KeyValue<Object, Object>> sinkEmitter) {
    super(pluginInstantiator, macroEvaluator, metrics, sourceStageName, collectStageStatistics);
    this.collectors = collectors;
    this.sec = sec;
    this.arguments = arguments;
    this.sinkEmitter = sinkEmitter;
  }

  @Override
  protected DataTracer getDataTracer(String stageName) {
    return sec.getDataTracer(stageName);
  }

  @Override
  protected StageStatisticsCollector getStatisticsCollector(String stageName) {
    return collectors.getOrDefault(stageName, NoopStageStatisticsCollector.INSTANCE);
  }

  @Override
  protected BatchRuntimeContext createRuntimeContext(StageSpec stageSpec) {
    PipelineRuntime pipelineRuntime = new PipelineRuntime(
      sec.getNamespace(), sec.getApplicationSpecification().getName(), sec.getLogicalStartTime(),
      arguments, metrics, sec.getPluginContext(), sec.getServiceDiscoverer(), sec.getSecureStore());
    return new SparkBatchRuntimeContext(pipelineRuntime, stageSpec);
  }

  @Override
  protected PipeStage getSinkPipeStage(StageSpec stageSpec) throws Exception {
    Transformation<Object, KeyValue<Object, Object>> sink = getTransformation(stageSpec);
    return new UnwrapPipeStage(stageSpec.getName(), sink, sinkEmitter);
  }
}
