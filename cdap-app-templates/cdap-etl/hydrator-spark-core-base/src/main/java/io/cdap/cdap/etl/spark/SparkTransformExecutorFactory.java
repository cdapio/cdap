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

import com.google.common.base.Function;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.etl.api.Transformation;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.batch.PipelinePluginInstantiator;
import io.cdap.cdap.etl.common.DefaultEmitter;
import io.cdap.cdap.etl.common.NoopStageStatisticsCollector;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.common.TransformExecutor;
import io.cdap.cdap.etl.common.TransformingEmitter;
import io.cdap.cdap.etl.exec.PipeStage;
import io.cdap.cdap.etl.exec.TransformExecutorFactory;
import io.cdap.cdap.etl.exec.UnwrapPipeStage;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.batch.SparkBatchRuntimeContext;
import scala.Tuple2;

import java.util.Map;

/**
 * Helps create {@link TransformExecutor TransformExecutors}.
 *
 * @param <T> the type of input for the created transform executors
 */
public class SparkTransformExecutorFactory<T> extends TransformExecutorFactory<T> {
  private final Map<String, StageStatisticsCollector> collectors;
  private final Map<String, DataTracer> dataTracers;
  private final PipelineRuntime pipelineRuntime;
  private final DefaultEmitter<Tuple2<String, KeyValue<Object, Object>>> sinkEmitter;

  public SparkTransformExecutorFactory(PipelinePluginInstantiator pluginInstantiator, MacroEvaluator macroEvaluator,
                                       String sourceStageName,
                                       Map<String, StageStatisticsCollector> collectors,
                                       Map<String, DataTracer> dataTracers,
                                       PipelineRuntime pipelineRuntime,
                                       DefaultEmitter<Tuple2<String, KeyValue<Object, Object>>> sinkEmitter) {
    super(pluginInstantiator, macroEvaluator, pipelineRuntime.getMetrics(), sourceStageName, !collectors.isEmpty());
    this.collectors = collectors;
    this.dataTracers = dataTracers;
    this.pipelineRuntime = pipelineRuntime;
    this.sinkEmitter = sinkEmitter;
  }

  @Override
  protected DataTracer getDataTracer(String stageName) {
    return dataTracers.get(stageName);
  }

  @Override
  protected StageStatisticsCollector getStatisticsCollector(String stageName) {
    return collectors.getOrDefault(stageName, NoopStageStatisticsCollector.INSTANCE);
  }

  @Override
  protected BatchRuntimeContext createRuntimeContext(StageSpec stageSpec) {
    return new SparkBatchRuntimeContext(pipelineRuntime, stageSpec);
  }

  @Override
  protected PipeStage getSinkPipeStage(StageSpec stageSpec) throws Exception {
    Transformation<Object, KeyValue<Object, Object>> sink = getTransformation(stageSpec);
    return new UnwrapPipeStage(stageSpec.getName(), sink,
                               new TransformingEmitter<>(sinkEmitter, new StageTaggingFunction<>(stageSpec.getName())));
  }

  /**
   * Takes a KeyValue and outputs a Tuple2 with the stage name as the first value and the keyvalue as the second
   * value.
   *
   * @param <K> type of key
   * @param <V> type of value
   */
  private static class StageTaggingFunction<K, V> implements Function<KeyValue<K, V>, Tuple2<String, KeyValue<K, V>>> {
    private final String stage;

    private StageTaggingFunction(String stage) {
      this.stage = stage;
    }

    @Override
    public Tuple2<String, KeyValue<K, V>> apply(KeyValue<K, V> kv) {
      return new Tuple2<>(stage, kv);
    }
  }
}
