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

package io.cdap.cdap.datastreams;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.JoinElement;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.common.DefaultAutoJoinerContext;
import io.cdap.cdap.etl.common.PipelinePhase;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.common.plugin.JoinerBridge;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.SparkPairCollection;
import io.cdap.cdap.etl.spark.SparkPipelineRunner;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import io.cdap.cdap.etl.spark.function.PluginFunctionContext;
import io.cdap.cdap.etl.spark.plugin.SparkPipelinePluginContext;
import io.cdap.cdap.etl.spark.streaming.DStreamCollection;
import io.cdap.cdap.etl.spark.streaming.DefaultStreamingContext;
import io.cdap.cdap.etl.spark.streaming.DynamicDriverContext;
import io.cdap.cdap.etl.spark.streaming.PairDStreamCollection;
import io.cdap.cdap.etl.spark.streaming.function.CountingTransformFunction;
import io.cdap.cdap.etl.spark.streaming.function.DynamicJoinMerge;
import io.cdap.cdap.etl.spark.streaming.function.DynamicJoinOn;
import io.cdap.cdap.etl.spark.streaming.function.WrapOutputTransformFunction;
import io.cdap.cdap.etl.spark.streaming.function.preview.LimitingFunction;
import io.cdap.cdap.etl.validation.LoggingFailureCollector;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Driver for running pipelines using Spark Streaming.
 */
public class SparkStreamingPipelineRunner extends SparkPipelineRunner {

  private final JavaSparkExecutionContext sec;
  private final JavaStreamingContext streamingContext;
  private final DataStreamsPipelineSpec spec;
  private final boolean checkpointsDisabled;

  public SparkStreamingPipelineRunner(JavaSparkExecutionContext sec, JavaStreamingContext streamingContext,
                                      DataStreamsPipelineSpec spec, boolean checkpointsDisabled) {
    this.sec = sec;
    this.streamingContext = streamingContext;
    this.checkpointsDisabled = checkpointsDisabled;
    this.spec = spec;
  }

  @Override
  protected SparkCollection<RecordInfo<Object>> getSource(StageSpec stageSpec,
                                                          FunctionCache.Factory functionCacheFactory,
                                                          StageStatisticsCollector collector) throws Exception {
    StreamingSource<Object> source;
    if (checkpointsDisabled) {
      PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
      source = pluginFunctionContext.createPlugin();
    } else {
      // check for macros in any StreamingSource. If checkpoints are enabled,
      // SparkStreaming will serialize all InputDStreams created in the checkpoint, which means
      // the InputDStream is deserialized directly from the checkpoint instead of instantiated through CDAP.
      // This means there isn't any way for us to perform macro evaluation on sources when they are loaded from
      // checkpoints. We can work around this in all other pipeline stages by dynamically instantiating the
      // plugin in all DStream functions, but can't for InputDStreams because the InputDStream constructor
      // adds itself to the context dag. Yay for constructors with global side effects.
      // TODO: (HYDRATOR-1030) figure out how to do this at configure time instead of run time
      MacroEvaluator macroEvaluator = new ErrorMacroEvaluator(
        "Due to spark limitations, macro evaluation is not allowed in streaming sources when checkpointing " +
          "is enabled.");
      PluginContext pluginContext = new SparkPipelinePluginContext(sec.getPluginContext(), sec.getMetrics(),
                                                                   spec.isStageLoggingEnabled(),
                                                                   spec.isProcessTimingEnabled());
      source = pluginContext.newPluginInstance(stageSpec.getName(), macroEvaluator);
    }

    DataTracer dataTracer = sec.getDataTracer(stageSpec.getName());
    StreamingContext sourceContext = new DefaultStreamingContext(stageSpec, sec, streamingContext);
    JavaDStream<Object> javaDStream = source.getStream(sourceContext);
    if (dataTracer.isEnabled()) {
      // it will create a new function for each RDD, which would limit each RDD but not the entire DStream.
      javaDStream = javaDStream.transform(new LimitingFunction<>(spec.getNumOfRecordsPreview()));
    }
    JavaDStream<RecordInfo<Object>> outputDStream = javaDStream
      .transform(new CountingTransformFunction<>(stageSpec.getName(), sec.getMetrics(), "records.out", dataTracer))
      .map(new WrapOutputTransformFunction<>(stageSpec.getName()));
    return new DStreamCollection<>(sec, functionCacheFactory, outputDStream);
  }

  @Override
  protected SparkPairCollection<Object, Object> addJoinKey(StageSpec stageSpec,
                                                           FunctionCache.Factory functionCacheFactory,
                                                           String inputStageName,
                                                           SparkCollection<Object> inputCollection,
                                                           StageStatisticsCollector collector) throws Exception {
    DynamicDriverContext dynamicDriverContext = new DynamicDriverContext(stageSpec, sec, collector);
    JavaDStream<Object> dStream = inputCollection.getUnderlying();
    JavaPairDStream<Object, Object> result = dStream.transformToPair(
      new DynamicJoinOn<>(dynamicDriverContext, functionCacheFactory.newCache(), inputStageName));
    return new PairDStreamCollection<>(sec, functionCacheFactory, result);
  }

  @Override
  protected SparkCollection<Object> mergeJoinResults(
    StageSpec stageSpec, FunctionCache.Factory functionCacheFactory, SparkPairCollection<Object,
    List<JoinElement<Object>>> joinedInputs, StageStatisticsCollector collector) throws Exception {

    DynamicDriverContext dynamicDriverContext = new DynamicDriverContext(stageSpec, sec, collector);
    JavaPairDStream<Object, List<JoinElement<Object>>> pairDStream = joinedInputs.getUnderlying();
    JavaDStream<Object> result = pairDStream.transform(
      new DynamicJoinMerge<>(dynamicDriverContext, functionCacheFactory.newCache()));
    return new DStreamCollection<>(sec, functionCacheFactory, result);
  }

  @Override
  protected SparkCollection<Object> handleJoin(Map<String, SparkCollection<Object>> inputDataCollections,
                                               PipelinePhase pipelinePhase, PluginFunctionContext pluginFunctionContext,
                                               StageSpec stageSpec, FunctionCache.Factory functionCacheFactory,
                                               Object plugin, Integer numPartitions,
                                               StageStatisticsCollector collector,
                                               Set<String> shufflers) throws Exception {
    String stageName = stageSpec.getName();
    BatchJoiner<?, ?, ?> joiner;
    if (plugin instanceof BatchAutoJoiner) {
      BatchAutoJoiner autoJoiner = (BatchAutoJoiner) plugin;

      Map<String, Schema> inputSchemas = new HashMap<>();
      for (String inputStageName : pipelinePhase.getStageInputs(stageName)) {
        StageSpec inputStageSpec = pipelinePhase.getStage(inputStageName);
        inputSchemas.put(inputStageName, inputStageSpec.getOutputSchema());
      }
      FailureCollector failureCollector = new LoggingFailureCollector(stageName, inputSchemas);
      AutoJoinerContext autoJoinerContext = DefaultAutoJoinerContext.from(inputSchemas, failureCollector);
      failureCollector.getOrThrowException();

      JoinDefinition joinDefinition = autoJoiner.define(autoJoinerContext);
      if (joinDefinition == null) {
        throw new IllegalStateException(
          String.format("Joiner stage '%s' did not specify a join definition. " +
                          "Check with the plugin developer to ensure it is implemented correctly.",
                        stageName));
      }
      joiner = new JoinerBridge(stageName, autoJoiner, joinDefinition);
    } else if (plugin instanceof BatchJoiner) {
      joiner = (BatchJoiner) plugin;
    } else {
      // should never happen unless there is a bug in the code. should have failed during deployment
      throw new IllegalStateException(String.format("Stage '%s' is an unknown joiner type %s",
                                                    stageName, plugin.getClass().getName()));
    }

    BatchJoinerRuntimeContext joinerRuntimeContext = pluginFunctionContext.createBatchRuntimeContext();
    joiner.initialize(joinerRuntimeContext);
    shufflers.add(stageName);
    return handleJoin(joiner, inputDataCollections, stageSpec, functionCacheFactory, numPartitions, collector);
  }
}
