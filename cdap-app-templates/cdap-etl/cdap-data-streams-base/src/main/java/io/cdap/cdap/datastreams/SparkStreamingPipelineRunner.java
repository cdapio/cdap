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
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingEventHandler;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.DefaultAutoJoinerContext;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.NoopStageStatisticsCollector;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.PipelinePhase;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.common.plugin.JoinerBridge;
import io.cdap.cdap.etl.planner.CombinerDag;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.SparkPairCollection;
import io.cdap.cdap.etl.spark.SparkPipelineRunner;
import io.cdap.cdap.etl.spark.batch.RDDCollection;
import io.cdap.cdap.etl.spark.batch.SparkBatchSinkFactory;
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
import io.cdap.cdap.etl.spark.streaming.function.StreamingBatchSinkFunction;
import io.cdap.cdap.etl.spark.streaming.function.WrapOutputTransformFunction;
import io.cdap.cdap.etl.spark.streaming.function.preview.LimitingFunction;
import io.cdap.cdap.etl.validation.LoggingFailureCollector;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Driver for running pipelines using Spark Streaming.
 */
public class SparkStreamingPipelineRunner extends SparkPipelineRunner {

  private final JavaSparkExecutionContext sec;
  private final JavaStreamingContext streamingContext;
  private final DataStreamsPipelineSpec spec;
  private final boolean checkpointsDisabled;

  private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingPipelineRunner.class);

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
    StreamingSource<Object, Object> source = createPluginInstance(stageSpec, collector);

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

  private StreamingSource<Object, Object> createPluginInstance(StageSpec stageSpec,
                                                               StageStatisticsCollector collector) throws Exception {
    StreamingSource<Object, Object> source;
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
    return source;
  }

  @Override
  public void runStatefulPipeline(PhaseSpec phaseSpec, String sourcePluginType,
                                  JavaSparkExecutionContext sec,
                                  Map<String, Integer> stagePartitions,
                                  PluginContext pluginContext,
                                  Map<String, StageStatisticsCollector> collectors,
                                  Set<String> uncombinableSinks,
                                  boolean consolidateStages,
                                  boolean cacheFunctions) throws Exception {
    PipelinePhase pipelinePhase = phaseSpec.getPhase();
    BasicArguments arguments = new BasicArguments(sec);
    FunctionCache.Factory functionCacheFactory = FunctionCache.Factory.newInstance(cacheFunctions);
    MacroEvaluator macroEvaluator =
      new DefaultMacroEvaluator(arguments,
                                sec.getLogicalStartTime(),
                                sec.getSecureStore(),
                                sec.getServiceDiscoverer(),
                                sec.getNamespace());
    Map<String, EmittedRecords> emittedRecords = new HashMap<>();

    // should never happen, but removes warning
    if (pipelinePhase.getDag() == null) {
      throw new IllegalStateException("Pipeline phase has no connections.");
    }

    Set<String> uncombinableStages = new HashSet<>(uncombinableSinks);
    for (String uncombinableType : UNCOMBINABLE_PLUGIN_TYPES) {
      pipelinePhase.getStagesOfType(uncombinableType).stream()
        .map(StageSpec::getName)
        .forEach(s -> uncombinableStages.add(s));
    }

    CombinerDag groupedDag = new CombinerDag(pipelinePhase.getDag(), uncombinableStages);
    Map<String, Set<String>> groups = consolidateStages ? groupedDag.groupNodes() : Collections.emptyMap();
    if (!groups.isEmpty()) {
      LOG.debug("Stage consolidation is on.");
      int groupNum = 1;
      for (Set<String> group : groups.values()) {
        LOG.debug("Group{}: {}", groupNum, group);
        groupNum++;
      }
    }
    Set<String> branchers = new HashSet<>();
    for (String stageName : groupedDag.getNodes()) {
      if (groupedDag.getNodeOutputs(stageName).size() > 1) {
        branchers.add(stageName);
      }
    }
    Set<String> shufflers = pipelinePhase.getStagesOfType(BatchAggregator.PLUGIN_TYPE).stream()
      .map(StageSpec::getName)
      .collect(Collectors.toSet());

    //Process source separately
    String source = groupedDag.getTopologicalOrder().get(0);
    StageSpec stageSpec = pipelinePhase.getStage(source);
    StageStatisticsCollector collector = collectors.get(source) == null ? new NoopStageStatisticsCollector()
      : collectors.get(source);
    StreamingSource<Object, Object> pluginInstance = createPluginInstance(stageSpec, collector);
    StreamingContext sourceStreamingContext = new DefaultStreamingContext(stageSpec, sec, streamingContext);
    DataTracer dataTracer = sec.getDataTracer(stageSpec.getName());
    JavaDStream<Object> dStream = pluginInstance.getStream(sourceStreamingContext);
    dStream.foreachRDD((javaRDD, time) -> {
      if (dataTracer.isEnabled()) {
        // it will create a new function for each RDD, which would limit each RDD but not the entire DStream.
        javaRDD = new LimitingFunction<>(spec.getNumOfRecordsPreview()).call(javaRDD);
      }
      javaRDD = new CountingTransformFunction<>(stageSpec.getName(), sec.getMetrics(), "records.out", dataTracer).call(
        javaRDD);
      JavaRDD<RecordInfo<Object>> wrapped = javaRDD.map(new WrapOutputTransformFunction<>(stageSpec.getName()));
      RDDCollection<RecordInfo<Object>> rddCollection = new RDDCollection<>(sec,
                                                                            functionCacheFactory,
                                                                            streamingContext.sparkContext(),
                                                                            null, null,
                                                                            new SparkBatchSinkFactory(),
                                                                            wrapped);
      EmittedRecords.Builder emittedBuilder = EmittedRecords.builder();
      EmittedRecords.Builder builder = addEmitted(emittedBuilder, pipelinePhase, stageSpec, rddCollection, groupedDag,
                                                  branchers, shufflers, false, false);
      emittedRecords.put(source, builder.build());
      processDag(phaseSpec, sourcePluginType, sec, stagePartitions, pluginContext, collectors, pipelinePhase,
                 functionCacheFactory, macroEvaluator, emittedRecords, groupedDag, groups, branchers, shufflers);
      if (dStream instanceof StreamingEventHandler) {
        ((StreamingEventHandler) dStream).onBatchCompleted(sourceStreamingContext);
      } else if (dStream.dstream() instanceof StreamingEventHandler) {
        ((StreamingEventHandler) (dStream.dstream())).onBatchCompleted(sourceStreamingContext);
      }
    });
  }

  //TODO - Do this for all sinks
  @Override
  protected Runnable getSinkTask(FunctionCache.Factory functionCacheFactory, StageSpec stageSpec,
                                 SparkCollection<Object> stageData, PluginFunctionContext pluginFunctionContext) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          //TODO - get time from dstream
          new StreamingBatchSinkFunction<>(sec, stageSpec, functionCacheFactory.newCache()).call(
            stageData.getUnderlying(), Time.apply(System.currentTimeMillis()));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
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
