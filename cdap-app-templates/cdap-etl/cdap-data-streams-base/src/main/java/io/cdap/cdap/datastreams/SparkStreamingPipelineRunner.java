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

import io.cdap.cdap.api.Transactionals;
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
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingEventHandler;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.common.DefaultAutoJoinerContext;
import io.cdap.cdap.etl.common.NoopStageStatisticsCollector;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.PipelinePhase;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.common.plugin.JoinerBridge;
import io.cdap.cdap.etl.planner.CombinerDag;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.EmittedRecords;
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
import io.cdap.cdap.etl.spark.streaming.StreamingRetrySettings;
import io.cdap.cdap.etl.spark.streaming.function.CountingTransformFunction;
import io.cdap.cdap.etl.spark.streaming.function.DynamicJoinMerge;
import io.cdap.cdap.etl.spark.streaming.function.DynamicJoinOn;
import io.cdap.cdap.etl.spark.streaming.function.StreamingBatchSinkFunction;
import io.cdap.cdap.etl.spark.streaming.function.StreamingMultiSinkFunction;
import io.cdap.cdap.etl.spark.streaming.function.StreamingSparkSinkFunction;
import io.cdap.cdap.etl.spark.streaming.function.WrapOutputTransformFunction;
import io.cdap.cdap.etl.spark.streaming.function.preview.LimitingFunction;
import io.cdap.cdap.etl.validation.LoggingFailureCollector;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Driver for running pipelines using Spark Streaming.
 */
public class SparkStreamingPipelineRunner extends SparkPipelineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingPipelineRunner.class);
  private final JavaSparkExecutionContext sec;
  private final JavaStreamingContext javaStreamingContext;
  private final DataStreamsPipelineSpec spec;
  private final boolean stateStoreEnabled;
  private final StreamingRetrySettings streamingRetrySettings;

  public SparkStreamingPipelineRunner(JavaSparkExecutionContext sec, JavaStreamingContext javaStreamingContext,
                                      DataStreamsPipelineSpec spec) {
    this.sec = sec;
    this.javaStreamingContext = javaStreamingContext;
    this.spec = spec;
    this.stateStoreEnabled = spec.getStateSpec().getMode() == DataStreamsStateSpec.Mode.STATE_STORE
      && !spec.isPreviewEnabled(sec);
    this.streamingRetrySettings = spec.getStreamingRetrySettings();
    LOG.debug("State handling mode is : {}", spec.getStateSpec().getMode());
  }

  @Override
  protected SparkCollection<RecordInfo<Object>> getSource(StageSpec stageSpec,
                                                          FunctionCache.Factory functionCacheFactory,
                                                          StageStatisticsCollector collector,
      Callable<Void> batchRetryFunction) throws Exception {
    JavaDStream<Object> javaDStream = getDStream(stageSpec, collector);
    DataTracer dataTracer = sec.getDataTracer(stageSpec.getName());
    if (dataTracer.isEnabled()) {
      // it will create a new function for each RDD, which would limit each RDD but not the entire DStream.
      javaDStream = javaDStream.transform(new LimitingFunction<>(spec.getNumOfRecordsPreview()));
    }
    JavaDStream<RecordInfo<Object>> outputDStream = javaDStream
      .transform(new CountingTransformFunction<>(stageSpec.getName(), sec.getMetrics(), "records.out", dataTracer))
      .map(new WrapOutputTransformFunction<>(stageSpec.getName()));
    return new DStreamCollection<>(sec, functionCacheFactory, outputDStream, streamingRetrySettings,
        batchRetryFunction);
  }

  private JavaDStream<Object> getDStream(StageSpec stageSpec, StageStatisticsCollector collector) throws Exception {
    StreamingSource<Object> source;
    if (spec.getStateSpec().getMode() != DataStreamsStateSpec.Mode.SPARK_CHECKPOINTING) {
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
        "Due to spark limitations, macro evaluation is not allowed in streaming sources when checkpointing "
          + "is enabled.");
      PluginContext pluginContext = new SparkPipelinePluginContext(sec.getPluginContext(), sec.getMetrics(),
                                                                   spec.isStageLoggingEnabled(),
                                                                   spec.isProcessTimingEnabled());
      source = pluginContext.newPluginInstance(stageSpec.getName(), macroEvaluator);
    }

    StreamingContext sourceContext = new DefaultStreamingContext(stageSpec, sec, javaStreamingContext,
                                                                 stateStoreEnabled);
    return source.getStream(sourceContext);
  }

  @Override
  protected JavaSparkContext getSparkContext() {
    return javaStreamingContext.sparkContext();
  }

  @Override
  protected SparkPairCollection<Object, Object> addJoinKey(StageSpec stageSpec,
                                                           FunctionCache.Factory functionCacheFactory,
                                                           String inputStageName,
                                                           SparkCollection<Object> inputCollection,
                                                           StageStatisticsCollector collector,
      Callable<Void> batchRetryFunction) throws Exception {
    DynamicDriverContext dynamicDriverContext = new DynamicDriverContext(stageSpec, sec, collector);
    JavaDStream<Object> dStream = inputCollection.getUnderlying();
    JavaPairDStream<Object, Object> result = dStream.transformToPair(
      new DynamicJoinOn<>(dynamicDriverContext, functionCacheFactory.newCache(), inputStageName));
    return new PairDStreamCollection<>(sec, functionCacheFactory, result, streamingRetrySettings,
        batchRetryFunction);
  }

  @Override
  protected SparkCollection<Object> mergeJoinResults(
    StageSpec stageSpec, FunctionCache.Factory functionCacheFactory, SparkPairCollection<Object,
    List<JoinElement<Object>>> joinedInputs, StageStatisticsCollector collector,
      Callable<Void> batchRetryFunction) throws Exception {

    DynamicDriverContext dynamicDriverContext = new DynamicDriverContext(stageSpec, sec, collector);
    JavaPairDStream<Object, List<JoinElement<Object>>> pairDStream = joinedInputs.getUnderlying();
    JavaDStream<Object> result = pairDStream.transform(
      new DynamicJoinMerge<>(dynamicDriverContext, functionCacheFactory.newCache()));
    return new DStreamCollection<>(sec, functionCacheFactory, result, streamingRetrySettings,
        batchRetryFunction);
  }

  @Override
  protected SparkCollection<Object> handleJoin(Map<String, SparkCollection<Object>> inputDataCollections,
                                               PipelinePhase pipelinePhase, PluginFunctionContext pluginFunctionContext,
                                               StageSpec stageSpec, FunctionCache.Factory functionCacheFactory,
                                               Object plugin, Integer numPartitions,
                                               StageStatisticsCollector collector,
                                               Set<String> shufflers,
      Callable<Void> batchRetryFunction) throws Exception {
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
          String.format("Joiner stage '%s' did not specify a join definition. "
                          + "Check with the plugin developer to ensure it is implemented correctly.",
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
    return handleJoin(joiner, inputDataCollections, stageSpec, functionCacheFactory, numPartitions, collector,
        batchRetryFunction);
  }

  @Override
  protected void processDag(PhaseSpec phaseSpec, String sourcePluginType, JavaSparkExecutionContext sec,
                            Map<String, Integer> stagePartitions, PluginContext pluginContext,
                            Map<String, StageStatisticsCollector> collectors, PipelinePhase pipelinePhase,
                            FunctionCache.Factory functionCacheFactory,
                            MacroEvaluator macroEvaluator, CombinerDag groupedDag,
                            Map<String, Set<String>> groups, Set<String> branchers,
                            Set<String> shufflers) throws Exception {
    if (!stateStoreEnabled) {
      super.processDag(phaseSpec, sourcePluginType, sec, stagePartitions, pluginContext, collectors, pipelinePhase,
                       functionCacheFactory, macroEvaluator, groupedDag, groups, branchers, shufflers);
      return;
    }

    //Process source separately
    List<String> topologicalOrder = groupedDag.getTopologicalOrder();
    String source = topologicalOrder.get(0);
    StageSpec stageSpec = pipelinePhase.getStage(source);
    StageStatisticsCollector collector = collectors.getOrDefault(source, new NoopStageStatisticsCollector());
    JavaDStream<Object> dStream = getDStream(stageSpec, collector);
    StreamingContext streamingContext = new DefaultStreamingContext(stageSpec, sec, javaStreamingContext,
                                                                    stateStoreEnabled);
    DataTracer dataTracer = sec.getDataTracer(stageSpec.getName());
    AtomicBoolean failedBatch = new AtomicBoolean();
    StreamingEventHandler eventHandler = getEventHandler(dStream);
    Callable<Void> batchRetryCallable = () -> {
      if(eventHandler != null){
        eventHandler.onBatchRetry(streamingContext);
      }
      return null;
    };

    dStream.foreachRDD((javaRDD, time) -> {
      // Invoke onBatchStarted
      if (eventHandler != null) {
        eventHandler.onBatchStarted(streamingContext);
      }

      Collection<Runnable> sinkRunnables = new ArrayList<>();
      Transactionals.execute(sec, context -> {
        JavaRDD<Object> batchRDD = javaRDD;
        if (dataTracer.isEnabled()) {
          // it will create a new function for each RDD, which would limit each RDD but not the entire DStream.
          batchRDD = new LimitingFunction<>(spec.getNumOfRecordsPreview()).call(batchRDD);
        }
        batchRDD = new CountingTransformFunction<>(stageSpec.getName(), sec.getMetrics(), "records.out", dataTracer)
          .call(batchRDD);
        JavaRDD<RecordInfo<Object>> wrapped = batchRDD.map(new WrapOutputTransformFunction<>(stageSpec.getName()));
        RDDCollection<RecordInfo<Object>> rddCollection =
          new RDDCollection<RecordInfo<Object>>(sec, functionCacheFactory, javaStreamingContext.sparkContext(),
                                                new SQLContext(javaStreamingContext.sparkContext()),
                                                context, new SparkBatchSinkFactory(), wrapped);
        EmittedRecords records = getEmittedRecords(pipelinePhase, stageSpec, rddCollection, groupedDag,
                                                   branchers, shufflers, false, false);
        Map<String, EmittedRecords> emittedRecords = new HashMap<>();
        emittedRecords.put(source, records);

        //process the remaining stages
        for (int i = 1; i < topologicalOrder.size(); i++) {
          String stageName = topologicalOrder.get(i);
          processStage(phaseSpec, sourcePluginType, sec, stagePartitions, pluginContext, collectors, pipelinePhase,
                       functionCacheFactory, macroEvaluator, emittedRecords, groupedDag, groups, branchers, shufflers,
                       sinkRunnables, stageName, time.milliseconds(), context, batchRetryCallable);
        }
      }, Exception.class);

      // Exception is thrown from sink runnables to terminate the pipeline on a batch failure.
      // But in most cases it was observed that the next pending batch starts processing while the shutdown is ongoing.
      // Set the failure status in StreamingContext so that further batches do not save state even if they succeed.
      // Without this, there is a risk of a batch being lost and at least once guarantee will fail.
      try {
        //We should have all the sink runnables at this point, execute them
        executeSinkRunnables(sec, sinkRunnables);
      } catch (Exception e) {
        failedBatch.set(true);
        LOG.debug("Batch failure flag set.");
        throw e;
      }

      if (failedBatch.get()) {
        LOG.info("Current batch succeeded, but state will not be saved since a previous batch failed.");
        return;
      }

      // Invoke onBatchCompleted
      if(eventHandler != null){
        eventHandler.onBatchCompleted(streamingContext);
      }
    });
  }

  @Override
  protected Runnable getSparkSinkRunnable(StageSpec stageSpec, SparkCollection<Object> stageData,
                                          SparkSink<Object> sparkSink, long time,
      Callable<Void> batchRetryFunction) throws Exception {
    if (!stateStoreEnabled) {
      return super.getSparkSinkRunnable(stageSpec, stageData, sparkSink, time, batchRetryFunction);
    }

    return () -> {
      try {
        new StreamingSparkSinkFunction<>(sec, stageSpec, streamingRetrySettings, batchRetryFunction)
          .call(stageData.getUnderlying(), Time.apply(time));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  protected Runnable getBatchSinkRunnable(FunctionCache.Factory functionCacheFactory, StageSpec stageSpec,
                                          SparkCollection<Object> stageData,
                                          PluginFunctionContext pluginFunctionContext, long time,
      Callable<Void> batchRetryFunction) {
    if (!stateStoreEnabled) {
      return super.getBatchSinkRunnable(functionCacheFactory, stageSpec, stageData, pluginFunctionContext, time,
          batchRetryFunction);
    }

    return () -> {
      try {
        new StreamingBatchSinkFunction<>(sec, stageSpec, functionCacheFactory.newCache(), streamingRetrySettings,
            batchRetryFunction)
          .call(stageData.getUnderlying(), Time.apply(time));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  protected Runnable getGroupSinkRunnable(PhaseSpec phaseSpec, Set<String> groupStages,
                                          Map<String, StageStatisticsCollector> collectors,
                                          SparkCollection<RecordInfo<Object>> fullInput, Set<String> groupSinks,
                                          long time, Callable<Void> batchRetryFunction) {
    if (!stateStoreEnabled) {
      return super.getGroupSinkRunnable(phaseSpec, groupStages, collectors, fullInput, groupSinks, time,
          batchRetryFunction);
    }

    return () -> {
      try {
        new StreamingMultiSinkFunction(sec, phaseSpec, groupStages, groupSinks, collectors, streamingRetrySettings,
            batchRetryFunction)
          .call(fullInput.getUnderlying(), Time.apply(time));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Nullable
  private StreamingEventHandler getEventHandler(JavaDStream<Object> dStream){
    if (dStream instanceof StreamingEventHandler) {
      return (StreamingEventHandler) dStream;
    } else if (dStream.dstream() instanceof StreamingEventHandler) {
      return (StreamingEventHandler) (dStream.dstream());
    }

    return null;
  }
}
