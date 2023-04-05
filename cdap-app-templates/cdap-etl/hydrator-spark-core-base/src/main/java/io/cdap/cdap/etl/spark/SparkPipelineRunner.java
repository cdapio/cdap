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

package io.cdap.cdap.etl.spark;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.api.AlertPublisher;
import io.cdap.cdap.etl.api.ErrorRecord;
import io.cdap.cdap.etl.api.ErrorTransform;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.JoinElement;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchReducibleAggregator;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.join.AutoJoiner;
import io.cdap.cdap.etl.api.join.AutoJoinerContext;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinKey;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.api.relational.LinearRelationalTransform;
import io.cdap.cdap.etl.api.relational.RelationalTransform;
import io.cdap.cdap.etl.api.streaming.Windower;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultAutoJoinerContext;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.NoopStageStatisticsCollector;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.PipelinePhase;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.Schemas;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.common.plugin.PluginWrapper;
import io.cdap.cdap.etl.planner.CombinerDag;
import io.cdap.cdap.etl.planner.Dag;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec.Port;
import io.cdap.cdap.etl.spark.batch.BatchSinkRunnableProvider;
import io.cdap.cdap.etl.spark.batch.SQLBackedCollection;
import io.cdap.cdap.etl.spark.batch.WrappedSQLEngineCollection;
import io.cdap.cdap.etl.spark.function.AlertPassFilter;
import io.cdap.cdap.etl.spark.function.ErrorPassFilter;
import io.cdap.cdap.etl.spark.function.ErrorTransformFunction;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import io.cdap.cdap.etl.spark.function.InitialJoinFunction;
import io.cdap.cdap.etl.spark.function.JoinFlattenFunction;
import io.cdap.cdap.etl.spark.function.LeftJoinFlattenFunction;
import io.cdap.cdap.etl.spark.function.OuterJoinFlattenFunction;
import io.cdap.cdap.etl.spark.function.OutputPassFilter;
import io.cdap.cdap.etl.spark.function.PluginFunctionContext;
import io.cdap.cdap.etl.spark.join.JoinCollection;
import io.cdap.cdap.etl.spark.join.JoinExpressionRequest;
import io.cdap.cdap.etl.spark.join.JoinRequest;
import io.cdap.cdap.etl.spark.streaming.function.RecordInfoWrapper;
import io.cdap.cdap.etl.validation.LoggingFailureCollector;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base Spark program to run a Hydrator pipeline.
 */
public abstract class SparkPipelineRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SparkPipelineRunner.class);
  private static final Set<String> UNCOMBINABLE_PLUGIN_TYPES = ImmutableSet.of(
    BatchJoiner.PLUGIN_TYPE, BatchAggregator.PLUGIN_TYPE, Constants.Connector.PLUGIN_TYPE,
    SparkCompute.PLUGIN_TYPE, SparkSink.PLUGIN_TYPE, AlertPublisher.PLUGIN_TYPE);

  protected abstract SparkCollection<RecordInfo<Object>> getSource(StageSpec stageSpec,
                                                                   FunctionCache.Factory functionCacheFactory,
                                                                   StageStatisticsCollector collector) throws Exception;

  protected abstract JavaSparkContext getSparkContext();

  protected abstract SparkPairCollection<Object, Object> addJoinKey(
    StageSpec stageSpec, FunctionCache.Factory functionCacheFactory, String inputStageName,
    SparkCollection<Object> inputCollection, StageStatisticsCollector collector) throws Exception;

  protected abstract SparkCollection<Object> mergeJoinResults(
    StageSpec stageSpec,
    FunctionCache.Factory functionCacheFactory, SparkPairCollection<Object, List<JoinElement<Object>>> joinedInputs,
    StageStatisticsCollector collector) throws Exception;

  public void runPipeline(PhaseSpec phaseSpec, String sourcePluginType,
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

    processDag(phaseSpec, sourcePluginType, sec, stagePartitions, pluginContext, collectors, pipelinePhase,
               functionCacheFactory, macroEvaluator, groupedDag, groups, branchers, shufflers);
  }

  protected void processDag(PhaseSpec phaseSpec, String sourcePluginType, JavaSparkExecutionContext sec,
                            Map<String, Integer> stagePartitions, PluginContext pluginContext,
                            Map<String, StageStatisticsCollector> collectors, PipelinePhase pipelinePhase,
                            FunctionCache.Factory functionCacheFactory, MacroEvaluator macroEvaluator,
                            CombinerDag groupedDag, Map<String, Set<String>> groups, Set<String> branchers,
                            Set<String> shufflers) throws Exception {
    Collection<Runnable> sinkRunnables = new ArrayList<>();
    Map<String, EmittedRecords> emittedRecords = new HashMap<>();
    //Emitted records and sinkRunnables will be populated as each stage is processed
    SinkRunnableProvider sinkRunnableProvider = new BatchSinkRunnableProvider();
    for (String stageName : groupedDag.getTopologicalOrder()) {
      processStage(phaseSpec, sourcePluginType, sec, stagePartitions, pluginContext, collectors,
          pipelinePhase, functionCacheFactory, macroEvaluator, emittedRecords, groupedDag, groups,
          branchers, shufflers, sinkRunnables, stageName, System.currentTimeMillis(), null,
          sinkRunnableProvider);
    }
    //We should have all the sink runnables at this point, execute them
    executeSinkRunnables(sec, sinkRunnables);
  }

  protected void executeSinkRunnables(JavaSparkExecutionContext sec, Collection<Runnable> sinkRunnables) {
    boolean shouldWriteInParallel = Boolean.parseBoolean(
      sec.getRuntimeArguments().get("pipeline.spark.parallel.sinks.enabled"));
    if (!shouldWriteInParallel) {
      for (Runnable runnable : sinkRunnables) {
        runnable.run();
      }
      return;
    }

    Collection<Future> sinkFutures = new ArrayList<>(sinkRunnables.size());
    ExecutorService executorService = Executors.newFixedThreadPool(sinkRunnables.size(), new ThreadFactoryBuilder()
      .setNameFormat("pipeline-sink-task")
      .build());
    for (Runnable runnable : sinkRunnables) {
      sinkFutures.add(executorService.submit(runnable));
    }

    Throwable error = null;
    Iterator<Future> futureIter = sinkFutures.iterator();
    for (Future future : sinkFutures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        error = e.getCause();
        break;
      } catch (InterruptedException e) {
        break;
      }
    }
    executorService.shutdownNow();
    if (error != null) {
      throw Throwables.propagate(error);
    }
  }

  protected void processStage(PhaseSpec phaseSpec, String sourcePluginType,
      JavaSparkExecutionContext sec,
      Map<String, Integer> stagePartitions, PluginContext pluginContext,
      Map<String, StageStatisticsCollector> collectors, PipelinePhase pipelinePhase,
      FunctionCache.Factory functionCacheFactory, MacroEvaluator macroEvaluator,
      Map<String, EmittedRecords> emittedRecords, CombinerDag groupedDag,
      Map<String, Set<String>> groups, Set<String> branchers, Set<String> shufflers,
      Collection<Runnable> sinkRunnables, String stageName, long time,
      @Nullable DatasetContext datasetContext,
      SinkRunnableProvider sinkRunnableProvider) throws Exception {
    if (groups.containsKey(stageName)) {
      sinkRunnables.add(getGroupRunnable(sec, phaseSpec, groups.get(stageName),
          groupedDag.getNodeInputs(stageName), emittedRecords, collectors, time, datasetContext,
          sinkRunnableProvider));
      return;
    }

    StageSpec stageSpec = pipelinePhase.getStage(stageName);
    String pluginType = stageSpec.getPluginType();
    JavaSparkContext jsc = getSparkContext();

    try {
      String pluginName = stageSpec.getPlugin().getName();
      String artifactInfo = "";
      if (stageSpec.getPlugin().getArtifact() != null) {
        artifactInfo = stageSpec.getPlugin().getArtifact().toString();
      }

      jsc.setCallSite(stageName + " (Plugin " + pluginName + " of Type " + pluginType + ", " + artifactInfo + ")");

      // don't want to do an additional filter for stages that can emit errors,
      // but aren't connected to an ErrorTransform
      // similarly, don't want to do an additional filter for alerts when the stage isn't connected to
      // an AlertPublisher
      boolean hasErrorOutput = false;
      boolean hasAlertOutput = false;
      Set<String> outputs = pipelinePhase.getStageOutputs(stageName);
      for (String output : outputs) {
        String outputPluginType = pipelinePhase.getStage(output).getPluginType();
        //noinspection ConstantConditions
        if (ErrorTransform.PLUGIN_TYPE.equals(outputPluginType)) {
          hasErrorOutput = true;
        } else if (AlertPublisher.PLUGIN_TYPE.equals(outputPluginType)) {
          hasAlertOutput = true;
        }
      }

      Set<String> stageInputs = pipelinePhase.getStageInputs(stageName);
      Map<String, SparkCollection<Object>> inputDataCollections = getInputDataCollections(pipelinePhase, emittedRecords,
                                                                                          stageName, pluginType,
                                                                                          stageInputs);
      SparkCollection<Object> stageData = getStageData(pluginType, inputDataCollections);

      boolean isConnectorSource =
        Constants.Connector.PLUGIN_TYPE.equals(pluginType) && pipelinePhase.getSources().contains(stageName);
      boolean isConnectorSink =
        Constants.Connector.PLUGIN_TYPE.equals(pluginType) && pipelinePhase.getSinks().contains(stageName);

      StageStatisticsCollector collector = collectors.get(stageName) == null ? new NoopStageStatisticsCollector()
        : collectors.get(stageName);

      PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
      EmittedRecords emittedRecordsForStage = EmittedRecords.builder().build();
      if (stageData == null) {

        // this if-else is nested inside the stageRDD null check to avoid warnings about stageRDD possibly being
        // null in the other else-if conditions
        if (sourcePluginType.equals(pluginType) || isConnectorSource) {
          SparkCollection<RecordInfo<Object>> combinedData = getSource(stageSpec, functionCacheFactory, collector);
          EmittedRecords.Builder emittedBuilder = EmittedRecords.builder();
          emittedRecordsForStage = getEmittedRecords(pipelinePhase, stageSpec, combinedData, groupedDag, branchers,
                                                     shufflers, hasErrorOutput, hasAlertOutput);
        } else {
          throw new IllegalStateException(String.format("Stage '%s' has no input and is not a source.", stageName));
        }

      } else if (BatchSink.PLUGIN_TYPE.equals(pluginType) || isConnectorSink) {
        sinkRunnables.add(sinkRunnableProvider.getBatchSinkRunnable(stageSpec, stageData,
            functionCacheFactory, pluginFunctionContext));
      } else if (SparkSink.PLUGIN_TYPE.equals(pluginType)) {

        SparkSink<Object> sparkSink = pluginContext.newPluginInstance(stageName, macroEvaluator);
        sinkRunnables.add(
            sinkRunnableProvider.getSparkSinkRunnable(stageSpec, stageData, sparkSink));

      } else if (AlertPublisher.PLUGIN_TYPE.equals(pluginType)) {

        // union all the alerts coming into this stage
        SparkCollection<Alert> inputAlerts = null;
        for (String inputStage : stageInputs) {
          SparkCollection<Alert> inputErrorsFromStage = emittedRecords.get(inputStage).getAlertRecords();
          if (inputErrorsFromStage == null) {
            continue;
          }
          if (inputAlerts == null) {
            inputAlerts = inputErrorsFromStage;
          } else {
            inputAlerts = inputAlerts.union(inputErrorsFromStage);
          }
        }

        if (inputAlerts != null) {
          inputAlerts.publishAlerts(stageSpec, collector);
        }

      } else {
        emittedRecordsForStage = processOtherPluginTypes(pluginType, emittedRecords, stageInputs, stageSpec, collector,
                                                         pluginFunctionContext, functionCacheFactory, pipelinePhase,
                                                         groupedDag, branchers, shufflers, stageName, hasErrorOutput,
                                                         hasAlertOutput, stageData, inputDataCollections,
                                                         stagePartitions, pluginContext, macroEvaluator);
      }

      emittedRecords.put(stageName, emittedRecordsForStage);
    } finally {
      jsc.clearCallSite();
    }
  }

  private EmittedRecords processOtherPluginTypes(String pluginType, Map<String, EmittedRecords> emittedRecords,
                                                 Set<String> stageInputs, StageSpec stageSpec,
                                                 StageStatisticsCollector collector,
                                                 PluginFunctionContext pluginFunctionContext,
                                                 FunctionCache.Factory functionCacheFactory,
                                                 PipelinePhase pipelinePhase,
                                                 CombinerDag groupedDag, Set<String> branchers, Set<String> shufflers,
                                                 String stageName, boolean hasErrorOutput, boolean hasAlertOutput,
                                                 SparkCollection<Object> stageData,
                                                 Map<String, SparkCollection<Object>> inputDataCollections,
                                                 Map<String, Integer> stagePartitions,
                                                 PluginContext pluginContext,
                                                 MacroEvaluator macroEvaluator) throws Exception {
    if (ErrorTransform.PLUGIN_TYPE.equals(pluginType)) {

      // union all the errors coming into this stage
      SparkCollection<ErrorRecord<Object>> inputErrors = null;
      for (String inputStage : stageInputs) {
        SparkCollection<ErrorRecord<Object>> inputErrorsFromStage = emittedRecords.get(inputStage).getErrorRecords();
        if (inputErrorsFromStage == null) {
          continue;
        }
        if (inputErrors == null) {
          inputErrors = inputErrorsFromStage;
        } else {
          inputErrors = inputErrors.union(inputErrorsFromStage);
        }
      }

      if (inputErrors != null) {
        SparkCollection<RecordInfo<Object>> combinedData = inputErrors.flatMap(
          stageSpec,
          new ErrorTransformFunction<Object, Object>(pluginFunctionContext, functionCacheFactory.newCache()));
        return getEmittedRecords(pipelinePhase, stageSpec,
                                 combinedData, groupedDag, branchers, shufflers, hasErrorOutput, hasAlertOutput);
      }

    } else {
      Object plugin = pluginContext.newPluginInstance(stageName, macroEvaluator);
      Optional<EmittedRecords> declarativeBuilder = tryRelationalTransform(
        pipelinePhase, groupedDag, branchers, shufflers, stageName, stageSpec,
        hasErrorOutput, hasAlertOutput, stageData, inputDataCollections, plugin);

      if (declarativeBuilder.isPresent()) {
        return declarativeBuilder.get();
      } else {
        return transform(stagePartitions, pipelinePhase, functionCacheFactory, groupedDag, branchers, shufflers,
                         stageName, stageSpec, pluginType, hasErrorOutput, hasAlertOutput,
                         stageData, inputDataCollections, collector, pluginFunctionContext, plugin);
      }
    }
    return EmittedRecords.builder().build();
  }

  private SparkCollection<Object> getStageData(String pluginType,
                                               Map<String, SparkCollection<Object>> inputDataCollections) {
    SparkCollection<Object> stageData = null;
    // if this stage has multiple inputs, and is not a joiner plugin, or an error transform
    // initialize the stageRDD as the union of all input RDDs.
    if (!inputDataCollections.isEmpty()) {
      Iterator<SparkCollection<Object>> inputCollectionIter = inputDataCollections.values().iterator();
      stageData = inputCollectionIter.next();
      // don't union inputs records if we're joining or if we're processing errors
      while (!BatchJoiner.PLUGIN_TYPE.equals(pluginType) && !ErrorTransform.PLUGIN_TYPE.equals(pluginType)
        && inputCollectionIter.hasNext()) {
        stageData = stageData.union(inputCollectionIter.next());
      }
    }
    return stageData;
  }

  private Map<String, SparkCollection<Object>> getInputDataCollections(PipelinePhase pipelinePhase,
                                                                       Map<String, EmittedRecords> emittedRecords,
                                                                       String stageName, String pluginType,
                                                                       Set<String> stageInputs) {
    Map<String, SparkCollection<Object>> inputDataCollections = new HashMap<>();
    for (String inputStageName : stageInputs) {
      StageSpec inputStageSpec = pipelinePhase.getStage(inputStageName);
      if (inputStageSpec == null) {
        // means the input to this stage is in a separate phase. For example, it is an action.
        continue;
      }
      String port = null;
      // if this stage is a connector like c1.connector, the outputPorts will contain the original 'c1' as
      // a key, but not 'c1.connector'. Similarly, if the input stage is a connector, it won't have any output ports
      // however, this is fine since we know that the output of a connector stage is always normal output,
      // not errors or alerts or output port records
      if (!Constants.Connector.PLUGIN_TYPE.equals(inputStageSpec.getPluginType())
        && !Constants.Connector.PLUGIN_TYPE.equals(pluginType)) {
        port = inputStageSpec.getOutputPorts().get(stageName).getPort();
      }
      SparkCollection<Object> inputRecords = port == null
        ? emittedRecords.get(inputStageName).getOutputRecords() :
        emittedRecords.get(inputStageName).getOutputPortRecords().get(port);
      inputDataCollections.put(inputStageName, inputRecords);
    }
    return inputDataCollections;
  }

  /**
   * Performs tranformation or analytical function.
   * @return updated emitted records builder
   */
  private EmittedRecords transform(Map<String, Integer> stagePartitions, PipelinePhase pipelinePhase,
                                   FunctionCache.Factory functionCacheFactory,
                                   CombinerDag groupedDag, Set<String> branchers, Set<String> shufflers,
                                   String stageName, StageSpec stageSpec, String pluginType,
                                   boolean hasErrorOutput, boolean hasAlertOutput, SparkCollection<Object> stageData,
                                   Map<String, SparkCollection<Object>> inputDataCollections,
                                   StageStatisticsCollector collector,
                                   PluginFunctionContext pluginFunctionContext, Object plugin) throws Exception {

    if (Transform.PLUGIN_TYPE.equals(pluginType)) {

      SparkCollection<RecordInfo<Object>> combinedData = stageData.transform(stageSpec, collector);
      return getEmittedRecords(pipelinePhase, stageSpec,
                               combinedData, groupedDag, branchers, shufflers, hasErrorOutput, hasAlertOutput);

    } else if (SplitterTransform.PLUGIN_TYPE.equals(pluginType)) {

      SparkCollection<RecordInfo<Object>> combinedData = stageData.multiOutputTransform(stageSpec, collector);
      return getEmittedRecords(pipelinePhase, stageSpec,
                               combinedData, groupedDag, branchers, shufflers, hasErrorOutput, hasAlertOutput);

    } else if (SparkCompute.PLUGIN_TYPE.equals(pluginType)) {

      SparkCompute<Object, Object> sparkCompute = (SparkCompute<Object, Object>) plugin;
      SparkCollection<Object> computed = stageData.compute(stageSpec, sparkCompute);
      return getEmittedRecords(pipelinePhase, stageSpec, mapToRecordInfoCollection(stageName, computed),
                               groupedDag, branchers, shufflers, false, false);

    } else if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {

      Integer partitions = stagePartitions.get(stageName);

      if (plugin instanceof BatchReducibleAggregator) {
        SparkCollection<RecordInfo<Object>> combinedData = stageData.reduceAggregate(stageSpec, partitions,
                                                                                     collector);
        return getEmittedRecords(pipelinePhase, stageSpec,
                                 combinedData, groupedDag, branchers, shufflers, hasErrorOutput, hasAlertOutput);
      } else {
        SparkCollection<RecordInfo<Object>> combinedData = stageData.aggregate(stageSpec, partitions, collector);
        return getEmittedRecords(pipelinePhase, stageSpec,
                                 combinedData, groupedDag, branchers, shufflers, hasErrorOutput, hasAlertOutput);
      }

    } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {

      Integer numPartitions = stagePartitions.get(stageName);
      SparkCollection<Object> joined = handleJoin(inputDataCollections, pipelinePhase, pluginFunctionContext,
                                                  stageSpec, functionCacheFactory, plugin,
                                                  numPartitions, collector, shufflers);
      return getEmittedRecords(pipelinePhase, stageSpec,
                               mapToRecordInfoCollection(stageName, joined),
                               groupedDag, branchers, shufflers, false, false);

    } else if (Windower.PLUGIN_TYPE.equals(pluginType)) {

      Windower windower = (Windower) plugin;
      SparkCollection<Object> windowed = stageData.window(stageSpec, windower);
      return getEmittedRecords(pipelinePhase, stageSpec, mapToRecordInfoCollection(stageName, windowed),
                               groupedDag, branchers, shufflers, false, false);

    } else {
      throw new IllegalStateException(String.format("Stage %s is of unsupported plugin type %s.",
                                                    stageName, pluginType));
    }
  }

  /**
   * Tries to do transform as {@see RelationalTransform}
   * @return updated {@link EmittedRecords.Builder} if it was transformed or empty {@link Optional} if transform
   * can't be done in {@see RelationalTransform} way
   */
  private Optional<EmittedRecords> tryRelationalTransform(
    PipelinePhase pipelinePhase, CombinerDag groupedDag, Set<String> branchers, Set<String> shufflers,
    String stageName, StageSpec stageSpec,
    boolean hasErrorOutput, boolean hasAlertOutput, SparkCollection<Object> stageData,
    Map<String, SparkCollection<Object>> inputDataCollections, Object plugin) {

    // If this is a wrapped plugin instance, get the underlying implementation.
    while (plugin instanceof PluginWrapper) {
      plugin = ((PluginWrapper<?>) plugin).getWrapped();
    }

    if (plugin instanceof RelationalTransform) {
      RelationalTransform transform = (RelationalTransform) plugin;

      if (inputDataCollections.size() > 1 && transform.requireUnionInputs()) {
        Iterator<String> keys = inputDataCollections.keySet().iterator();
        String firstKey = keys.next();
        Schema firstSchema = stageSpec.getInputSchemas().get(firstKey);
        SparkCollection<Object> first = inputDataCollections.get(firstKey);

        while (keys.hasNext()) {
          String currKey = keys.next();
          //Validate schema is Same for all
          if (firstSchema.equals(stageSpec.getInputSchemas().get(currKey))) {
            first = first.union(inputDataCollections.get(currKey));
          } else {
            LOG.warn(String.format("There is a schema mismatch between the given inputs for the stage %s, " +
                                     "Pushdown transformation will be skipped for this stage.", stageName));
            return Optional.empty();
          }
        }
        inputDataCollections.clear();
        inputDataCollections.put(firstKey, first);
      }

      for (SparkCollectionRelationalEngine engine : getRelationalEngines(stageSpec, stageData)) {
        if (!transform.canUseEngine(engine.getRelationalEngine())) {
          continue;
        }
        Optional<SparkCollection<Object>> transformedData =
          engine.tryRelationalTransform(stageSpec, transform, inputDataCollections);
        if (transformedData.isPresent()) {
          return Optional.of(getEmittedRecords(pipelinePhase, stageSpec,
                                               mapToRecordInfoCollection(stageName, transformedData.get()),
                                               groupedDag, branchers, shufflers, hasErrorOutput, hasAlertOutput));
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Decides on relational engines to use for given stage
   * @param stageData input collection
   * @return list of engines to try
   */
  protected Iterable<SparkCollectionRelationalEngine> getRelationalEngines(StageSpec stageSpec,
                                                                           SparkCollection<Object> stageData) {
    //TODO CDAP-18608: Add Spark engine here
    return Collections.emptyList();
  }

  private Runnable getGroupRunnable(JavaSparkExecutionContext sec, PhaseSpec phaseSpec,
      Set<String> groupStages, Set<String> groupInputs, Map<String, EmittedRecords> emittedRecords,
      Map<String, StageStatisticsCollector> collectors, long time, DatasetContext datasetContext,
      SinkRunnableProvider sinkRunnableProvider) {
    /*
        with a pipeline like:

             |--> t1 --> k1
        s1 --|
             |--> k2
                  ^
            s2 ---|

        the group will include stages t1, k1, and k2:

        s1 --|
             |--> group1
        s2 --|

        note that although s1 and s2 are both inputs to the group, the records for s2 should only be sent to
        k2 and not to t1. Thus, every record must be tagged with the stage that it came from, so that they can
        be sent to the right underlying stages.
     */
    SparkCollection<RecordInfo<Object>> fullInput = null;
    for (String inputStage : groupInputs) {
      SparkCollection<RecordInfo<Object>> inputData = emittedRecords.get(inputStage).getRawData();
      if (fullInput == null) {
        fullInput = inputData;
        continue;
      }

      fullInput = fullInput.union(inputData);
    }

    // Sets.intersection returns an unserializable Set, so copy it into a HashSet.
    Set<String> groupSinks = new HashSet<>(Sets.intersection(groupStages, phaseSpec.getPhase().getSinks()));
    return sinkRunnableProvider.getGroupSinkRunnable(phaseSpec, fullInput, groupStages, collectors,
        groupSinks);
  }

  protected SparkCollection<Object> handleJoin(Map<String, SparkCollection<Object>> inputDataCollections,
                                               PipelinePhase pipelinePhase, PluginFunctionContext pluginFunctionContext,
                                               StageSpec stageSpec, FunctionCache.Factory functionCacheFactory,
                                               Object plugin, Integer numPartitions,
                                               StageStatisticsCollector collector,
                                               Set<String> shufflers) throws Exception {
    String stageName = stageSpec.getName();
    if (plugin instanceof BatchJoiner) {
      BatchJoiner<Object, Object, Object> joiner = (BatchJoiner<Object, Object, Object>) plugin;
      BatchJoinerRuntimeContext joinerRuntimeContext = pluginFunctionContext.createBatchRuntimeContext();
      joiner.initialize(joinerRuntimeContext);
      shufflers.add(stageName);
      return handleJoin(joiner, inputDataCollections, stageSpec, functionCacheFactory, numPartitions, collector);
    } else if (plugin instanceof AutoJoiner) {
      AutoJoiner autoJoiner = (AutoJoiner) plugin;
      Map<String, Schema> inputSchemas = new HashMap<>();
      for (String inputStageName : pipelinePhase.getStageInputs(stageName)) {
        StageSpec inputStageSpec = pipelinePhase.getStage(inputStageName);
        Port outputPort = inputStageSpec.getOutputPorts().get(stageName);
        if (outputPort == null) {
          inputSchemas.put(inputStageName, null);
        } else {
          inputSchemas.put(inputStageName, outputPort.getSchema());
        }
      }
      FailureCollector failureCollector = new LoggingFailureCollector(stageName, inputSchemas);
      AutoJoinerContext autoJoinerContext = DefaultAutoJoinerContext.from(inputSchemas, failureCollector);

      // joinDefinition will always be non-null because
      // it is checked by PipelinePhasePreparer at the start of the run.
      JoinDefinition joinDefinition = autoJoiner.define(autoJoinerContext);
      failureCollector.getOrThrowException();
      if (joinDefinition.getStages().stream().noneMatch(JoinStage::isBroadcast)) {
        shufflers.add(stageName);
      }
      return handleAutoJoin(stageName, joinDefinition, inputDataCollections, numPartitions);
    } else {
      // should never happen unless there is a bug in the code. should have failed during deployment
      throw new IllegalStateException(String.format("Stage '%s' is an unknown joiner type %s",
                                                    stageName, plugin.getClass().getName()));
    }
  }

  protected SparkCollection<Object> handleAutoJoin(String stageName, JoinDefinition joinDefinition,
                                                   Map<String, SparkCollection<Object>> inputDataCollections,
                                                   @Nullable Integer numPartitions) {
    JoinCondition.Op conditionType = joinDefinition.getCondition().getOp();
    if (conditionType == JoinCondition.Op.KEY_EQUALITY) {
      return handleAutoJoinOnKeys(stageName, joinDefinition, inputDataCollections, numPartitions);
    } else if (conditionType == JoinCondition.Op.EXPRESSION) {
      return handleAutoJoinWithSQL(stageName, joinDefinition, inputDataCollections);
    }

    // should never happen
    throw new IllegalStateException(conditionType + " join conditions are not supported");
  }

  /**
   * The purpose of this method is to collect various pieces of information together into a JoinRequest.
   * This amounts to gathering the SparkCollection, schema, join key, and join type for each stage involved in the join.
   */
  private SparkCollection<Object> handleAutoJoinOnKeys(String stageName, JoinDefinition joinDefinition,
                                                       Map<String, SparkCollection<Object>> inputDataCollections,
                                                       @Nullable Integer numPartitions) {
    // sort stages to join so that broadcasts happen last. This is to ensure that the left side is not a broadcast
    // so that we don't try to broadcast both sides of the join. It also causes less data to be shuffled for the
    // non-broadcast joins.
    List<JoinStage> joinOrder = new ArrayList<>(joinDefinition.getStages());
    joinOrder.sort((s1, s2) -> {
      if (s1.isBroadcast() && !s2.isBroadcast()) {
        return 1;
      } else if (!s1.isBroadcast() && s2.isBroadcast()) {
        return -1;
      }
      return 0;
    });

    Iterator<JoinStage> stageIter = joinOrder.iterator();
    JoinStage left = stageIter.next();
    String leftName = left.getStageName();
    SparkCollection<Object> leftCollection = inputDataCollections.get(left.getStageName());
    Schema leftSchema = left.getSchema();

    JoinCondition condition = joinDefinition.getCondition();
    JoinCondition.OnKeys onKeys = (JoinCondition.OnKeys) condition;

    // If this is a join on A.x = B.y = C.z and A.k = B.k = C.k, then stageKeys will look like:
    // A -> [x, k]
    // B -> [y, k]
    // C -> [z, k]
    Map<String, List<String>> stageKeys = onKeys.getKeys().stream()
      .collect(Collectors.toMap(JoinKey::getStageName, JoinKey::getFields));

    Schema outputSchema = joinDefinition.getOutputSchema();
    // if the output schema is null it means it could not be generated because some of the input schemas are null.
    // there isn't a reliable way to get the schema from the data at this point, so we require the user to have
    // provided the output schema directly
    // it is technically possible to take a single StructuredRecord from each input to determine the schema,
    // but this approach will not work if the input RDD is empty.
    // in addition, RDD.take(1) and RDD.isEmpty() both trigger a Spark job, which is not desirable.
    // when we properly propagate schema at runtime, this condition should no longer happen
    if (outputSchema == null) {
      throw new IllegalArgumentException(
        String.format("Joiner stage '%s' cannot calculate its output schema because "
                        + "one or more inputs have dynamic or unknown schema. "
                        + "An output schema must be directly provided.", stageName));
    }
    List<JoinCollection> toJoin = new ArrayList<>();
    List<Schema> keySchema = null;
    while (stageIter.hasNext()) {
      // in this loop, information for each stage to be joined is gathered together into a JoinCollection
      JoinStage right = stageIter.next();
      String rightName = right.getStageName();
      Schema rightSchema = right.getSchema();
      List<String> key = stageKeys.get(rightName);
      if (rightSchema == null) {
        if (keySchema == null) {
          keySchema = deriveKeySchema(stageName, stageKeys, joinDefinition);
        }
        // if the schema is not known, generate it from the provided output schema and the selected fields
        rightSchema = deriveInputSchema(stageName, rightName, key, keySchema,
                                        joinDefinition.getSelectedFields(), joinDefinition.getOutputSchema());
      } else {
        // drop fields that aren't included in the final output
        // don't need to do this if the schema was derived, since it will already only contain
        // fields in the output schema
        Set<String> requiredFields = new HashSet<>(key);
        for (JoinField joinField : joinDefinition.getSelectedFields()) {
          if (!joinField.getStageName().equals(rightName)) {
            continue;
          }
          requiredFields.add(joinField.getFieldName());
        }
        rightSchema = trimSchema(rightSchema, requiredFields);
      }

      // JoinCollection contains the stage name,  SparkCollection, schema, joinkey,
      // whether it's required, and whether to broadcast
      toJoin.add(new JoinCollection(rightName, inputDataCollections.get(rightName), rightSchema, key,
                                    right.isRequired(), right.isBroadcast()));
    }

    List<String> leftKey = stageKeys.get(leftName);
    if (leftSchema == null) {
      if (keySchema == null) {
        keySchema = deriveKeySchema(stageName, stageKeys, joinDefinition);
      }
      leftSchema = deriveInputSchema(stageName, leftName, leftKey, keySchema,
                                     joinDefinition.getSelectedFields(), joinDefinition.getOutputSchema());
    }

    // JoinRequest contains the left side of the join, plus 1 or more other stages to join to.
    JoinRequest joinRequest = new JoinRequest(stageName, leftName, leftKey, leftSchema,
                                              left.isRequired(), onKeys.isNullSafe(),
                                              joinDefinition.getSelectedFields(),
                                              joinDefinition.getOutputSchema(), toJoin, numPartitions,
                                              joinDefinition.getDistribution(), joinDefinition);
    return leftCollection.join(joinRequest);
  }

  /*
      Implement a join by generating a SQL query that Spark will execute.
      Joins on key equality are not implemented this way because they have special repartitioning
      that allows them to specify a different number of partitions for different joins in the same pipeline.

      When Spark handles SQL queries, it uses spark.sql.shuffle.partitions number of partitions, which is a global
      setting that applies to any SQL join in the pipeline.
   */
  private SparkCollection<Object> handleAutoJoinWithSQL(String stageName, JoinDefinition joinDefinition,
                                                        Map<String, SparkCollection<Object>> inputDataCollections) {
    JoinCondition.OnExpression condition = (JoinCondition.OnExpression) joinDefinition.getCondition();
    Map<String, String> aliases = condition.getDatasetAliases();

    // earlier validation ensure there are exactly 2 inputs being joined
    JoinStage leftStage = joinDefinition.getStages().get(0);
    JoinStage rightStage = joinDefinition.getStages().get(1);
    String leftStageName = leftStage.getStageName();
    String rightStageName = rightStage.getStageName();

    SparkCollection<Object> leftData = inputDataCollections.get(leftStageName);
    JoinCollection leftCollection = new JoinCollection(leftStageName, inputDataCollections.get(leftStageName),
                                                       leftStage.getSchema(), Collections.emptyList(),
                                                       leftStage.isRequired(), leftStage.isBroadcast());
    JoinCollection rightCollection = new JoinCollection(rightStageName, inputDataCollections.get(rightStageName),
                                                        rightStage.getSchema(), Collections.emptyList(),
                                                        rightStage.isRequired(), rightStage.isBroadcast());
    JoinExpressionRequest joinRequest = new JoinExpressionRequest(stageName, joinDefinition.getSelectedFields(),
                                                                  leftCollection, rightCollection, condition,
                                                                  joinDefinition.getOutputSchema(), joinDefinition);

    return leftData.join(joinRequest);
  }

  /**
   * Derive the key schema based on the provided output schema and the final list of selected fields.
   * This is not possible if the key is not present in the output schema, in which case the user will need to
   * add it to the output schema. However, this is an unlikely scenario as most joins will want to
   * preserve the key.
   */
  @VisibleForTesting
  static List<Schema> deriveKeySchema(String joinerStageName, Map<String, List<String>> keys,
                                      JoinDefinition joinDefinition) {
    int numKeyFields = keys.values().iterator().next().size();

    // (stage, field) -> JoinField
    Table<String, String, JoinField> fieldTable = HashBasedTable.create();
    for (JoinField joinField : joinDefinition.getSelectedFields()) {
      fieldTable.put(joinField.getStageName(), joinField.getFieldName(), joinField);
    }

    /*
        Suppose the join keys are:

        A, (x, y)
        B, (x, z))

        and selected fields are:

        A.x as id, A.y as name, B.z as item
     */
    Schema outputSchema = joinDefinition.getOutputSchema();
    List<Schema> keySchema = new ArrayList<>(numKeyFields);
    for (int i = 0; i < numKeyFields; i++) {
      keySchema.add(null);
    }
    for (Map.Entry<String, List<String>> keyEntry : keys.entrySet()) {
      String keyStage = keyEntry.getKey();
      int keyFieldNum = 0;
      for (String keyField : keyEntry.getValue()) {
        // If key field is A.x, this will fetch (A.x as id)
        // the JoinField might not exist. For example, B.x will not find anything in the output
        JoinField selectedKeyField = fieldTable.get(keyStage, keyField);
        if (selectedKeyField == null) {
          continue;
        }

        // if the key field is A.x, JoinField is (A.x as id), and outputField is the 'id' field in the output schema
        String outputFieldName = selectedKeyField.getAlias() == null
          ? selectedKeyField.getFieldName() : selectedKeyField.getAlias();
        Schema.Field outputField = outputSchema.getField(outputFieldName);

        if (outputField == null) {
          // this is an invalid join definition
          throw new IllegalArgumentException(
            String.format("Joiner stage '%s' provided an invalid definition. "
                            + "The output schema does not contain a field for selected field '%s'.'%s'%s",
                          joinerStageName, keyStage, selectedKeyField.getFieldName(),
                          selectedKeyField.getAlias() == null ? "" : "as " + selectedKeyField.getAlias()));
        }

        // make the schema nullable because one stage might have it as non-nullable
        // while another stage has it as nullable.
        // for example, when joining on A.id = B.id,
        // A.id might not be nullable, but B.id could be.
        // this will never be exposed to the user since the final output will use whatever schema that was provided
        // by the user.
        Schema keyFieldSchema = outputField.getSchema();
        if (!keyFieldSchema.isNullable()) {
          keyFieldSchema = Schema.nullableOf(keyFieldSchema);
        }

        Schema existingSchema = keySchema.get(keyFieldNum);
        if (existingSchema != null && existingSchema.isSimpleOrNullableSimple()
          && !Schemas.equalsIgnoringRecordName(existingSchema, keyFieldSchema)) {
          // this is an invalid join definition
          // this condition is normally checked at deployment time,
          // but it will be skipped if the input schema is not known.
          throw new IllegalArgumentException(
            String.format("Joiner stage '%s' has mismatched join key types. "
                            + "Key field '%s' from stage '%s' has a different than another stage.",
                          joinerStageName, keyField, keyStage));
        }
        keySchema.set(keyFieldNum, keyFieldSchema);
        keyFieldNum++;
      }
    }
    for (int i = 0; i < numKeyFields; i++) {
      Schema keyFieldSchema = keySchema.get(i);
      if (keyFieldSchema == null) {
        throw new IllegalArgumentException(
          String.format("Joiner stage '%s' has inputs with dynamic or unknown schema. "
                          + "Unable to derive the schema for key field #%d. "
                          + "Please include all key fields in the output schema.",
                        joinerStageName, i + 1));
      }
    }
    return keySchema;
  }

  /**
   * Derive the schema for an input stage based on the known output schema and the final list of selected fields.
   * For example, if the final output schema is:
   *
   *   purchase_id (int), user_id (int), user_name (string)
   *
   * and the selected fields are:
   *
   *   A.id as purchase_id, B.id as user_id, B.name as user_name
   *
   * then the schema for A can be determined to be:
   *
   *   id (int)
   *
   * and the schema for B can be determined to be:
   *
   *   id (int), name (string)
   */
  @VisibleForTesting
  static Schema deriveInputSchema(String joinerStageName, String inputStageName, List<String> key,
                                  List<Schema> keySchema, List<JoinField> selectedFields, Schema outputSchema) {
    List<Schema.Field> stageFields = new ArrayList<>();
    Iterator<String> keyIter = key.iterator();
    Iterator<Schema> keySchemaIter = keySchema.iterator();
    while (keyIter.hasNext()) {
      stageFields.add(Schema.Field.of(keyIter.next(), keySchemaIter.next()));
    }

    Set<String> keySet = new HashSet<>(key);
    for (JoinField selectedField : selectedFields) {
      if (!selectedField.getStageName().equals(inputStageName)) {
        continue;
      }
      String preAliasedName = selectedField.getFieldName();
      if (keySet.contains(preAliasedName)) {
        continue;
      }
      String outputName = selectedField.getAlias() == null ? preAliasedName : selectedField.getAlias();
      Schema.Field outputField = outputSchema.getField(outputName);
      if (outputField == null) {
        throw new IllegalArgumentException(
          String.format("Joiner stage '%s' provided an invalid definition. "
                          + "The output schema does not contain a field for selected field '%s'.'%s'%s",
                        joinerStageName, selectedField.getStageName(), preAliasedName,
                        preAliasedName == null ? "" : "as " + preAliasedName));
      }
      stageFields.add(Schema.Field.of(preAliasedName, outputField.getSchema()));
    }
    return Schema.recordOf(inputStageName, stageFields);
  }

  /**
   * Remove fields from the given schema that are not eventually selected in the final output.
   * This is done to trim down the amount of data that needs to be processed in the join.
   */
  private static Schema trimSchema(Schema stageSchema, Set<String> requiredFields) {
    List<Schema.Field> trimmed = new ArrayList<>();
    for (Schema.Field field : stageSchema.getFields()) {
      if (requiredFields.contains(field.getName())) {
        trimmed.add(field);
      }
    }
    return Schema.recordOf(stageSchema.getRecordName(), trimmed);
  }

  protected SparkCollection<Object> handleJoin(BatchJoiner<?, ?, ?> joiner,
                                               Map<String, SparkCollection<Object>> inputDataCollections,
                                               StageSpec stageSpec,
                                               FunctionCache.Factory functionCacheFactory,
                                               Integer numPartitions,
                                               StageStatisticsCollector collector) throws Exception {
    Map<String, SparkPairCollection<Object, Object>> preJoinStreams = new HashMap<>();
    for (Map.Entry<String, SparkCollection<Object>> inputStreamEntry : inputDataCollections.entrySet()) {
      String inputStage = inputStreamEntry.getKey();
      SparkCollection<Object> inputStream = inputStreamEntry.getValue();
      preJoinStreams.put(inputStage, addJoinKey(stageSpec, functionCacheFactory, inputStage, inputStream, collector));
    }

    Set<String> remainingInputs = new HashSet<>();
    remainingInputs.addAll(inputDataCollections.keySet());

    SparkPairCollection<Object, List<JoinElement<Object>>> joinedInputs = null;
    // inner join on required inputs
    for (final String inputStageName : joiner.getJoinConfig().getRequiredInputs()) {
      SparkPairCollection<Object, Object> preJoinCollection = preJoinStreams.get(inputStageName);

      if (joinedInputs == null) {
        joinedInputs = preJoinCollection.mapValues(new InitialJoinFunction<>(inputStageName));
      } else {
        JoinFlattenFunction<Object> joinFlattenFunction = new JoinFlattenFunction<>(inputStageName);
        joinedInputs = numPartitions == null
          ? joinedInputs.join(preJoinCollection).mapValues(joinFlattenFunction) :
          joinedInputs.join(preJoinCollection, numPartitions).mapValues(joinFlattenFunction);
      }
      remainingInputs.remove(inputStageName);
    }

    // outer join on non-required inputs
    boolean isFullOuter = joinedInputs == null;
    for (final String inputStageName : remainingInputs) {
      SparkPairCollection<Object, Object> preJoinStream = preJoinStreams.get(inputStageName);

      if (joinedInputs == null) {
        joinedInputs = preJoinStream.mapValues(new InitialJoinFunction<>(inputStageName));
      } else {
        if (isFullOuter) {
          OuterJoinFlattenFunction<Object> flattenFunction = new OuterJoinFlattenFunction<>(inputStageName);

          joinedInputs = numPartitions == null
            ? joinedInputs.fullOuterJoin(preJoinStream).mapValues(flattenFunction) :
            joinedInputs.fullOuterJoin(preJoinStream, numPartitions).mapValues(flattenFunction);
        } else {
          LeftJoinFlattenFunction<Object> flattenFunction = new LeftJoinFlattenFunction<>(inputStageName);

          joinedInputs = numPartitions == null
            ? joinedInputs.leftOuterJoin(preJoinStream).mapValues(flattenFunction) :
            joinedInputs.leftOuterJoin(preJoinStream, numPartitions).mapValues(flattenFunction);
        }
      }
    }

    // should never happen, but removes warnings
    if (joinedInputs == null) {
      throw new IllegalStateException("There are no inputs into join stage " + stageSpec.getName());
    }

    return mergeJoinResults(stageSpec, functionCacheFactory, joinedInputs, collector);
  }


  /**
   * A stage should be cached if it prevents a source from being recomputed. For example:
   *
   *                        |--> agg1 --> sink1
   * source --> transform --|
   *                        |--> agg2 --> sink2
   *
   * The output of the transform stage should be cached, otherwise the two branches would cause the source to be
   * recomputed. However, if the pipeline was a little different:
   *
   *                   |--> agg1 --> sink1
   * source --> agg0 --|
   *                   |--> agg2 --> sink
   *
   * Nothing needs to be cached here because agg0 is an aggregator, which involves a data shuffle.
   * When there is a shuffle, Spark knows to re-read from the shuffle data instead of the source.
   * Aggregators and non-broadcast joins will shuffle data.
   *
   * Cache points can't be pre-computed because we don't know which joins are broadcast joins until the
   * actual plugin is instantiated and the JoinDefinition is fetched from the plugin. This method assumes it
   * will be called on stages in topological order, where any parent that is a joiner will be included in the
   * provided shufflers set.
   */
  private boolean shouldCache(Dag dag,
                              String stageName,
                              Set<String> branchers,
                              Set<String> shufflers,
                              SparkCollection<RecordInfo<Object>> stageData) {
    if (!branchers.contains(stageName) || shufflers.contains(stageName)) {
      return false;
    }

    // Skip caching for SQL engine collections
    if (stageData instanceof SQLBackedCollection) {
      return false;
    }

    // the stage is a non-shuffle stage with multiple outputs.
    // check if there is a path from this stage back to a source without passing through another branch point
    // or a shuffler. If so, this stage needs to be cached.
    Set<String> stopNodes = new HashSet<>(branchers);
    stopNodes.addAll(shufflers);
    Set<String> parents = dag.parentsOf(stageName, stopNodes);
    return !Sets.intersection(dag.getSources(), parents).isEmpty();
  }

  /**
   * Wraps a Spark Collection with RecordInfo for the stage.
   *
   * @param stageName name of the stage to use.
   * @param collection Collection to use.
   * @return Instance of a spark collection with RecordInfo attached to output records.
   */
  protected SparkCollection<RecordInfo<Object>> mapToRecordInfoCollection(String stageName,
                                                                        SparkCollection<Object> collection) {
    // For SQLEngineCollection or WrappedSparkCollection, we wrap the collection in order to not force a
    // premature/unnecessary pull operation from the SQL engine.
    if (collection instanceof SQLBackedCollection) {
      return new WrappedSQLEngineCollection<>((SQLBackedCollection<Object>) collection,
                                              (c) -> c.map(new RecordInfoWrapper<>(stageName)));
    }

    return collection.map(new RecordInfoWrapper<>(stageName));
  }

  protected EmittedRecords getEmittedRecords(PipelinePhase pipelinePhase,
                                             StageSpec stageSpec, SparkCollection<RecordInfo<Object>> stageData,
                                             Dag dag, Set<String> branchers, Set<String> shufflers,
                                             boolean hasErrors, boolean hasAlerts) {
    EmittedRecords.Builder builder = EmittedRecords.builder();
    builder.setRawData(stageData);

    if (shouldCache(dag, stageSpec.getName(), branchers, shufflers, stageData)) {
      stageData = stageData.cache();
    }

    if (hasErrors) {
      SparkCollection<ErrorRecord<Object>> errors =
        stageData.flatMap(stageSpec, new ErrorPassFilter<Object>());
      builder.setErrors(errors);
    }
    if (hasAlerts) {
      SparkCollection<Alert> alerts = stageData.flatMap(stageSpec, new AlertPassFilter());
      builder.setAlerts(alerts);
    }

    if (SplitterTransform.PLUGIN_TYPE.equals(stageSpec.getPluginType())) {
      // set collections for each port, implemented as a filter on the port.
      for (StageSpec.Port portSpec : stageSpec.getOutputPorts().values()) {
        String port = portSpec.getPort();
        SparkCollection<Object> portData = filterPortRecords(stageSpec, stageData, port);
        builder.addPort(port, portData);
      }
    } else {
      SparkCollection<Object> outputs = filterPortRecords(stageSpec, stageData, null);
      builder.setOutput(outputs);
    }

    return builder.build();
  }

  /**
   * Filter output records for a given port using an {@link OutputPassFilter}.
   *
   * @param stageSpec Pipeline Stage specification.
   * @param stageData Spark Collection.
   * @param port port to use for filtering, can be null.
   * @return {@link SparkCollection} after the filter has been applied.
   */
  private SparkCollection<Object> filterPortRecords(StageSpec stageSpec,
                                                    SparkCollection<RecordInfo<Object>> stageData,
                                                    @Nullable String port) {
    // Port filtering can only be applied to SQL Backed Collections when there is no output port specified.
    // This is due to the fact that records need to be present in Spark to apply this filtering operation.
    if (port == null && stageData instanceof SQLBackedCollection) {
      return new WrappedSQLEngineCollection<>((SQLBackedCollection<RecordInfo<Object>>) stageData,
                                              (c) -> c.flatMap(stageSpec, new OutputPassFilter<>(port)));
    }

    return stageData.flatMap(stageSpec, new OutputPassFilter<>(port));
  }

}
