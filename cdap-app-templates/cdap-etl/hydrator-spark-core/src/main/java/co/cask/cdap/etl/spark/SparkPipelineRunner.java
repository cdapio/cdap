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

package co.cask.cdap.etl.spark;

import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.ErrorRecord;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.streaming.Windower;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.plugin.PipelinePluginContext;
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.etl.spark.function.BatchSinkFunction;
import co.cask.cdap.etl.spark.function.ErrorFilter;
import co.cask.cdap.etl.spark.function.ErrorTransformFunction;
import co.cask.cdap.etl.spark.function.InitialJoinFunction;
import co.cask.cdap.etl.spark.function.JoinFlattenFunction;
import co.cask.cdap.etl.spark.function.LeftJoinFlattenFunction;
import co.cask.cdap.etl.spark.function.OuterJoinFlattenFunction;
import co.cask.cdap.etl.spark.function.OutputFilter;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base Spark program to run a Hydrator pipeline.
 */
public abstract class SparkPipelineRunner {

  protected abstract SparkCollection<Tuple2<Boolean, Object>> getSource(StageInfo stageInfo) throws Exception;


  protected abstract SparkPairCollection<Object, Object> addJoinKey(
    StageInfo stageInfo, String inputStageName,
    SparkCollection<Object> inputCollection) throws Exception;

  protected abstract SparkCollection<Object> mergeJoinResults(
    StageInfo stageInfo,
    SparkPairCollection<Object, List<JoinElement<Object>>> joinedInputs) throws Exception;

  public void runPipeline(PipelinePhase pipelinePhase, String sourcePluginType,
                          JavaSparkExecutionContext sec,
                          Map<String, Integer> stagePartitions) throws Exception {

    MacroEvaluator macroEvaluator =
      new DefaultMacroEvaluator(sec.getWorkflowToken(), sec.getRuntimeArguments(), sec.getLogicalStartTime(), sec,
                                sec.getNamespace());
    Map<String, SparkCollection<Object>> stageDataCollections = new HashMap<>();
    Map<String, SparkCollection<ErrorRecord<Object>>> stageErrorCollections = new HashMap<>();
    PluginContext pluginContext = new PipelinePluginContext(sec.getPluginContext(), sec.getMetrics());

    // should never happen, but removes warning
    if (pipelinePhase.getDag() == null) {
      throw new IllegalStateException("Pipeline phase has no connections.");
    }

    for (String stageName : pipelinePhase.getDag().getTopologicalOrder()) {
      StageInfo stageInfo = pipelinePhase.getStage(stageName);
      //noinspection ConstantConditions
      String pluginType = stageInfo.getPluginType();

      // don't want to do an additional filter for stages that can emit errors,
      // but aren't connected to an ErrorTransform
      boolean hasErrorOutput = false;
      Set<String> outputs = pipelinePhase.getStageOutputs(stageInfo.getName());
      for (String output : outputs) {
        //noinspection ConstantConditions
        if (ErrorTransform.PLUGIN_TYPE.equals(pipelinePhase.getStage(output).getPluginType())) {
          hasErrorOutput = true;
          break;
        }
      }

      SparkCollection<Object> stageData = null;

      Map<String, SparkCollection<Object>> inputDataCollections = new HashMap<>();
      Set<String> stageInputs = stageInfo.getInputs();
      for (String inputStageName : stageInputs) {
        inputDataCollections.put(inputStageName, stageDataCollections.get(inputStageName));
      }

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

      SparkCollection<ErrorRecord<Object>> stageErrors = null;
      PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageInfo, sec);
      if (stageData == null) {

        // this if-else is nested inside the stageRDD null check to avoid warnings about stageRDD possibly being
        // null in the other else-if conditions
        if (sourcePluginType.equals(pluginType)) {
          SparkCollection<Tuple2<Boolean, Object>> combinedData = getSource(stageInfo);
          if (hasErrorOutput) {
            // need to cache, otherwise the stage can be computed twice, once for output and once for errors.
            combinedData.cache();
            stageErrors = combinedData.flatMap(stageInfo, new OutputFilter<>());
          }
          stageData = combinedData.flatMap(stageInfo, new ErrorFilter<>());
        } else {
          throw new IllegalStateException(String.format("Stage '%s' has no input and is not a source.", stageName));
        }

      } else if (BatchSink.PLUGIN_TYPE.equals(pluginType)) {

        stageData.store(stageInfo, new BatchSinkFunction(pluginFunctionContext));

      } else if (Transform.PLUGIN_TYPE.equals(pluginType)) {

        SparkCollection<Tuple2<Boolean, Object>> combinedData = stageData.transform(stageInfo);
        if (hasErrorOutput) {
          // need to cache, otherwise the stage can be computed twice, once for output and once for errors.
          combinedData.cache();
          stageErrors = combinedData.flatMap(stageInfo, new OutputFilter<>());
        }
        stageData = combinedData.flatMap(stageInfo, new ErrorFilter<>());

      } else if (ErrorTransform.PLUGIN_TYPE.equals(pluginType)) {

        // union all the errors coming into this stage
        SparkCollection<ErrorRecord<Object>> inputErrors = null;
        for (String inputStage : stageInputs) {
          SparkCollection<ErrorRecord<Object>> inputErrorsFromStage = stageErrorCollections.get(inputStage);
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
          SparkCollection<Tuple2<Boolean, Object>> combinedData =
            inputErrors.flatMap(stageInfo, new ErrorTransformFunction<>(pluginFunctionContext));
          if (hasErrorOutput) {
            // need to cache, otherwise the stage can be computed twice, once for output and once for errors.
            combinedData.cache();
            stageErrors = combinedData.flatMap(stageInfo, new OutputFilter<>());
          }
          stageData = combinedData.flatMap(stageInfo, new ErrorFilter<>());
        }

      } else if (SparkCompute.PLUGIN_TYPE.equals(pluginType)) {

        SparkCompute<Object, Object> sparkCompute = pluginContext.newPluginInstance(stageName, macroEvaluator);
        stageData = stageData.compute(stageInfo, sparkCompute);

      } else if (SparkSink.PLUGIN_TYPE.equals(pluginType)) {

        SparkSink<Object> sparkSink = pluginContext.newPluginInstance(stageName, macroEvaluator);
        stageData.store(stageInfo, sparkSink);

      } else if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {

        Integer partitions = stagePartitions.get(stageName);
        SparkCollection<Tuple2<Boolean, Object>> combinedData = stageData.aggregate(stageInfo, partitions);
        if (hasErrorOutput) {
          // need to cache, otherwise the stage can be computed twice, once for output and once for errors.
          combinedData.cache();
          stageErrors = combinedData.flatMap(stageInfo, new OutputFilter<>());
        }
        stageData = combinedData.flatMap(stageInfo, new ErrorFilter<>());

      } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {

        BatchJoiner<Object, Object, Object> joiner = pluginContext.newPluginInstance(stageName, macroEvaluator);
        BatchJoinerRuntimeContext joinerRuntimeContext = pluginFunctionContext.createBatchRuntimeContext();
        joiner.initialize(joinerRuntimeContext);

        Map<String, SparkPairCollection<Object, Object>> preJoinStreams = new HashMap<>();
        for (Map.Entry<String, SparkCollection<Object>> inputStreamEntry : inputDataCollections.entrySet()) {
          String inputStage = inputStreamEntry.getKey();
          SparkCollection<Object> inputStream = inputStreamEntry.getValue();
          preJoinStreams.put(inputStage, addJoinKey(stageInfo, inputStage, inputStream));
        }

        Set<String> remainingInputs = new HashSet<>();
        remainingInputs.addAll(inputDataCollections.keySet());

        Integer numPartitions = stagePartitions.get(stageName);

        SparkPairCollection<Object, List<JoinElement<Object>>> joinedInputs = null;
        // inner join on required inputs
        for (final String inputStageName : joiner.getJoinConfig().getRequiredInputs()) {
          SparkPairCollection<Object, Object> preJoinCollection = preJoinStreams.get(inputStageName);

          if (joinedInputs == null) {
            joinedInputs = preJoinCollection.mapValues(new InitialJoinFunction<>(inputStageName));
          } else {
            JoinFlattenFunction<Object> joinFlattenFunction = new JoinFlattenFunction<>(inputStageName);
            joinedInputs = numPartitions == null ?
              joinedInputs.join(preJoinCollection).mapValues(joinFlattenFunction) :
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

              joinedInputs = numPartitions == null ?
                joinedInputs.fullOuterJoin(preJoinStream).mapValues(flattenFunction) :
                joinedInputs.fullOuterJoin(preJoinStream, numPartitions).mapValues(flattenFunction);
            } else {
              LeftJoinFlattenFunction<Object> flattenFunction = new LeftJoinFlattenFunction<>(inputStageName);

              joinedInputs = numPartitions == null ?
                joinedInputs.leftOuterJoin(preJoinStream).mapValues(flattenFunction) :
                joinedInputs.leftOuterJoin(preJoinStream, numPartitions).mapValues(flattenFunction);
            }
          }
        }

        // should never happen, but removes warnings
        if (joinedInputs == null) {
          throw new IllegalStateException("There are no inputs into join stage " + stageName);
        }

        stageData = mergeJoinResults(stageInfo, joinedInputs).cache();

      } else if (Windower.PLUGIN_TYPE.equals(pluginType)) {

        Windower windower = pluginContext.newPluginInstance(stageName, macroEvaluator);
        stageData = stageData.window(stageInfo, windower);

      } else {
        throw new IllegalStateException(String.format("Stage %s is of unsupported plugin type %s.",
                                                      stageName, pluginType));
      }

      if (shouldCache(pipelinePhase, stageInfo)) {
        stageData = stageData.cache();
        if (stageErrors != null) {
          stageErrors = stageErrors.cache();
        }
      }
      stageDataCollections.put(stageName, stageData);
      stageErrorCollections.put(stageName, stageErrors);
    }
  }

  // return whether this stage should be cached to avoid recomputation
  private boolean shouldCache(PipelinePhase pipelinePhase, StageInfo stageInfo) {

    // cache this RDD if it has multiple outputs,
    // otherwise computation of each output may trigger recomputing this stage
    Set<String> outputs = pipelinePhase.getStageOutputs(stageInfo.getName());
    if (outputs.size() > 1) {
      return true;
    }

    // cache this stage if it is an input to a stage that has multiple inputs.
    // otherwise the union computation may trigger recomputing this stage
    for (String outputStageName : outputs) {
      StageInfo outputStage = pipelinePhase.getStage(outputStageName);
      //noinspection ConstantConditions
      if (outputStage.getInputs().size() > 1) {
        return true;
      }
    }

    return false;
  }
}
