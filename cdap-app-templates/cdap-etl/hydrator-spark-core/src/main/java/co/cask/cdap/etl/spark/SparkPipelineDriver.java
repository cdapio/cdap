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
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
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
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.etl.spark.function.AggregatorAggregateFunction;
import co.cask.cdap.etl.spark.function.AggregatorGroupByFunction;
import co.cask.cdap.etl.spark.function.BatchSinkFunction;
import co.cask.cdap.etl.spark.function.InitialJoinFunction;
import co.cask.cdap.etl.spark.function.JoinFlattenFunction;
import co.cask.cdap.etl.spark.function.JoinMergeFunction;
import co.cask.cdap.etl.spark.function.JoinOnFunction;
import co.cask.cdap.etl.spark.function.LeftJoinFlattenFunction;
import co.cask.cdap.etl.spark.function.OuterJoinFlattenFunction;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;
import co.cask.cdap.etl.spark.function.TransformFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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
public abstract class SparkPipelineDriver {

  protected abstract SparkCollection<Object> getSource(String stageName,
                                                       PluginFunctionContext pluginFunctionContext) throws Exception;

  public void runPipeline(PipelinePhase pipelinePhase, String sourcePluginType,
                          JavaSparkExecutionContext sec,
                          Map<String, Integer> stagePartitions) throws Exception {

    MacroEvaluator macroEvaluator =
      new DefaultMacroEvaluator(sec.getWorkflowToken(), sec.getRuntimeArguments(), sec.getLogicalStartTime(), sec,
                                sec.getNamespace());
    Map<String, SparkCollection<Object>> stageDataCollections = new HashMap<>();

    // should never happen, but removes warning
    if (pipelinePhase.getDag() == null) {
      throw new IllegalStateException("Pipeline phase has no connections.");
    }

    for (String stageName : pipelinePhase.getDag().getTopologicalOrder()) {
      StageInfo stageInfo = pipelinePhase.getStage(stageName);
      String pluginType = stageInfo.getPluginType();

      SparkCollection<Object> stageData = null;

      Map<String, SparkCollection<Object>> inputDataCollections = new HashMap<>();
      for (String inputStageName : stageInfo.getInputs()) {
        inputDataCollections.put(inputStageName, stageDataCollections.get(inputStageName));
      }

      // if this stage has multiple inputs, and is not a joiner plugin,
      // initialize the stageRDD as the union of all input RDDs.
      if (!inputDataCollections.isEmpty()) {
        Iterator<SparkCollection<Object>> inputCollectionIter = inputDataCollections.values().iterator();
        stageData = inputCollectionIter.next();
        // don't union if we're joining
        while (!BatchJoiner.PLUGIN_TYPE.equals(pluginType) && inputCollectionIter.hasNext()) {
          stageData = stageData.union(inputCollectionIter.next());
        }
      }

      PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageName, sec, pipelinePhase);
      if (stageData == null) {

        // this if-else is nested inside the stageRDD null check to avoid warnings about stageRDD possibly being
        // null in the other else-if conditions
        if (sourcePluginType.equals(pluginType)) {
          stageData = getSource(stageName, pluginFunctionContext);
        } else {
          throw new IllegalStateException(String.format("Stage '%s' has no input and is not a source.", stageName));
        }

      } else if (BatchSink.PLUGIN_TYPE.equals(pluginType)) {

        stageData.store(stageName, new BatchSinkFunction(pluginFunctionContext));

      } else if (Transform.PLUGIN_TYPE.equals(pluginType)) {

        stageData = stageData.flatMap(new TransformFunction(pluginFunctionContext));

      } else if (SparkCompute.PLUGIN_TYPE.equals(pluginType)) {

        SparkCompute<Object, Object> sparkCompute =
          sec.getPluginContext().newPluginInstance(stageName, macroEvaluator);
        stageData = stageData.compute(stageName, sparkCompute);

      } else if (SparkSink.PLUGIN_TYPE.equals(pluginType)) {

        SparkSink<Object> sparkSink = sec.getPluginContext().newPluginInstance(stageName, macroEvaluator);
        stageData.store(stageName, sparkSink);

      } else if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {

        PairFlatMapFunction<Object, Object, Object> groupByFunction =
          new AggregatorGroupByFunction(pluginFunctionContext);
        FlatMapFunction<Tuple2<Object, Iterable<Object>>, Object> aggregateFunction =
          new AggregatorAggregateFunction(pluginFunctionContext);

        Integer partitions = stagePartitions.get(stageName);
        SparkPairCollection<Object, Object> keyedCollection = stageData.flatMapToPair(groupByFunction);

        SparkPairCollection<Object, Iterable<Object>> groupedCollection = partitions == null ?
          keyedCollection.groupByKey() : keyedCollection.groupByKey(partitions);
        stageData = groupedCollection.flatMap(aggregateFunction);

      } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {

        BatchJoiner<Object, Object, Object> joiner =
          sec.getPluginContext().newPluginInstance(stageName, macroEvaluator);
        BatchJoinerRuntimeContext joinerRuntimeContext = pluginFunctionContext.createJoinerRuntimeContext();
        joiner.initialize(joinerRuntimeContext);

        Map<String, SparkPairCollection<Object, Object>> preJoinStreams = new HashMap<>();
        for (Map.Entry<String, SparkCollection<Object>> inputStreamEntry : inputDataCollections.entrySet()) {
          String inputStage = inputStreamEntry.getKey();
          SparkCollection<Object> inputStream = inputStreamEntry.getValue();
          preJoinStreams.put(inputStage,
                             inputStream.flatMapToPair(new JoinOnFunction(pluginFunctionContext, inputStage)));
        }

        Set<String> remainingInputs = new HashSet<>();
        remainingInputs.addAll(inputDataCollections.keySet());

        Integer numPartitions = stagePartitions.get(stageName);

        SparkPairCollection<Object, List<JoinElement<Object>>> joinedInputs = null;
        // inner join on required inputs
        for (final String inputStageName : joiner.getJoinConfig().getRequiredInputs()) {
          SparkPairCollection<Object, Object> preJoinCollection = preJoinStreams.get(inputStageName);

          if (joinedInputs == null) {
            joinedInputs = preJoinCollection.mapValues(new InitialJoinFunction(inputStageName));
          } else {
            JoinFlattenFunction joinFlattenFunction = new JoinFlattenFunction(inputStageName);
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
            joinedInputs = preJoinStream.mapValues(new InitialJoinFunction(inputStageName));
          } else {
            if (isFullOuter) {
              OuterJoinFlattenFunction outerJoinFlattenFunction = new OuterJoinFlattenFunction(inputStageName);

              joinedInputs = numPartitions == null ?
                joinedInputs.fullOuterJoin(preJoinStream).mapValues(outerJoinFlattenFunction) :
                joinedInputs.fullOuterJoin(preJoinStream, numPartitions).mapValues(outerJoinFlattenFunction);
            } else {
              LeftJoinFlattenFunction joinFlattenFunction = new LeftJoinFlattenFunction(inputStageName);

              joinedInputs = numPartitions == null ?
                joinedInputs.leftOuterJoin(preJoinStream).mapValues(joinFlattenFunction) :
                joinedInputs.leftOuterJoin(preJoinStream, numPartitions).mapValues(joinFlattenFunction);
            }
          }
        }

        // should never happen, but removes warnings
        if (joinedInputs == null) {
          throw new IllegalStateException("There are no inputs into join stage " + stageName);
        }

        stageData = joinedInputs.flatMap(new JoinMergeFunction(pluginFunctionContext)).cache();

      } else if (Windower.PLUGIN_TYPE.equals(pluginType)) {

        Windower windower = sec.getPluginContext().newPluginInstance(stageName, macroEvaluator);
        stageData = stageData.window(stageName, windower);

      } else {
        throw new IllegalStateException(String.format("Stage %s is of unsupported plugin type %s.",
                                                      stageName, pluginType));
      }

      if (shouldCache(pipelinePhase, stageInfo)) {
        stageData = stageData.cache();
      }
      stageDataCollections.put(stageName, stageData);
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
      if (outputStage.getInputs().size() > 1) {
        return true;
      }
    }

    return false;
  }
}
