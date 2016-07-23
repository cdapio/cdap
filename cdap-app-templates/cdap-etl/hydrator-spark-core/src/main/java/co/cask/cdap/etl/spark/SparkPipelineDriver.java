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
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.planner.StageInfo;
import co.cask.cdap.etl.spark.function.AggregatorAggregateFunction;
import co.cask.cdap.etl.spark.function.AggregatorGroupByFunction;
import co.cask.cdap.etl.spark.function.BatchSinkFunction;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;
import co.cask.cdap.etl.spark.function.TransformFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base Spark program to run a Hydrator pipeline.
 *
 * @param <T> type of Spark object the pipeline operates on.
 */
public abstract class SparkPipelineDriver<T> {

  protected abstract T union(T input1, T input2);

  protected abstract void cache(T data);

  protected abstract T getSource(String stageName, PluginFunctionContext pluginFunctionContext);

  protected abstract T handleTransform(String stageName, T inputData, TransformFunction function);

  protected abstract T handleSparkCompute(String stageName, T inputData,
                                          SparkCompute<Object, Object> plugin) throws Exception;

  protected abstract T handleAggregator(String stageName, T inputData,
                                        AggregatorGroupByFunction groupByFunction,
                                        AggregatorAggregateFunction aggregateFunction);

  protected abstract T handleJoin(String stageName,
                                  BatchJoiner<Object, Object, Object> joiner,
                                  Map<String, T> inputData,
                                  PluginFunctionContext pluginFunctionContext) throws Exception;

  protected abstract void handleBatchSink(String stageName, T inputData, BatchSinkFunction sinkFunction);

  protected abstract void handleSparkSink(String stageName, T inputData, SparkSink<Object> plugin) throws Exception;

  public void runPipeline(PipelinePhase pipelinePhase, String sourcePluginType,
                          JavaSparkExecutionContext sec) throws Exception {

    MacroEvaluator macroEvaluator =
      new DefaultMacroEvaluator(sec.getWorkflowToken(), sec.getRuntimeArguments(), sec.getLogicalStartTime(), sec,
                                sec.getNamespace());
    Map<String, T> stageDataCollections = new HashMap<>();
    for (String stageName : pipelinePhase.getDag().getTopologicalOrder()) {
      StageInfo stageInfo = pipelinePhase.getStage(stageName);
      String pluginType = stageInfo.getPluginType();

      T stageData = null;

      Map<String, T> inputDataCollections = new HashMap<>();
      for (String inputStageName : stageInfo.getInputs()) {
        inputDataCollections.put(inputStageName, stageDataCollections.get(inputStageName));
      }

      // if this stage has multiple inputs, and is not a joiner plugin,
      // initialize the stageRDD as the union of all input RDDs.
      if (!inputDataCollections.isEmpty()) {
        Iterator<T> inputRDDIter = inputDataCollections.values().iterator();
        stageData = inputRDDIter.next();
        // don't union if we're joining
        while (!BatchJoiner.PLUGIN_TYPE.equals(pluginType) && inputRDDIter.hasNext()) {
          stageData = union(stageData, inputRDDIter.next());
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
        handleBatchSink(stageName, stageData, new BatchSinkFunction(pluginFunctionContext));
      } else if (Transform.PLUGIN_TYPE.equals(pluginType)) {
        stageData = handleTransform(stageName, stageData, new TransformFunction(pluginFunctionContext));
      } else if (SparkCompute.PLUGIN_TYPE.equals(pluginType)) {
        SparkCompute<Object, Object> sparkCompute =
          sec.getPluginContext().newPluginInstance(stageName, macroEvaluator);
        stageData = handleSparkCompute(stageName, stageData, sparkCompute);
      } else if (SparkSink.PLUGIN_TYPE.equals(pluginType)) {
        SparkSink<Object> sparkSink = sec.getPluginContext().newPluginInstance(stageName, macroEvaluator);
        handleSparkSink(stageName, stageData, sparkSink);
      } else if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {
        stageData = handleAggregator(stageName, stageData,
                                     new AggregatorGroupByFunction(pluginFunctionContext),
                                     new AggregatorAggregateFunction(pluginFunctionContext));
      } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {
        BatchJoiner<Object, Object, Object> joiner =
          sec.getPluginContext().newPluginInstance(stageName, macroEvaluator);
        BatchJoinerRuntimeContext joinerRuntimeContext = pluginFunctionContext.createJoinerRuntimeContext();
        joiner.initialize(joinerRuntimeContext);
        stageData = handleJoin(stageName, joiner, inputDataCollections, pluginFunctionContext);
      } else {
        throw new IllegalStateException(String.format("Stage %s is of unsupported plugin type %s.",
                                                      stageName, pluginType));
      }

      if (shouldCache(pipelinePhase, stageInfo)) {
        cache(stageData);
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
