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
import co.cask.cdap.etl.api.Alert;
import co.cask.cdap.etl.api.AlertPublisher;
import co.cask.cdap.etl.api.ErrorRecord;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.SplitterTransform;
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
import co.cask.cdap.etl.common.RecordInfo;
import co.cask.cdap.etl.common.plugin.PipelinePluginContext;
import co.cask.cdap.etl.spark.function.AlertPassFilter;
import co.cask.cdap.etl.spark.function.BatchSinkFunction;
import co.cask.cdap.etl.spark.function.ErrorPassFilter;
import co.cask.cdap.etl.spark.function.ErrorTransformFunction;
import co.cask.cdap.etl.spark.function.InitialJoinFunction;
import co.cask.cdap.etl.spark.function.JoinFlattenFunction;
import co.cask.cdap.etl.spark.function.LeftJoinFlattenFunction;
import co.cask.cdap.etl.spark.function.OuterJoinFlattenFunction;
import co.cask.cdap.etl.spark.function.OutputPassFilter;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;
import co.cask.cdap.etl.spec.StageSpec;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Base Spark program to run a Hydrator pipeline.
 */
public abstract class SparkPipelineRunner {

  protected abstract SparkCollection<RecordInfo<Object>> getSource(StageSpec stageSpec) throws Exception;

  protected abstract SparkPairCollection<Object, Object> addJoinKey(
    StageSpec stageSpec, String inputStageName,
    SparkCollection<Object> inputCollection) throws Exception;

  protected abstract SparkCollection<Object> mergeJoinResults(
    StageSpec stageSpec,
    SparkPairCollection<Object, List<JoinElement<Object>>> joinedInputs) throws Exception;

  public void runPipeline(PipelinePhase pipelinePhase, String sourcePluginType,
                          JavaSparkExecutionContext sec,
                          Map<String, Integer> stagePartitions,
                          PipelinePluginContext pluginContext) throws Exception {

    MacroEvaluator macroEvaluator =
      new DefaultMacroEvaluator(sec.getWorkflowToken(), sec.getRuntimeArguments(), sec.getLogicalStartTime(), sec,
                                sec.getNamespace());
    Map<String, EmittedRecords> emittedRecords = new HashMap<>();

    // should never happen, but removes warning
    if (pipelinePhase.getDag() == null) {
      throw new IllegalStateException("Pipeline phase has no connections.");
    }

    for (String stageName : pipelinePhase.getDag().getTopologicalOrder()) {
      StageSpec stageSpec = pipelinePhase.getStage(stageName);
      //noinspection ConstantConditions
      String pluginType = stageSpec.getPluginType();

      EmittedRecords.Builder emittedBuilder = EmittedRecords.builder();

      // don't want to do an additional filter for stages that can emit errors,
      // but aren't connected to an ErrorTransform
      // similarly, don't want to do an additional filter for alerts when the stage isn't connected to
      // an AlertPublisher
      boolean hasErrorOutput = false;
      boolean hasAlertOutput = false;
      Set<String> outputs = pipelinePhase.getStageOutputs(stageSpec.getName());
      for (String output : outputs) {
        String outputPluginType = pipelinePhase.getStage(output).getPluginType();
        //noinspection ConstantConditions
        if (ErrorTransform.PLUGIN_TYPE.equals(outputPluginType)) {
          hasErrorOutput = true;
        } else if (AlertPublisher.PLUGIN_TYPE.equals(outputPluginType)) {
          hasAlertOutput = true;
        }
      }

      SparkCollection<Object> stageData = null;

      Map<String, SparkCollection<Object>> inputDataCollections = new HashMap<>();
      Set<String> stageInputs = stageSpec.getInputs();
      for (String inputStageName : stageInputs) {
        StageSpec inputStageSpec = pipelinePhase.getStage(inputStageName);
        if (inputStageSpec == null) {
          // means the input to this stage is in a separate phase. For example, it is an action.
          continue;
        }
        String port = inputStageSpec.getOutputPorts().get(stageName).getPort();
        SparkCollection<Object> inputRecords = port == null ?
          emittedRecords.get(inputStageName).outputRecords :
          emittedRecords.get(inputStageName).outputPortRecords.get(port);
        inputDataCollections.put(inputStageName, inputRecords);
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

      PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec);
      if (stageData == null) {

        // this if-else is nested inside the stageRDD null check to avoid warnings about stageRDD possibly being
        // null in the other else-if conditions
        if (sourcePluginType.equals(pluginType)) {
          SparkCollection<RecordInfo<Object>> combinedData = getSource(stageSpec);
          emittedBuilder = addEmitted(emittedBuilder, pipelinePhase, stageSpec,
                                      combinedData, hasErrorOutput, hasAlertOutput);
        } else {
          throw new IllegalStateException(String.format("Stage '%s' has no input and is not a source.", stageName));
        }

      } else if (BatchSink.PLUGIN_TYPE.equals(pluginType)) {

        stageData.store(stageSpec, Compat.convert(new BatchSinkFunction(pluginFunctionContext)));

      } else if (Transform.PLUGIN_TYPE.equals(pluginType)) {

        SparkCollection<RecordInfo<Object>> combinedData = stageData.transform(stageSpec);
        emittedBuilder = addEmitted(emittedBuilder, pipelinePhase, stageSpec,
                                    combinedData, hasErrorOutput, hasAlertOutput);

      } else if (SplitterTransform.PLUGIN_TYPE.equals(pluginType)) {

        SparkCollection<RecordInfo<Object>> combinedData = stageData.multiOutputTransform(stageSpec);
        emittedBuilder = addEmitted(emittedBuilder, pipelinePhase, stageSpec,
                                    combinedData, hasErrorOutput, hasAlertOutput);

      } else if (ErrorTransform.PLUGIN_TYPE.equals(pluginType)) {

        // union all the errors coming into this stage
        SparkCollection<ErrorRecord<Object>> inputErrors = null;
        for (String inputStage : stageInputs) {
          SparkCollection<ErrorRecord<Object>> inputErrorsFromStage = emittedRecords.get(inputStage).errorRecords;
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
          SparkCollection<RecordInfo<Object>> combinedData =
            inputErrors.flatMap(stageSpec, Compat.convert(new ErrorTransformFunction<>(pluginFunctionContext)));
          emittedBuilder = addEmitted(emittedBuilder, pipelinePhase, stageSpec,
                                      combinedData, hasErrorOutput, hasAlertOutput);
        }

      } else if (SparkCompute.PLUGIN_TYPE.equals(pluginType)) {

        SparkCompute<Object, Object> sparkCompute = pluginContext.newPluginInstance(stageName, macroEvaluator);
        emittedBuilder = emittedBuilder.setOutput(stageData.compute(stageSpec, sparkCompute));

      } else if (SparkSink.PLUGIN_TYPE.equals(pluginType)) {

        SparkSink<Object> sparkSink = pluginContext.newPluginInstance(stageName, macroEvaluator);
        stageData.store(stageSpec, sparkSink);

      } else if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {

        Integer partitions = stagePartitions.get(stageName);
        SparkCollection<RecordInfo<Object>> combinedData = stageData.aggregate(stageSpec, partitions);
        emittedBuilder = addEmitted(emittedBuilder, pipelinePhase, stageSpec,
                                    combinedData, hasErrorOutput, hasAlertOutput);

      } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {

        BatchJoiner<Object, Object, Object> joiner = pluginContext.newPluginInstance(stageName, macroEvaluator);
        BatchJoinerRuntimeContext joinerRuntimeContext = pluginFunctionContext.createBatchRuntimeContext();
        joiner.initialize(joinerRuntimeContext);

        Map<String, SparkPairCollection<Object, Object>> preJoinStreams = new HashMap<>();
        for (Map.Entry<String, SparkCollection<Object>> inputStreamEntry : inputDataCollections.entrySet()) {
          String inputStage = inputStreamEntry.getKey();
          SparkCollection<Object> inputStream = inputStreamEntry.getValue();
          preJoinStreams.put(inputStage, addJoinKey(stageSpec, inputStage, inputStream));
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

        emittedBuilder = emittedBuilder.setOutput(mergeJoinResults(stageSpec, joinedInputs).cache());

      } else if (Windower.PLUGIN_TYPE.equals(pluginType)) {

        Windower windower = pluginContext.newPluginInstance(stageName, macroEvaluator);
        emittedBuilder = emittedBuilder.setOutput(stageData.window(stageSpec, windower));

      } else if (AlertPublisher.PLUGIN_TYPE.equals(pluginType)) {

        // union all the alerts coming into this stage
        SparkCollection<Alert> inputAlerts = null;
        for (String inputStage : stageInputs) {
          SparkCollection<Alert> inputErrorsFromStage = emittedRecords.get(inputStage).alertRecords;
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
          inputAlerts.publishAlerts(stageSpec);
        }

      } else {
        throw new IllegalStateException(String.format("Stage %s is of unsupported plugin type %s.",
                                                      stageName, pluginType));
      }

      emittedRecords.put(stageName, emittedBuilder.build());
    }
  }

  // return whether this stage should be cached to avoid recomputation
  private boolean shouldCache(PipelinePhase pipelinePhase, StageSpec stageSpec) {

    // cache this RDD if it has multiple outputs,
    // otherwise computation of each output may trigger recomputing this stage
    Set<String> outputs = pipelinePhase.getStageOutputs(stageSpec.getName());
    if (outputs.size() > 1) {
      return true;
    }

    // cache this stage if it is an input to a stage that has multiple inputs.
    // otherwise the union computation may trigger recomputing this stage
    for (String outputStageName : outputs) {
      StageSpec outputStage = pipelinePhase.getStage(outputStageName);
      //noinspection ConstantConditions
      if (outputStage.getInputs().size() > 1) {
        return true;
      }
    }

    return false;
  }

  private EmittedRecords.Builder addEmitted(EmittedRecords.Builder builder, PipelinePhase pipelinePhase,
                                            StageSpec stageSpec, SparkCollection<RecordInfo<Object>> stageData,
                                            boolean hasErrors, boolean hasAlerts) {

    if (hasErrors || hasAlerts || stageSpec.getOutputPorts().size() > 1) {
      // need to cache, otherwise the stage can be computed once per type of emitted record
      stageData = stageData.cache();
    }

    boolean shouldCache = shouldCache(pipelinePhase, stageSpec);

    if (hasErrors) {
      SparkCollection<ErrorRecord<Object>> errors =
        stageData.flatMap(stageSpec, Compat.convert(new ErrorPassFilter<>()));
      if (shouldCache) {
        errors = errors.cache();
      }
      builder.setErrors(errors);
    }
    if (hasAlerts) {
      SparkCollection<Alert> alerts = stageData.flatMap(stageSpec, Compat.convert(new AlertPassFilter()));
      if (shouldCache) {
        alerts = alerts.cache();
      }
      builder.setAlerts(alerts);
    }

    if (SplitterTransform.PLUGIN_TYPE.equals(stageSpec.getPluginType())) {
      // set collections for each port, implemented as a filter on the port.
      for (StageSpec.Port portSpec : stageSpec.getOutputPorts().values()) {
        String port = portSpec.getPort();
        SparkCollection<Object> portData = stageData.flatMap(stageSpec, Compat.convert(new OutputPassFilter<>(port)));
        if (shouldCache) {
          portData = portData.cache();
        }
        builder.addPort(port, portData);
      }
    } else {
      SparkCollection<Object> outputs = stageData.flatMap(stageSpec, Compat.convert(new OutputPassFilter<>()));
      if (shouldCache) {
        outputs = outputs.cache();
      }
      builder.setOutput(outputs);
    }

    return builder;
  }

  /**
   * Holds all records emitted by a stage.
   */
  private static class EmittedRecords {
    private final Map<String, SparkCollection<Object>> outputPortRecords;
    private final SparkCollection<Object> outputRecords;
    private final SparkCollection<ErrorRecord<Object>> errorRecords;
    private final SparkCollection<Alert> alertRecords;

    private EmittedRecords(Map<String, SparkCollection<Object>> outputPortRecords,
                           SparkCollection<Object> outputRecords,
                           SparkCollection<ErrorRecord<Object>> errorRecords,
                           SparkCollection<Alert> alertRecords) {
      this.outputPortRecords = outputPortRecords;
      this.outputRecords = outputRecords;
      this.errorRecords = errorRecords;
      this.alertRecords = alertRecords;
    }

    private static Builder builder() {
      return new Builder();
    }

    private static class Builder {
      private Map<String, SparkCollection<Object>> outputPortRecords;
      private SparkCollection<Object> outputRecords;
      private SparkCollection<ErrorRecord<Object>> errorRecords;
      private SparkCollection<Alert> alertRecords;

      private Builder() {
        outputPortRecords = new HashMap<>();
      }

      private Builder addPort(String port, SparkCollection<Object> records) {
        outputPortRecords.put(port, records);
        return this;
      }

      private Builder setOutput(SparkCollection<Object> records) {
        outputRecords = records;
        return this;
      }

      private Builder setErrors(SparkCollection<ErrorRecord<Object>> errors) {
        errorRecords = errors;
        return this;
      }

      private Builder setAlerts(SparkCollection<Alert> alerts) {
        alertRecords = alerts;
        return this;
      }

      private EmittedRecords build() {
        return new EmittedRecords(outputPortRecords, outputRecords, errorRecords, alertRecords);
      }
    }
  }
}
