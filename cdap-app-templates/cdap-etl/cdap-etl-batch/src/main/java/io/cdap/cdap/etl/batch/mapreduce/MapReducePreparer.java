/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.mapreduce;

import com.google.gson.Gson;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.SubmitterLifecycle;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchConfigurable;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchReducibleAggregator;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.batch.BatchPhaseSpec;
import io.cdap.cdap.etl.batch.DefaultAggregatorContext;
import io.cdap.cdap.etl.batch.DefaultJoinerContext;
import io.cdap.cdap.etl.batch.conversion.WritableConversion;
import io.cdap.cdap.etl.batch.conversion.WritableConversions;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.TypeChecker;
import io.cdap.cdap.etl.common.submit.AggregatorContextProvider;
import io.cdap.cdap.etl.common.submit.ContextProvider;
import io.cdap.cdap.etl.common.submit.Finisher;
import io.cdap.cdap.etl.common.submit.JoinerContextProvider;
import io.cdap.cdap.etl.common.submit.PipelinePhasePreparer;
import io.cdap.cdap.etl.common.submit.SubmitterPlugin;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.tephra.TransactionFailureException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * For each stage, call prepareRun() in topological order. prepareRun will setup the input/output of the pipeline phase
 * and set any arguments that should be visible to subsequent stages. These configure and prepare operations must be
 * performed in topological order to ensure that arguments set by one stage are available to subsequent stages.
 */
public class MapReducePreparer extends PipelinePhasePreparer {

  private static final Gson GSON = new Gson();
  private final MapReduceContext context;
  private final Set<String> connectorDatasets;
  private Job job;
  private Configuration hConf;
  private Map<String, SinkOutput> sinkOutputs;
  private Map<String, String> inputAliasToStage;

  public MapReducePreparer(MapReduceContext context, Metrics metrics, MacroEvaluator macroEvaluator,
                           PipelineRuntime pipelineRuntime, Set<String> connectorDatasets) {
    super(context, metrics, macroEvaluator, pipelineRuntime);
    this.context = context;
    this.connectorDatasets = connectorDatasets;
  }

  public List<Finisher> prepare(BatchPhaseSpec phaseSpec,
                                Job job) throws TransactionFailureException, InstantiationException, IOException {
    this.job = job;
    this.hConf = job.getConfiguration();
    hConf.setBoolean("mapreduce.map.speculative", false);
    hConf.setBoolean("mapreduce.reduce.speculative", false);

    sinkOutputs = new HashMap<>();
    inputAliasToStage = new HashMap<>();
    // Collect field operations emitted by various stages in this MapReduce program
    stageOperations = new HashMap<>();

    List<Finisher> finishers = prepare(phaseSpec);

    hConf.set(ETLMapReduce.SINK_OUTPUTS_KEY, GSON.toJson(sinkOutputs));
    hConf.set(ETLMapReduce.INPUT_ALIAS_KEY, GSON.toJson(inputAliasToStage));

    WorkflowToken token = context.getWorkflowToken();
    if (token != null) {
      for (Map.Entry<String, String> entry : pipelineRuntime.getArguments().getAddedArguments().entrySet()) {
        token.put(entry.getKey(), entry.getValue());
      }

      // Put the collected field operations in workflow token
      token.put(Constants.FIELD_OPERATION_KEY_IN_WORKFLOW_TOKEN, GSON.toJson(stageOperations));
    }
    // token is null when just the mapreduce job is run but not the entire workflow
    // we still want things to work in that case.
    hConf.set(ETLMapReduce.RUNTIME_ARGS_KEY, GSON.toJson(pipelineRuntime.getArguments().asMap()));

    return finishers;
  }

  @Nullable
  @Override
  protected SubmitterPlugin create(StageSpec stageSpec, SubmitterLifecycle<?> plugin) throws InstantiationException {
    return null;
  }

  @Override
  protected SubmitterPlugin createSource(BatchConfigurable<BatchSourceContext> batchSource, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<MapReduceBatchContext> contextProvider =
      new MapReduceBatchContextProvider(context, pipelineRuntime, stageSpec, connectorDatasets);
    return new SubmitterPlugin<>(stageName, context, batchSource, contextProvider, sourceContext -> {
      for (String inputAlias : sourceContext.getInputNames()) {
        inputAliasToStage.put(inputAlias, stageName);
      }
      stageOperations.put(stageName, sourceContext.getFieldOperations());
    });
  }

  @Override
  protected SubmitterPlugin createSink(BatchConfigurable<BatchSinkContext> batchSink, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<MapReduceBatchContext> contextProvider =
      new MapReduceBatchContextProvider(context, pipelineRuntime, stageSpec, connectorDatasets);
    return new SubmitterPlugin<>(stageName, context, batchSink, contextProvider, sinkContext -> {
      sinkOutputs.put(stageName, new SinkOutput(sinkContext.getOutputNames()));
      stageOperations.put(stageName, sinkContext.getFieldOperations());
    });
  }

  @Override
  protected SubmitterPlugin createTransform(Transform<?, ?> transform, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<MapReduceBatchContext> contextProvider =
      new MapReduceBatchContextProvider(context, pipelineRuntime, stageSpec, connectorDatasets);
    return new SubmitterPlugin<>(stageName, context, transform, contextProvider,
                                 ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));
  }

  @Override
  protected SubmitterPlugin createSplitterTransform(SplitterTransform<?, ?> splitterTransform, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<MapReduceBatchContext> contextProvider =
      new MapReduceBatchContextProvider(context, pipelineRuntime, stageSpec, connectorDatasets);
    return new SubmitterPlugin<>(stageName, context, splitterTransform, contextProvider,
                                 ctx -> stageOperations.put(stageName, ctx.getFieldOperations()));
  }

  @Override
  protected SubmitterPlugin createAggregator(BatchAggregator<?, ?, ?> aggregator, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<DefaultAggregatorContext> contextProvider =
      new AggregatorContextProvider(pipelineRuntime, stageSpec, context.getAdmin());
    return new SubmitterPlugin<>(stageName, context, aggregator, contextProvider, aggregatorContext -> {
      if (aggregatorContext.getNumPartitions() != null) {
        job.setNumReduceTasks(aggregatorContext.getNumPartitions());
      }
      Class<?> outputKeyClass = aggregatorContext.getGroupKeyClass();
      Class<?> outputValClass = aggregatorContext.getGroupValueClass();

      if (outputKeyClass == null) {
        outputKeyClass = TypeChecker.getGroupKeyClass(aggregator);
      }
      if (outputValClass == null) {
        outputValClass = TypeChecker.getGroupValueClass(aggregator);
      }
      hConf.set(ETLMapReduce.MAP_KEY_CLASS, outputKeyClass.getName());
      hConf.set(ETLMapReduce.MAP_VAL_CLASS, outputValClass.getName());
      job.setMapOutputKeyClass(getOutputKeyClass(stageName, outputKeyClass));
      job.setMapOutputValueClass(getOutputValClass(stageName, outputValClass));
      stageOperations.put(stageName, aggregatorContext.getFieldOperations());
    });
  }

  @Override
  protected SubmitterPlugin createReducibleAggregator(BatchReducibleAggregator<?, ?, ?, ?> aggregator,
                                                      StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<DefaultAggregatorContext> contextProvider =
      new AggregatorContextProvider(pipelineRuntime, stageSpec, context.getAdmin());
    return new SubmitterPlugin<>(stageName, context, aggregator, contextProvider, aggregatorContext -> {
      if (aggregatorContext.getNumPartitions() != null) {
        job.setNumReduceTasks(aggregatorContext.getNumPartitions());
      }
      Class<?> outputKeyClass = aggregatorContext.getGroupKeyClass();
      Class<?> outputValClass = aggregatorContext.getGroupValueClass();

      if (outputKeyClass == null) {
        outputKeyClass = TypeChecker.getGroupKeyClass(aggregator);
      }
      if (outputValClass == null) {
        outputValClass = TypeChecker.getGroupValueClass(aggregator);
      }
      hConf.set(ETLMapReduce.MAP_KEY_CLASS, outputKeyClass.getName());
      hConf.set(ETLMapReduce.MAP_VAL_CLASS, outputValClass.getName());
      job.setMapOutputKeyClass(getOutputKeyClass(stageName, outputKeyClass));
      job.setMapOutputValueClass(getOutputValClass(stageName, outputValClass));
      stageOperations.put(stageName, aggregatorContext.getFieldOperations());
    });
  }

  @Override
  protected SubmitterPlugin createJoiner(BatchJoiner<?, ?, ?> batchJoiner, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<DefaultJoinerContext> contextProvider =
      new JoinerContextProvider(pipelineRuntime, stageSpec, context.getAdmin());
    return new SubmitterPlugin<>(stageName, context, batchJoiner, contextProvider, joinerContext -> {
      if (joinerContext.getNumPartitions() != null) {
        job.setNumReduceTasks(joinerContext.getNumPartitions());
      }
      Class<?> outputKeyClass = joinerContext.getJoinKeyClass();
      Class<?> inputRecordClass = joinerContext.getJoinInputRecordClass();

      if (outputKeyClass == null) {
        outputKeyClass = TypeChecker.getJoinKeyClass(batchJoiner);
      }
      if (inputRecordClass == null) {
        inputRecordClass = TypeChecker.getJoinInputRecordClass(batchJoiner);
      }
      hConf.set(ETLMapReduce.MAP_KEY_CLASS, outputKeyClass.getName());
      hConf.set(ETLMapReduce.MAP_VAL_CLASS, inputRecordClass.getName());
      job.setMapOutputKeyClass(getOutputKeyClass(stageName, outputKeyClass));
      getOutputValClass(stageName, inputRecordClass);
      // for joiner plugin map output is tagged with stageName
      job.setMapOutputValueClass(TaggedWritable.class);
      stageOperations.put(stageName, joinerContext.getFieldOperations());
    });
  }

  @Override
  protected SubmitterPlugin createAutoJoiner(BatchAutoJoiner batchJoiner, StageSpec stageSpec) {
    String stageName = stageSpec.getName();
    ContextProvider<DefaultJoinerContext> contextProvider =
      new JoinerContextProvider(pipelineRuntime, stageSpec, context.getAdmin());
    return new SubmitterPlugin<>(stageName, context, batchJoiner, contextProvider, joinerContext -> {
      if (joinerContext.getNumPartitions() != null) {
        job.setNumReduceTasks(joinerContext.getNumPartitions());
      }
      hConf.set(ETLMapReduce.MAP_KEY_CLASS, StructuredRecord.class.getName());
      hConf.set(ETLMapReduce.MAP_VAL_CLASS, StructuredRecord.class.getName());
      job.setMapOutputKeyClass(getOutputKeyClass(stageName, StructuredRecord.class));
      getOutputValClass(stageName, StructuredRecord.class);
      // for joiner plugin map output is tagged with stageName
      job.setMapOutputValueClass(TaggedWritable.class);
      stageOperations.put(stageName, joinerContext.getFieldOperations());
    });
  }

  private Class<?> getOutputKeyClass(String reducerName, Class<?> outputKeyClass) {
    // in case the classes are not a WritableComparable, but is some common type we support
    // for example, a String or a StructuredRecord
    WritableConversion writableConversion = WritableConversions.getConversion(outputKeyClass.getName());
    // if the conversion is null, it means the user is using their own object.
    if (writableConversion != null) {
      outputKeyClass = writableConversion.getWritableClass();
    }
    // check classes here instead of letting mapreduce do it, since mapreduce throws a cryptic error
    if (!WritableComparable.class.isAssignableFrom(outputKeyClass)) {
      throw new IllegalArgumentException(String.format(
        "Invalid reducer %s. The key class %s must implement Hadoop's WritableComparable.",
        reducerName, outputKeyClass));
    }
    return outputKeyClass;
  }

  private Class<?> getOutputValClass(String reducerName, Class<?> outputValClass) {
    WritableConversion writableConversion;
    writableConversion = WritableConversions.getConversion(outputValClass.getName());
    if (writableConversion != null) {
      outputValClass = writableConversion.getWritableClass();
    }
    if (!Writable.class.isAssignableFrom(outputValClass)) {
      throw new IllegalArgumentException(String.format(
        "Invalid reducer %s. The value class %s must implement Hadoop's Writable.",
        reducerName, outputValClass));
    }
    return outputValClass;
  }

  /**
   * Context provider for mapreduce.
   */
  private static class MapReduceBatchContextProvider implements ContextProvider<MapReduceBatchContext> {

    private final MapReduceContext context;
    private final PipelineRuntime pipelineRuntime;
    private final StageSpec stageSpec;
    private final Set<String> connectorDatasets;

    MapReduceBatchContextProvider(MapReduceContext context, PipelineRuntime pipelineRuntime,
                                  StageSpec stageSpec, Set<String> connectorDatasets) {
      this.context = context;
      this.pipelineRuntime = pipelineRuntime;
      this.stageSpec = stageSpec;
      this.connectorDatasets = connectorDatasets;
    }

    @Override
    public MapReduceBatchContext getContext(DatasetContext datasetContext) {
      return new MapReduceBatchContext(context, pipelineRuntime, stageSpec, connectorDatasets, datasetContext);
    }
  }
}
