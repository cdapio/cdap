/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.preview.DataTracer;
import co.cask.cdap.etl.api.Aggregator;
import co.cask.cdap.etl.api.AlertPublisher;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.Joiner;
import co.cask.cdap.etl.api.SplitterTransform;
import co.cask.cdap.etl.api.StageLifecycle;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.ConnectorSourceEmitter;
import co.cask.cdap.etl.batch.DirectOutputPipeStage;
import co.cask.cdap.etl.batch.MultiOutputTransformPipeStage;
import co.cask.cdap.etl.batch.PipeEmitter;
import co.cask.cdap.etl.batch.PipeStage;
import co.cask.cdap.etl.batch.PipeTransformExecutor;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.batch.UnwrapPipeStage;
import co.cask.cdap.etl.batch.conversion.WritableConversion;
import co.cask.cdap.etl.batch.conversion.WritableConversions;
import co.cask.cdap.etl.batch.join.Join;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.cdap.etl.common.NoErrorEmitter;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.common.RecordInfo;
import co.cask.cdap.etl.common.StageStatisticsCollector;
import co.cask.cdap.etl.common.TrackedMultiOutputTransform;
import co.cask.cdap.etl.common.TrackedTransform;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.common.preview.LimitingTransform;
import co.cask.cdap.etl.spec.StageSpec;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Helps create {@link TransformExecutor TransformExecutors}.
 *
 * @param <T> the type of input for the created transform executors
 */
public class MapReduceTransformExecutorFactory<T> {
  private final String sourceStageName;
  private final MacroEvaluator macroEvaluator;
  private final PipelinePluginInstantiator pluginInstantiator;
  private final Metrics metrics;
  private final MapReduceTaskContext taskContext;
  private final String mapOutputKeyClassName;
  private final String mapOutputValClassName;
  private final int numberOfRecordsPreview;
  private final BasicArguments arguments;
  private boolean isMapPhase;

  public MapReduceTransformExecutorFactory(MapReduceTaskContext taskContext,
                                           PipelinePluginInstantiator pluginInstantiator,
                                           Metrics metrics,
                                           BasicArguments arguments,
                                           String sourceStageName,
                                           int numberOfRecordsPreview) {
    this.taskContext = taskContext;
    this.numberOfRecordsPreview = numberOfRecordsPreview;
    this.pluginInstantiator = pluginInstantiator;
    this.metrics = metrics;
    this.sourceStageName = sourceStageName;
    this.macroEvaluator =
      new DefaultMacroEvaluator(taskContext.getWorkflowToken(), taskContext.getRuntimeArguments(),
                                taskContext.getLogicalStartTime(), taskContext,
                                taskContext.getNamespace());
    JobContext hadoopContext = (JobContext) taskContext.getHadoopContext();
    Configuration hConf = hadoopContext.getConfiguration();
    this.mapOutputKeyClassName = hConf.get(ETLMapReduce.MAP_KEY_CLASS);
    this.mapOutputValClassName = hConf.get(ETLMapReduce.MAP_VAL_CLASS);
    this.isMapPhase = hadoopContext instanceof Mapper.Context;
    this.arguments = arguments;
  }

  private MapReduceRuntimeContext createRuntimeContext(StageSpec stageInfo) {
    PipelineRuntime pipelineRuntime = new PipelineRuntime(taskContext, metrics, arguments);
    return new MapReduceRuntimeContext(taskContext, pipelineRuntime, stageInfo);
  }

  private <IN, ERROR> TrackedMultiOutputTransform<IN, ERROR> getMultiOutputTransform(StageSpec stageSpec)
    throws Exception {
    String stageName = stageSpec.getName();
    DefaultMacroEvaluator macroEvaluator = new DefaultMacroEvaluator(taskContext.getWorkflowToken(),
                                                                     taskContext.getRuntimeArguments(),
                                                                     taskContext.getLogicalStartTime(), taskContext,
                                                                     taskContext.getNamespace());
    SplitterTransform<IN, ERROR> splitterTransform =
      pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
    TransformContext transformContext = createRuntimeContext(stageSpec);
    splitterTransform.initialize(transformContext);

    StageMetrics stageMetrics = new DefaultStageMetrics(metrics, stageName);
    TaskAttemptContext taskAttemptContext = (TaskAttemptContext) taskContext.getHadoopContext();
    StageStatisticsCollector collector = new MapReduceStageStatisticsCollector(stageName, taskAttemptContext);
    return new TrackedMultiOutputTransform<>(splitterTransform, stageMetrics, taskContext.getDataTracer(stageName),
                                             collector);
  }

  @SuppressWarnings("unchecked")
  private <IN, OUT> TrackedTransform<IN, OUT> getTransformation(StageSpec stageSpec) throws Exception {

    DefaultMacroEvaluator macroEvaluator = new DefaultMacroEvaluator(taskContext.getWorkflowToken(),
                                                                     taskContext.getRuntimeArguments(),
                                                                     taskContext.getLogicalStartTime(), taskContext,
                                                                     taskContext.getNamespace());
    String stageName = stageSpec.getName();
    String pluginType = stageSpec.getPluginType();
    StageMetrics stageMetrics = new DefaultStageMetrics(metrics, stageName);
    TaskAttemptContext taskAttemptContext = (TaskAttemptContext) taskContext.getHadoopContext();
    StageStatisticsCollector collector = new MapReduceStageStatisticsCollector(stageName, taskAttemptContext);
    if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {
      BatchAggregator<?, ?, ?> batchAggregator = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
      BatchRuntimeContext runtimeContext = createRuntimeContext(stageSpec);
      batchAggregator.initialize(runtimeContext);
      if (isMapPhase) {
        return getTrackedEmitKeyStep(new MapperAggregatorTransformation(batchAggregator, mapOutputKeyClassName,
                                                                        mapOutputValClassName),
                                     stageMetrics, taskContext.getDataTracer(stageName), collector);
      } else {
        return getTrackedAggregateStep(new ReducerAggregatorTransformation(batchAggregator,
                                                                           mapOutputKeyClassName,
                                                                           mapOutputValClassName),
                                       stageMetrics, taskContext.getDataTracer(stageName), collector);
      }
    } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {
      BatchJoiner<?, ?, ?> batchJoiner = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
      BatchJoinerRuntimeContext runtimeContext = createRuntimeContext(stageSpec);
      batchJoiner.initialize(runtimeContext);
      if (isMapPhase) {
        return getTrackedEmitKeyStep(
          new MapperJoinerTransformation(batchJoiner, mapOutputKeyClassName, mapOutputValClassName), stageMetrics,
          taskContext.getDataTracer(stageName), collector);
      } else {
        return getTrackedMergeStep(
          new ReducerJoinerTransformation(batchJoiner, mapOutputKeyClassName, mapOutputValClassName,
                                          runtimeContext.getInputSchemas().size()), stageMetrics,
          taskContext.getDataTracer(stageName), collector);
      }
    }

    Transformation transformation = getInitializedTransformation(stageSpec);
    boolean isLimitingSource =
      taskContext.getDataTracer(stageName).isEnabled() && BatchSource.PLUGIN_TYPE.equals(pluginType) && isMapPhase;
    transformation = isLimitingSource ? new LimitingTransform(transformation, numberOfRecordsPreview) : transformation;
    // we emit metrics for records into alert publishers when the actual alerts are published,
    // not when we write the alerts to the temporary dataset
    String recordsInMetric = AlertPublisher.PLUGIN_TYPE.equals(pluginType) ? null : Constants.Metrics.RECORDS_IN;
    return new TrackedTransform<>(transformation, stageMetrics, recordsInMetric, Constants.Metrics.RECORDS_OUT,
                                  taskContext.getDataTracer(stageName), collector);
  }

  /**
   * Create a transform executor for the specified pipeline. Will instantiate and initialize all sources,
   * transforms, and sinks in the pipeline.
   *
   * @param pipeline the pipeline to create a transform executor for
   * @param outputWriter writes output records to the mapreduce context
   * @param errorOutputs writes to error datasets
   * @return executor for the pipeline
   * @throws InstantiationException if there was an error instantiating a plugin
   * @throws Exception              if there was an error initializing a plugin
   */
  public <KEY_OUT, VAL_OUT> PipeTransformExecutor<T> create(PipelinePhase pipeline,
                                                            OutputWriter<KEY_OUT, VAL_OUT> outputWriter,
                                                            Map<String, ErrorOutputWriter<Object, Object>> errorOutputs)
    throws Exception {
    // populate the pipe stages in reverse topological order to ensure that an output is always created before its
    // input. this will allow us to setup all outputs for a stage when we get to it.
    List<String> traversalOrder = pipeline.getDag().getTopologicalOrder();
    Collections.reverse(traversalOrder);

    Map<String, PipeStage> pipeStages = new HashMap<>();
    for (String stageName : traversalOrder) {
      pipeStages.put(stageName, getPipeStage(pipeline, stageName, pipeStages, outputWriter, errorOutputs));
    }

    // sourceStageName will be null in reducers, so need to handle that case
    Set<String> startingPoints = (sourceStageName == null) ? pipeline.getSources() : Sets.newHashSet(sourceStageName);
    return new PipeTransformExecutor<>(pipeStages, startingPoints);
  }

  private PipeStage getPipeStage(PipelinePhase pipeline, String stageName, Map<String, PipeStage> pipeStages,
                                 OutputWriter<?, ?> outputWriter,
                                 Map<String, ErrorOutputWriter<Object, Object>> errorOutputs) throws Exception {
    StageSpec stageSpec = pipeline.getStage(stageName);
    String pluginType = stageSpec.getPluginType();

    /*
     * Every stage in the pipe uses a PipeEmitter except for ending stages, which use an emitter that
     * lets them write directly to the MapReduce context instead of some other stage.
     *
     * Since every PipeEmitter outputs RecordInfo because some stages require the stage the record was from.
     * Connector sinks require the stage name so that they can preserve it for the next phase.
     * Joiners inside mappers require it so that they can include the stage name with each record, so that on the
     * reduce side, Joiner.join() can be sent records along with the stage they came from.
     *
     * PipeEmitter emits ErrorRecord for ErrorTransforms.
     *
     * Every other non-starting stage in the pipe takes RecordInfo as input.
     * These stages need to be wrapped so that they extract the actual
     * value inside a RecordInfo before passing it to the actual plugin.
     */

    // handle ending stage case, which don't use PipeEmitter
    if (pipeline.getSinks().contains(stageName)) {
      if (Constants.Connector.PLUGIN_TYPE.equals(pluginType) || BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {
        // connectors and joiners require the getting the RecordInfo class directly instead of unwrapping it
        Transformation<RecordInfo<Object>, Object> sink = getTransformation(stageSpec);
        return new DirectOutputPipeStage<>(stageName, sink, new SinkEmitter<>(stageName, outputWriter));
      } else {
        // others (batchsink, aggregators, alertpublisher), only required the value within the RecordInfo
        return new UnwrapPipeStage<>(stageName, getTransformation(stageSpec),
                                     new SinkEmitter<>(stageName, outputWriter));
      }
    }

    // create PipeEmitter, which holds all output PipeStages it needs to write to and wraps any output it gets
    // into a RecordInfo
    // ConnectorSources require a special emitter since they need to build RecordInfo from the temporary dataset
    PipeEmitter.Builder emitterBuilder =
      Constants.Connector.PLUGIN_TYPE.equals(pluginType) && pipeline.getSources().contains(stageName) ?
        ConnectorSourceEmitter.builder(stageName) : PipeEmitter.builder(stageName);

    emitterBuilder.setErrorOutputWriter(errorOutputs.get(stageName));

    Map<String, StageSpec.Port> outputPorts = stageSpec.getOutputPorts();
    for (String outputStageName : pipeline.getDag().getNodeOutputs(stageName)) {
      StageSpec outputStageSpec = pipeline.getStage(outputStageName);
      String outputStageType = outputStageSpec.getPluginType();
      PipeStage outputPipeStage = pipeStages.get(outputStageName);

      if (ErrorTransform.PLUGIN_TYPE.equals(outputStageType)) {
        emitterBuilder.addErrorConsumer(outputPipeStage);
      } else if (AlertPublisher.PLUGIN_TYPE.equals(outputStageType)) {
        emitterBuilder.addAlertConsumer(outputPipeStage);
      } else if (Constants.Connector.PLUGIN_TYPE.equals(pluginType)) {
        // connectors only have a single output
        emitterBuilder.addOutputConsumer(outputPipeStage);
      } else {
        // if the output is a connector like agg5.connector, the outputPorts will contain the original 'agg5' as
        // a key, but not 'agg5.connector' so we need to lookup the original stage from the connector's plugin spec
        String originalOutputName = Constants.Connector.PLUGIN_TYPE.equals(outputStageType) ?
          outputStageSpec.getPlugin().getProperties().get(Constants.Connector.ORIGINAL_NAME) : outputStageName;

        String port = outputPorts.containsKey(originalOutputName) ? outputPorts.get(originalOutputName).getPort()
          : null;
        if (port != null) {
          emitterBuilder.addOutputConsumer(outputPipeStage, port);
        } else {
          emitterBuilder.addOutputConsumer(outputPipeStage);
        }
      }
    }
    PipeEmitter pipeEmitter = emitterBuilder.build();

    if (SplitterTransform.PLUGIN_TYPE.equals(pluginType)) {
      // this is a SplitterTransform, needs to emit records to the right outputs based on port
      return new MultiOutputTransformPipeStage<>(stageName, getMultiOutputTransform(stageSpec), pipeEmitter);
    } else {
      return new UnwrapPipeStage<>(stageName, getTransformation(stageSpec), pipeEmitter);
    }
  }

  /**
   * Instantiates and initializes the plugin for the stage.
   *
   * @param stageInfo the stage info
   * @return the initialized Transformation
   * @throws InstantiationException if the plugin for the stage could not be instantiated
   * @throws Exception              if there was a problem initializing the plugin
   */
  private <T extends Transformation & StageLifecycle<BatchRuntimeContext>> Transformation
  getInitializedTransformation(StageSpec stageInfo) throws Exception {
    BatchRuntimeContext runtimeContext = createRuntimeContext(stageInfo);
    T plugin = pluginInstantiator.newPluginInstance(stageInfo.getName(), macroEvaluator);
    plugin.initialize(runtimeContext);
    return plugin;
  }

  private static <IN, OUT> TrackedTransform<IN, OUT> getTrackedEmitKeyStep(Transformation<IN, OUT> transform,
                                                                           StageMetrics stageMetrics,
                                                                           DataTracer dataTracer,
                                                                           StageStatisticsCollector collector) {
    return new TrackedTransform<>(transform, stageMetrics, Constants.Metrics.RECORDS_IN, null, dataTracer, collector);
  }

  private static <IN, OUT> TrackedTransform<IN, OUT> getTrackedAggregateStep(Transformation<IN, OUT> transform,
                                                                             StageMetrics stageMetrics,
                                                                             DataTracer dataTracer,
                                                                             StageStatisticsCollector collector) {
    // 'aggregator.groups' is the number of groups output by the aggregator
    return new TrackedTransform<>(transform, stageMetrics, Constants.Metrics.AGG_GROUPS, Constants.Metrics.RECORDS_OUT,
                                  dataTracer, collector);
  }

  private static <IN, OUT> TrackedTransform<IN, OUT> getTrackedMergeStep(Transformation<IN, OUT> transform,
                                                                         StageMetrics stageMetrics,
                                                                         DataTracer dataTracer,
                                                                         StageStatisticsCollector collector) {
    return new TrackedTransform<>(transform, stageMetrics, null, Constants.Metrics.RECORDS_OUT, dataTracer, collector);
  }

  /**
   * A Transformation that uses join's joinOn method. Converts join value to tagged output with stage name for
   * reducer. It uses {@link TaggedWritable} to tag join value with stage name so that we can use stage name
   * in reduce phase.
   *
   * @param <JOIN_KEY>     type of join key
   * @param <INPUT_RECORD> type of input record
   * @param <OUT>          type of the output of joiner
   * @param <OUT_KEY>      type of the map output
   */
  private static class MapperJoinerTransformation<JOIN_KEY, INPUT_RECORD, OUT, OUT_KEY
    extends Writable, OUT_VALUE extends Writable>
    implements Transformation<RecordInfo<INPUT_RECORD>, KeyValue<OUT_KEY, TaggedWritable<OUT_VALUE>>> {
    private final Joiner<JOIN_KEY, INPUT_RECORD, OUT> joiner;
    private final WritableConversion<JOIN_KEY, OUT_KEY> keyConversion;
    private final WritableConversion<INPUT_RECORD, OUT_VALUE> inputConversion;

    MapperJoinerTransformation(Joiner<JOIN_KEY, INPUT_RECORD, OUT> joiner, String joinKeyClassName,
                               String joinInputClassName) {
      this.joiner = joiner;
      WritableConversion<JOIN_KEY, OUT_KEY> keyConversion = WritableConversions.getConversion(joinKeyClassName);
      WritableConversion<INPUT_RECORD, OUT_VALUE> inputConversion =
        WritableConversions.getConversion(joinInputClassName);
      this.keyConversion = keyConversion == null ? new CastConversion<JOIN_KEY, OUT_KEY>() : keyConversion;
      this.inputConversion = inputConversion == null ? new CastConversion<INPUT_RECORD, OUT_VALUE>() : inputConversion;
    }

    @Override
    public void transform(RecordInfo<INPUT_RECORD> input,
                          Emitter<KeyValue<OUT_KEY, TaggedWritable<OUT_VALUE>>> emitter) throws Exception {
      String stageName = input.getFromStage();
      JOIN_KEY key = joiner.joinOn(stageName, input.getValue());
      TaggedWritable<OUT_VALUE> output = new TaggedWritable<>(stageName,
                                                              inputConversion.toWritable(input.getValue()));
      emitter.emit(new KeyValue<>(keyConversion.toWritable(key), output));
    }
  }

  /**
   * A Transformation that uses an join's emit method to emit joinResults.
   *
   * @param <JOIN_KEY>     type of join key
   * @param <INPUT_RECORD> type of input record
   * @param <OUT>          type of the output of joiner
   * @param <REDUCE_KEY>   type of the reduce key=
   */
  private static class ReducerJoinerTransformation<JOIN_KEY, INPUT_RECORD, OUT,
    REDUCE_KEY extends WritableComparable, REDUCE_VALUE extends Writable>
    implements Transformation<KeyValue<REDUCE_KEY, Iterator<TaggedWritable<REDUCE_VALUE>>>, OUT> {
    private final Joiner<JOIN_KEY, INPUT_RECORD, OUT> joiner;
    private final WritableConversion<JOIN_KEY, REDUCE_KEY> keyConversion;
    private final WritableConversion<INPUT_RECORD, REDUCE_VALUE> inputConversion;
    private final int numOfInputs;

    ReducerJoinerTransformation(Joiner<JOIN_KEY, INPUT_RECORD, OUT> joiner, String joinKeyClassName,
                                String joinInputClassName, int numOfInputs) {
      this.joiner = joiner;
      WritableConversion<JOIN_KEY, REDUCE_KEY> keyConversion = WritableConversions.getConversion(joinKeyClassName);
      WritableConversion<INPUT_RECORD, REDUCE_VALUE> inputConversion =
        WritableConversions.getConversion(joinInputClassName);
      this.keyConversion = keyConversion == null ? new CastConversion<JOIN_KEY, REDUCE_KEY>() : keyConversion;
      this.inputConversion = inputConversion == null ?
        new CastConversion<INPUT_RECORD, REDUCE_VALUE>() : inputConversion;
      this.numOfInputs = numOfInputs;
    }

    @Override
    public void transform(KeyValue<REDUCE_KEY, Iterator<TaggedWritable<REDUCE_VALUE>>> input,
                          Emitter<OUT> emitter) throws Exception {
      JOIN_KEY joinKey = keyConversion.fromWritable(input.getKey());
      Iterator<JoinElement<INPUT_RECORD>> inputIterator = Iterators.transform(input.getValue(), new
        Function<TaggedWritable<REDUCE_VALUE>, JoinElement<INPUT_RECORD>>() {
          @Nullable
          @Override
          public JoinElement<INPUT_RECORD> apply(@Nullable TaggedWritable<REDUCE_VALUE> input) {
            return new JoinElement<>(input.getStageName(), inputConversion.fromWritable(input.getRecord()));
          }
        });

      Join join = new Join<>(joiner, joinKey, inputIterator, numOfInputs, emitter);
      join.joinRecords();
    }
  }


  /**
   * A Transformation that uses an aggregator's groupBy method. Supports applying a function to the types
   * returned by the aggregator. These functions are used when the aggregator outputs group keys that are not
   * WritableComparable and values that are not Writable. For example, aggregators that output StructuredRecord
   * will need some function to change a StructuredRecord to a StructuredRecordWritable so that we can use this
   * in mapreduce.
   *
   * @param <GROUP_KEY> type of group key output by the aggregator
   * @param <GROUP_VAL> type of group value used by the aggregator
   * @param <OUT_KEY>   type of output key for mapreduce. Must implement WritableComparable
   * @param <OUT_VAL>   type of output value for mapreduce. Must implement Writable
   */
  private static class MapperAggregatorTransformation<GROUP_KEY, GROUP_VAL, OUT_KEY extends Writable,
    OUT_VAL extends Writable> implements Transformation<GROUP_VAL, KeyValue<OUT_KEY, OUT_VAL>> {
    private final Aggregator<GROUP_KEY, GROUP_VAL, ?> aggregator;
    private final NoErrorEmitter<GROUP_KEY> groupKeyEmitter;
    private final WritableConversion<GROUP_KEY, OUT_KEY> keyConversion;
    private final WritableConversion<GROUP_VAL, OUT_VAL> valConversion;

    MapperAggregatorTransformation(Aggregator<GROUP_KEY, GROUP_VAL, ?> aggregator,
                                   String groupKeyClassName,
                                   String groupValClassName) {
      this.aggregator = aggregator;
      this.groupKeyEmitter =
        new NoErrorEmitter<>("Errors and Alerts cannot be emitted from the groupBy method of an aggregator");
      WritableConversion<GROUP_KEY, OUT_KEY> keyConversion = WritableConversions.getConversion(groupKeyClassName);
      WritableConversion<GROUP_VAL, OUT_VAL> valConversion = WritableConversions.getConversion(groupValClassName);
      // if the conversion is null, it means the user is using a Writable already
      this.keyConversion = keyConversion == null ? new CastConversion<GROUP_KEY, OUT_KEY>() : keyConversion;
      this.valConversion = valConversion == null ? new CastConversion<GROUP_VAL, OUT_VAL>() : valConversion;
    }

    @Override
    public void transform(GROUP_VAL input, Emitter<KeyValue<OUT_KEY, OUT_VAL>> emitter) throws Exception {
      groupKeyEmitter.reset();
      aggregator.groupBy(input, groupKeyEmitter);
      for (GROUP_KEY groupKey : groupKeyEmitter.getEntries()) {
        emitter.emit(new KeyValue<>(keyConversion.toWritable(groupKey), valConversion.toWritable(input)));
      }
    }
  }

  /**
   * A Transformation that uses an aggregator's aggregate method. Supports applying a function to the types
   * send as input to the aggregator. These functions are used when the aggregator takes group keys that are not
   * WritableComparable and values that are not Writable. For example, aggregators that aggregate StructuredRecords
   * will need some function to change a StructuredRecordWritable to a StructuredRecord so that we can use this
   * in mapreduce.
   *
   * @param <GROUP_KEY>  type of group key output by the aggregator
   * @param <GROUP_VAL>  type of group value used by the aggregator
   * @param <REDUCE_KEY> type of reduce key for mapreduce. Must implement WritableComparable
   * @param <REDUCE_VAL> type of reduce value for mapreduce. Must implement Writable
   */
  private static class ReducerAggregatorTransformation<GROUP_KEY, GROUP_VAL, OUT,
    REDUCE_KEY extends WritableComparable, REDUCE_VAL extends Writable>
    implements Transformation<KeyValue<REDUCE_KEY, Iterator<REDUCE_VAL>>, OUT> {
    private final Aggregator<GROUP_KEY, GROUP_VAL, OUT> aggregator;
    private final WritableConversion<GROUP_KEY, REDUCE_KEY> keyConversion;
    private final WritableConversion<GROUP_VAL, REDUCE_VAL> valConversion;

    ReducerAggregatorTransformation(Aggregator<GROUP_KEY, GROUP_VAL, OUT> aggregator,
                                    String groupKeyClassName,
                                    String groupValClassName) {
      this.aggregator = aggregator;
      WritableConversion<GROUP_KEY, REDUCE_KEY> keyConversion = WritableConversions.getConversion(groupKeyClassName);
      WritableConversion<GROUP_VAL, REDUCE_VAL> valConversion = WritableConversions.getConversion(groupValClassName);
      this.keyConversion = keyConversion == null ? new CastConversion<GROUP_KEY, REDUCE_KEY>() : keyConversion;
      this.valConversion = valConversion == null ? new CastConversion<GROUP_VAL, REDUCE_VAL>() : valConversion;
    }

    @Override
    public void transform(KeyValue<REDUCE_KEY, Iterator<REDUCE_VAL>> input, Emitter<OUT> emitter) throws Exception {
      GROUP_KEY groupKey = keyConversion.fromWritable(input.getKey());
      Iterator<GROUP_VAL> iter = Iterators.transform(input.getValue(), new Function<REDUCE_VAL, GROUP_VAL>() {
        @Nullable
        @Override
        public GROUP_VAL apply(@Nullable REDUCE_VAL input) {
          return valConversion.fromWritable(input);
        }
      });
      aggregator.aggregate(groupKey, iter, emitter);
    }
  }

  /**
   * Conversion that doesn't do anything but cast types to each other.
   * This is used in the MapperAggregatorTransformation and ReducerAggregatorTransformation when the user is already
   * using a Writable class and we don't need to do any conversion.
   *
   * @param <T> type of object to convert to a Writable
   * @param <W> the Writable type to convert to
   */
  @SuppressWarnings("unchecked")
  private static class CastConversion<T, W extends Writable> extends WritableConversion<T, W> {

    @Override
    public W toWritable(T val) {
      return (W) val;
    }

    @Override
    public T fromWritable(W val) {
      return (T) val;
    }
  }
}
