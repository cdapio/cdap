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
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.ErrorTransform;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.Joiner;
import co.cask.cdap.etl.api.StageLifecycle;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.PipeTransformDetail;
import co.cask.cdap.etl.batch.PipeTransformExecutor;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.batch.conversion.WritableConversion;
import co.cask.cdap.etl.batch.conversion.WritableConversions;
import co.cask.cdap.etl.batch.join.Join;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.cdap.etl.common.LocationAwareMDCWrapperLogger;
import co.cask.cdap.etl.common.NoErrorEmitter;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.TrackedTransform;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.common.preview.LimitingTransform;
import co.cask.cdap.etl.spec.StageSpec;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
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
  private final Map<String, Map<String, String>> pluginRuntimeArgs;
  private final MapReduceTaskContext taskContext;
  private final String mapOutputKeyClassName;
  private final String mapOutputValClassName;
  private final int numberOfRecordsPreview;
  private boolean isMapPhase;
  private static final Logger PIPELINE_LOG =
    new LocationAwareMDCWrapperLogger(LoggerFactory.getLogger(MapReduceTransformExecutorFactory.class),
                                      Constants.EVENT_TYPE_TAG, Constants.PIPELINE_LIFECYCLE_TAG_VALUE);

  public MapReduceTransformExecutorFactory(MapReduceTaskContext taskContext,
                                           PipelinePluginInstantiator pluginInstantiator,
                                           Metrics metrics,
                                           Map<String, Map<String, String>> pluginRuntimeArgs,
                                           String sourceStageName,
                                           int numberOfRecordsPreview) {
    this.taskContext = taskContext;
    this.pluginRuntimeArgs = pluginRuntimeArgs;
    this.numberOfRecordsPreview = numberOfRecordsPreview;
    this.pluginInstantiator = pluginInstantiator;
    this.metrics = metrics;
    this.sourceStageName = sourceStageName;
    this.macroEvaluator =
      new DefaultMacroEvaluator(taskContext.getWorkflowToken(), taskContext.getRuntimeArguments(),
                                taskContext.getLogicalStartTime(), taskContext, taskContext.getNamespace());
    JobContext hadoopContext = (JobContext) taskContext.getHadoopContext();
    Configuration hConf = hadoopContext.getConfiguration();
    this.mapOutputKeyClassName = hConf.get(ETLMapReduce.MAP_KEY_CLASS);
    this.mapOutputValClassName = hConf.get(ETLMapReduce.MAP_VAL_CLASS);
    this.isMapPhase = hadoopContext instanceof Mapper.Context;
  }

  private MapReduceRuntimeContext createRuntimeContext(StageSpec stageInfo) {
    Map<String, String> stageRuntimeArgs = pluginRuntimeArgs.get(stageInfo.getName());
    if (stageRuntimeArgs == null) {
      stageRuntimeArgs = new HashMap<>();
    }
    return new MapReduceRuntimeContext(taskContext, metrics, new DatasetContextLookupProvider(taskContext),
                                       stageRuntimeArgs, stageInfo);
  }

  @SuppressWarnings("unchecked")
  private TrackedTransform getTransformation(StageSpec stageInfo) throws Exception {
    DefaultMacroEvaluator macroEvaluator = new DefaultMacroEvaluator(taskContext.getWorkflowToken(),
                                                                     taskContext.getRuntimeArguments(),
                                                                     taskContext.getLogicalStartTime(), taskContext,
                                                                     taskContext.getNamespace());
    String stageName = stageInfo.getName();
    String pluginType = stageInfo.getPluginType();
    StageMetrics stageMetrics = new DefaultStageMetrics(metrics, stageName);
    if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {
      BatchAggregator<?, ?, ?> batchAggregator = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
      BatchRuntimeContext runtimeContext = createRuntimeContext(stageInfo);
      batchAggregator.initialize(runtimeContext);
      if (isMapPhase) {
        return getTrackedEmitKeyStep(new MapperAggregatorTransformation(batchAggregator, mapOutputKeyClassName,
                                                                        mapOutputValClassName),
                                     stageMetrics, taskContext.getDataTracer(stageName));
      } else {
        return getTrackedAggregateStep(new ReducerAggregatorTransformation(batchAggregator,
                                                                           mapOutputKeyClassName,
                                                                           mapOutputValClassName),
                                       stageMetrics, taskContext.getDataTracer(stageName));
      }
    } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {
      BatchJoiner<?, ?, ?> batchJoiner = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
      BatchJoinerRuntimeContext runtimeContext = createRuntimeContext(stageInfo);
      batchJoiner.initialize(runtimeContext);
      if (isMapPhase) {
        return getTrackedEmitKeyStep(
          new MapperJoinerTransformation(batchJoiner, mapOutputKeyClassName, mapOutputValClassName), stageMetrics,
          taskContext.getDataTracer(stageName));
      } else {
        return getTrackedMergeStep(
          new ReducerJoinerTransformation(batchJoiner, mapOutputKeyClassName, mapOutputValClassName,
                                          runtimeContext.getInputSchemas().size()), stageMetrics,
          taskContext.getDataTracer(stageName));
      }
    }

    Transformation transformation = getInitializedTransformation(stageInfo);
    boolean isLimitingSource =
      taskContext.getDataTracer(stageName).isEnabled() && BatchSource.PLUGIN_TYPE.equals(pluginType) && isMapPhase;
    return new TrackedTransform(
      isLimitingSource ? new LimitingTransform(transformation, numberOfRecordsPreview) : transformation,
      stageMetrics, taskContext.getDataTracer(stageName));
  }

  /**
   * Create a transform executor for the specified pipeline. Will instantiate and initialize all sources,
   * transforms, and sinks in the pipeline.
   *
   * @param pipeline the pipeline to create a transform executor for
   * @return executor for the pipeline
   * @throws InstantiationException if there was an error instantiating a plugin
   * @throws Exception              if there was an error initializing a plugin
   */
  public <KEY_OUT, VAL_OUT> PipeTransformExecutor<T> create(PipelinePhase pipeline,
                                                            OutputWriter<KEY_OUT, VAL_OUT> outputWriter,
                                                            Map<String, ErrorOutputWriter<Object, Object>>
                                                              transformErrorSinkMap)
    throws Exception {
    Map<String, PipeTransformDetail> transformations = new HashMap<>();
    Set<String> sources = pipeline.getSources();

    // recursively set PipeTransformDetail for all the stages
    for (String source : sources) {
      setPipeTransformDetail(pipeline, source, transformations, transformErrorSinkMap, outputWriter);
    }

    // sourceStageName will be null in reducers, so need to handle that case
    Set<String> startingPoints = (sourceStageName == null) ? pipeline.getSources() : Sets.newHashSet(sourceStageName);
    return new PipeTransformExecutor<>(transformations, startingPoints);
  }

  private <KEY_OUT, VAL_OUT> void setPipeTransformDetail(PipelinePhase pipeline, String stageName,
                                                         Map<String, PipeTransformDetail> transformations,
                                                         Map<String, ErrorOutputWriter<Object, Object>>
                                                           transformErrorSinkMap,
                                                         OutputWriter<KEY_OUT, VAL_OUT> outputWriter)
    throws Exception {
    if (pipeline.getSinks().contains(stageName)) {
      StageSpec stageInfo = pipeline.getStage(stageName);
      // If there is a connector sink/ joiner at the end of pipeline, do not remove stage name. This is needed to save
      // stageName along with the record in connector sink and joiner takes input along with stageName
      String pluginType = stageInfo.getPluginType();
      boolean removeStageName = !(pluginType.equals(Constants.CONNECTOR_TYPE) ||
        pluginType.equals(BatchJoiner.PLUGIN_TYPE));
      boolean isErrorConsumer = pluginType.equals(ErrorTransform.PLUGIN_TYPE);
      transformations.put(stageName, new PipeTransformDetail(stageName, removeStageName, isErrorConsumer,
                                                             getTransformation(stageInfo),
                                                             new SinkEmitter<>(stageName, outputWriter)));
      return;
    }

    try {
      addTransformation(pipeline, stageName, transformations, transformErrorSinkMap);
    } catch (Exception e) {
      // Catch the Exception to generate a User Error Log for the Pipeline
      PIPELINE_LOG.error("Failed to start pipeline stage '{}' with the error: {}. Please review your pipeline " +
                          "configuration and check the system logs for more details.", stageName,
                        Throwables.getRootCause(e).getMessage(), Throwables.getRootCause(e));
      throw e;
    }

    for (String output : pipeline.getDag().getNodeOutputs(stageName)) {
      setPipeTransformDetail(pipeline, output, transformations, transformErrorSinkMap, outputWriter);
      transformations.get(stageName).addTransformation(output, transformations.get(output));
    }
  }

  private void addTransformation(PipelinePhase pipeline, String stageName,
                                 Map<String, PipeTransformDetail> transformations,
                                 Map<String, ErrorOutputWriter<Object, Object>> transformErrorSinkMap)
    throws Exception {
    StageSpec stageInfo = pipeline.getStage(stageName);
    String pluginType = stageInfo.getPluginType();
    ErrorOutputWriter<Object, Object> errorOutputWriter = transformErrorSinkMap.containsKey(stageName) ?
      transformErrorSinkMap.get(stageName) : null;

    // If stageName is a connector source, it will have stageName along with record so use ConnectorSourceEmitter
    if (pipeline.getSources().contains(stageName) && pluginType.equals(Constants.CONNECTOR_TYPE)) {
      transformations.put(stageName,
                          new PipeTransformDetail(stageName, true, false,
                                                  getTransformation(stageInfo),
                                                  new ConnectorSourceEmitter(stageName)));
    } else if (pluginType.equals(BatchJoiner.PLUGIN_TYPE) && isMapPhase) {
      // Do not remove stageName only for Map phase of BatchJoiner
      transformations.put(stageName,
                          new PipeTransformDetail(stageName, false, false,
                                                  getTransformation(stageInfo),
                                                  new TransformEmitter(stageName, errorOutputWriter)));
    } else {
      boolean isErrorConsumer = ErrorTransform.PLUGIN_TYPE.equals(pluginType);
      transformations.put(stageName,
                          new PipeTransformDetail(stageName, true, isErrorConsumer,
                                                  getTransformation(stageInfo),
                                                  new TransformEmitter(stageName, errorOutputWriter)));
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
                                                                             DataTracer dataTracer) {
    return new TrackedTransform<>(transform, stageMetrics, TrackedTransform.RECORDS_IN, null, dataTracer);
  }

  private static <IN, OUT> TrackedTransform<IN, OUT> getTrackedAggregateStep(Transformation<IN, OUT> transform,
                                                                               StageMetrics stageMetrics,
                                                                               DataTracer dataTracer) {
    // 'aggregator.groups' is the number of groups output by the aggregator
    return new TrackedTransform<>(transform, stageMetrics, "aggregator.groups", TrackedTransform.RECORDS_OUT,
                                  dataTracer);
  }

  private static <IN, OUT> TrackedTransform<IN, OUT> getTrackedMergeStep(Transformation<IN, OUT> transform,
                                                                           StageMetrics stageMetrics,
                                                                           DataTracer dataTracer) {
    return new TrackedTransform<>(transform, stageMetrics, null, TrackedTransform.RECORDS_OUT, dataTracer);
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
    implements Transformation<KeyValue<String, INPUT_RECORD>, KeyValue<OUT_KEY, TaggedWritable<OUT_VALUE>>> {
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
    public void transform(KeyValue<String, INPUT_RECORD> input,
                          Emitter<KeyValue<OUT_KEY, TaggedWritable<OUT_VALUE>>> emitter) throws Exception {
      String stageName = input.getKey();
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
        new NoErrorEmitter<>("Error records cannot be emitted from the groupBy method of an aggregator");
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
