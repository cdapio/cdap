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

package io.cdap.cdap.etl.batch.mapreduce;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.mapreduce.MapReduceTaskContext;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.etl.api.Aggregator;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.JoinElement;
import io.cdap.cdap.etl.api.Joiner;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.Transformation;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchAutoJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchReducibleAggregator;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.api.join.JoinStage;
import io.cdap.cdap.etl.batch.PipelinePluginInstantiator;
import io.cdap.cdap.etl.batch.conversion.WritableConversion;
import io.cdap.cdap.etl.batch.conversion.WritableConversions;
import io.cdap.cdap.etl.batch.join.Join;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultAutoJoinerContext;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.DefaultStageMetrics;
import io.cdap.cdap.etl.common.NoErrorEmitter;
import io.cdap.cdap.etl.common.NoopStageStatisticsCollector;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.common.TrackedTransform;
import io.cdap.cdap.etl.common.TransformExecutor;
import io.cdap.cdap.etl.common.plugin.AggregatorBridge;
import io.cdap.cdap.etl.common.plugin.JoinerBridge;
import io.cdap.cdap.etl.exec.DirectOutputPipeStage;
import io.cdap.cdap.etl.exec.PipeStage;
import io.cdap.cdap.etl.exec.TransformExecutorFactory;
import io.cdap.cdap.etl.exec.UnwrapPipeStage;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.validation.LoggingFailureCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Helps create {@link TransformExecutor TransformExecutors}.
 *
 * @param <T> the type of input for the created transform executors
 */
public class MapReduceTransformExecutorFactory<T> extends TransformExecutorFactory<T> {
  private final MapReduceTaskContext taskContext;
  private final String mapOutputKeyClassName;
  private final String mapOutputValClassName;
  private final BasicArguments arguments;
  private final OutputWriter<Object, Object> outputWriter;
  private final boolean isMapPhase;

  public MapReduceTransformExecutorFactory(MapReduceTaskContext taskContext,
                                           PipelinePluginInstantiator pluginInstantiator,
                                           Metrics metrics,
                                           BasicArguments arguments,
                                           String sourceStageName,
                                           boolean collectStageStatistics,
                                           OutputWriter<Object, Object> outputWriter) {
    super(pluginInstantiator, new DefaultMacroEvaluator(arguments, taskContext.getLogicalStartTime(),
                                                        taskContext, taskContext.getNamespace()),
          metrics, sourceStageName, collectStageStatistics);
    this.taskContext = taskContext;
    JobContext hadoopContext = (JobContext) taskContext.getHadoopContext();
    Configuration hConf = hadoopContext.getConfiguration();
    this.mapOutputKeyClassName = hConf.get(ETLMapReduce.MAP_KEY_CLASS);
    this.mapOutputValClassName = hConf.get(ETLMapReduce.MAP_VAL_CLASS);
    this.isMapPhase = hadoopContext instanceof Mapper.Context;
    this.arguments = arguments;
    this.outputWriter = outputWriter;
  }

  @Override
  protected MapReduceRuntimeContext createRuntimeContext(StageSpec stageInfo) {
    PipelineRuntime pipelineRuntime = new PipelineRuntime(taskContext, metrics, arguments);
    return new MapReduceRuntimeContext(taskContext, pipelineRuntime, stageInfo);
  }

  @Override
  protected StageStatisticsCollector getStatisticsCollector(String stageName) {
    return new MapReduceStageStatisticsCollector(stageName, (TaskAttemptContext) taskContext.getHadoopContext());
  }

  @Override
  protected DataTracer getDataTracer(String stageName) {
    return taskContext.getDataTracer(stageName);
  }

  @Override
  protected PipeStage getSinkPipeStage(StageSpec stageSpec) throws Exception {
    String stageName = stageSpec.getName();
    String pluginType = stageSpec.getPluginType();
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

  @SuppressWarnings("unchecked")
  @Override
  protected <IN, OUT> TrackedTransform<IN, OUT> getTransformation(StageSpec stageSpec) throws Exception {
    String stageName = stageSpec.getName();
    String pluginType = stageSpec.getPluginType();
    StageMetrics stageMetrics = new DefaultStageMetrics(metrics, stageName);
    TaskAttemptContext taskAttemptContext = (TaskAttemptContext) taskContext.getHadoopContext();
    StageStatisticsCollector collector = collectStageStatistics ?
      new MapReduceStageStatisticsCollector(stageName, taskAttemptContext) : new NoopStageStatisticsCollector();
    if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {
      Object plugin = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
      BatchAggregator<?, ?, ?> batchAggregator;
      if (plugin instanceof BatchReducibleAggregator) {
        BatchReducibleAggregator<?, ?, ?, ?> reducibleAggregator = (BatchReducibleAggregator<?, ?, ?, ?>) plugin;
        batchAggregator = new AggregatorBridge<>(reducibleAggregator);
      } else {
        batchAggregator = (BatchAggregator<?, ?, ?>) plugin;
      }

      BatchRuntimeContext runtimeContext = createRuntimeContext(stageSpec);
      batchAggregator.initialize(runtimeContext);
      if (isMapPhase) {
        return getTrackedEmitKeyStep(new MapperAggregatorTransformation(batchAggregator, mapOutputKeyClassName,
                                                                        mapOutputValClassName),
                                     stageMetrics, getDataTracer(stageName), collector);
      } else {
        return getTrackedAggregateStep(new ReducerAggregatorTransformation(batchAggregator,
                                                                           mapOutputKeyClassName,
                                                                           mapOutputValClassName),
                                       stageMetrics, getDataTracer(stageName), collector);
      }
    } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {
      Object plugin = pluginInstantiator.newPluginInstance(stageName, macroEvaluator);
      BatchJoiner<?, ?, ?> batchJoiner;
      Set<String> filterNullKeyStages = new HashSet<>();
      if (plugin instanceof BatchAutoJoiner) {
        BatchAutoJoiner autoJoiner = (BatchAutoJoiner) plugin;
        FailureCollector failureCollector = new LoggingFailureCollector(stageName, stageSpec.getInputSchemas());
        DefaultAutoJoinerContext context = DefaultAutoJoinerContext.from(stageSpec.getInputSchemas(),
                                                                         failureCollector);
        // definition will be non-null due to validate by PipelinePhasePreparer at the start of the run
        JoinDefinition joinDefinition = autoJoiner.define(context);
        JoinCondition condition = joinDefinition.getCondition();
        // should never happen as it's checked at deployment time, but add this to be safe.
        if (condition.getOp() != JoinCondition.Op.KEY_EQUALITY) {
          failureCollector.addFailure(
            String.format("Join stage '%s' uses a %s condition, which is not supported with the MapReduce engine.",
                          stageName, condition.getOp()),
            "Switch to a different execution engine.");
        }
        failureCollector.getOrThrowException();
        batchJoiner = new JoinerBridge(stageName, autoJoiner, joinDefinition);
        // null safe equality means A.id = B.id will match when the id is null
        // if it's not null safe, A.id = B.id will not match when the id is null
        // this is the same as filtering out records that have a null key if they are from an optional stage
        if (condition.getOp() == JoinCondition.Op.KEY_EQUALITY && !((JoinCondition.OnKeys) condition).isNullSafe()) {
          filterNullKeyStages = joinDefinition.getStages().stream()
            .filter(s -> !s.isRequired())
            .map(JoinStage::getStageName)
            .collect(Collectors.toSet());
        }
      } else {
        batchJoiner = (BatchJoiner<?, ?, ?>) plugin;
      }

      BatchJoinerRuntimeContext runtimeContext = createRuntimeContext(stageSpec);
      batchJoiner.initialize(runtimeContext);
      if (isMapPhase) {
        return getTrackedEmitKeyStep(
          new MapperJoinerTransformation(batchJoiner, mapOutputKeyClassName,
                                         mapOutputValClassName, filterNullKeyStages),
          stageMetrics,
          getDataTracer(stageName), collector);
      } else {
        return getTrackedMergeStep(
          new ReducerJoinerTransformation(batchJoiner, mapOutputKeyClassName, mapOutputValClassName,
                                          runtimeContext.getInputSchemas().size()), stageMetrics,
          getDataTracer(stageName), collector);
      }
    }

    return super.getTransformation(stageSpec);
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
    private final BiPredicate<String, JOIN_KEY> shouldFilter;

    MapperJoinerTransformation(Joiner<JOIN_KEY, INPUT_RECORD, OUT> joiner, String joinKeyClassName,
                               String joinInputClassName, Set<String> filterNullKeyStages) {
      this.joiner = joiner;
      WritableConversion<JOIN_KEY, OUT_KEY> keyConversion = WritableConversions.getConversion(joinKeyClassName);
      WritableConversion<INPUT_RECORD, OUT_VALUE> inputConversion =
        WritableConversions.getConversion(joinInputClassName);
      this.keyConversion = keyConversion == null ? new CastConversion<>() : keyConversion;
      this.inputConversion = inputConversion == null ? new CastConversion<>() : inputConversion;
      if (StructuredRecord.class.getName().equals(joinKeyClassName)) {
        /*
           Filter out the record if it comes from an optional stage
           and the key is null, or if any of the fields in the key is null.
           For example, suppose we are performing a left outer join on:

            A (id, name) = (0, alice), (null, bob)
            B (id, email) = (0, alice@example.com), (null, placeholder@example.com)

           The final output should be:

           joined (A.id, A.name, B.email) = (0, alice, alice@example.com), (null, bob, null, null)

           that is, the bob record should not be joined to the placeholder@example email, even though both their
           ids are null.
         */
        shouldFilter = (stage, key) -> {
          if (!filterNullKeyStages.contains(stage)) {
            return false;
          }
          if (key == null) {
            return true;
          }
          StructuredRecord record = (StructuredRecord) key;
          for (Schema.Field field : record.getSchema().getFields()) {
            if (record.get(field.getName()) == null) {
              return true;
            }
          }
          return false;
        };
      } else {
        // filter out the record if it comes from an optional stage and the key is null
        shouldFilter = (stage, key) -> key == null & filterNullKeyStages.contains(stage);
      }
    }

    @Override
    public void transform(RecordInfo<INPUT_RECORD> input,
                          Emitter<KeyValue<OUT_KEY, TaggedWritable<OUT_VALUE>>> emitter) throws Exception {
      String stageName = input.getFromStage();
      Collection<JOIN_KEY> keys = joiner.getJoinKeys(stageName, input.getValue());
      for (JOIN_KEY key : keys) {
        if (shouldFilter.test(stageName, key)) {
          continue;
        }
        TaggedWritable<OUT_VALUE> output = new TaggedWritable<>(stageName,
                                                                inputConversion.toWritable(input.getValue()));

        emitter.emit(new KeyValue<>(keyConversion.toWritable(key), output));
      }
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
      this.keyConversion = keyConversion == null ? new CastConversion<>() : keyConversion;
      this.inputConversion = inputConversion == null ? new CastConversion<>() : inputConversion;
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
      this.keyConversion = keyConversion == null ? new CastConversion<>() : keyConversion;
      this.valConversion = valConversion == null ? new CastConversion<>() : valConversion;
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
      this.keyConversion = keyConversion == null ? new CastConversion<>() : keyConversion;
      this.valConversion = valConversion == null ? new CastConversion<>() : valConversion;
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
