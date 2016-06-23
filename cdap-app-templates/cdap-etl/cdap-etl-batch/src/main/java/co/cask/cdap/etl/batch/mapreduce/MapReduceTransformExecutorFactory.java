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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.Aggregator;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.JoinConfig;
import co.cask.cdap.etl.api.Joiner;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.batch.TransformExecutorFactory;
import co.cask.cdap.etl.batch.conversion.WritableConversion;
import co.cask.cdap.etl.batch.conversion.WritableConversions;
import co.cask.cdap.etl.batch.join.InnerJoin;
import co.cask.cdap.etl.batch.join.OuterJoin;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.common.DefaultEmitter;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.cdap.etl.common.TrackedEmitter;
import co.cask.cdap.etl.common.TrackedTransform;
import co.cask.cdap.etl.common.TransformDetail;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Creates transform executors for mapreduce programs.
 *
 * @param <T> the type of input for the created transform executors
 */
public class MapReduceTransformExecutorFactory<T> extends TransformExecutorFactory<T> {
  private final Map<String, Map<String, String>> pluginRuntimeArgs;
  private final MapReduceTaskContext taskContext;
  private final String mapOutputKeyClassName;
  private final String mapOutputValClassName;
  private final boolean isMapper;

  public MapReduceTransformExecutorFactory(MapReduceTaskContext taskContext,
                                           PipelinePluginInstantiator pluginInstantiator,
                                           Metrics metrics,
                                           Map<String, Map<String, String>> pluginRuntimeArgs) {
    super(pluginInstantiator, metrics);
    this.taskContext = taskContext;
    this.pluginRuntimeArgs = pluginRuntimeArgs;
    JobContext hadoopContext = (JobContext) taskContext.getHadoopContext();
    Configuration hConf = hadoopContext.getConfiguration();
    this.mapOutputKeyClassName = hConf.get(ETLMapReduce.GROUP_KEY_CLASS);
    this.mapOutputValClassName = hConf.get(ETLMapReduce.GROUP_VAL_CLASS);
    this.isMapper = hadoopContext instanceof Mapper.Context;
  }

  @Override
  protected BatchRuntimeContext createRuntimeContext(String stageName) {
    Map<String, String> stageRuntimeArgs = pluginRuntimeArgs.get(stageName);
    if (stageRuntimeArgs == null) {
      stageRuntimeArgs = new HashMap<>();
    }
    return new MapReduceRuntimeContext(taskContext, metrics, new DatasetContextLookupProvider(taskContext),
                                       stageName, stageRuntimeArgs);
  }

  protected BatchJoinerRuntimeContext createJoinerRuntimeContext(String stageName) {
    Map<String, String> stageRuntimeArgs = pluginRuntimeArgs.get(stageName);
    if (stageRuntimeArgs == null) {
      stageRuntimeArgs = new HashMap<>();
    }
    return new MapReduceBatchJoinerRuntimeContext(taskContext, metrics, new DatasetContextLookupProvider(taskContext),
                                                  stageName, stageRuntimeArgs, perStageInputSchemas.get(stageName));
  }

  @SuppressWarnings("unchecked")
  @Override
  protected TrackedTransform getTransformation(String pluginType, String stageName) throws Exception {
    if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {
      BatchAggregator<?, ?, ?> batchAggregator = pluginInstantiator.newPluginInstance(stageName);
      BatchRuntimeContext runtimeContext = createRuntimeContext(stageName);
      batchAggregator.initialize(runtimeContext);
      StageMetrics stageMetrics = new DefaultStageMetrics(metrics, stageName);
      if (isMapper) {
        return getTrackedGroupStep(new MapperAggregatorTransformation(batchAggregator,
                                                                      mapOutputKeyClassName,
                                                                      mapOutputValClassName),
                                   stageMetrics);
      } else {
        return getTrackedAggregateStep(new ReducerAggregatorTransformation(batchAggregator,
                                                                           mapOutputKeyClassName,
                                                                           mapOutputValClassName),
                                       stageMetrics);
      }
    } else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {
      BatchJoiner<?, ?, ?> batchJoiner = pluginInstantiator.newPluginInstance(stageName);
      BatchJoinerRuntimeContext runtimeContext = createJoinerRuntimeContext(stageName);
      batchJoiner.initialize(runtimeContext);
      StageMetrics stageMetrics = new DefaultStageMetrics(metrics, stageName);
      if (isMapper) {
        return getTrackedJoinOnStep(new MapperJoinerTransformation(batchJoiner, mapOutputKeyClassName), stageMetrics);
      } else {
        return getTrackedMergeStep(new ReducerJoinerTransformation(batchJoiner, mapOutputKeyClassName), stageMetrics);
      }
    }
    return super.getTransformation(pluginType, stageName);
  }

  /**
   * A Transformation that uses an join's joinOn method. Converts join value to tagged map output with stage name for
   * reducer. It uses {@link MapTaggedOutputWritable} to tag join value with stage name so that we can use this
   * in mapreduce.
   *
   * @param <JOIN_KEY> type of join key
   * @param <JOIN_VALUE> type of the join value
   * @param <OUT> type of the output of joiner
   * @param <OUT_KEY> type of the map output
   */
  private static class MapperJoinerTransformation<JOIN_KEY, JOIN_VALUE, OUT, OUT_KEY extends Writable>
    implements Transformation<JOIN_VALUE, KeyValue<OUT_KEY, MapTaggedOutputWritable<JOIN_VALUE>>> {
    private final Joiner<JOIN_KEY, JOIN_VALUE, OUT> joiner;
    private final WritableConversion<JOIN_KEY, OUT_KEY> keyConversion;

    MapperJoinerTransformation(Joiner<JOIN_KEY, JOIN_VALUE, OUT> joiner, String joinKeyClassName) {
      this.joiner = joiner;
      WritableConversion<JOIN_KEY, OUT_KEY> keyConversion = WritableConversions.getConversion(joinKeyClassName);
      this.keyConversion = keyConversion == null ? new CastConversion<JOIN_KEY, OUT_KEY>() : keyConversion;
    }

    @Override
    public void transform(JOIN_VALUE input, Emitter<KeyValue<OUT_KEY, MapTaggedOutputWritable<JOIN_VALUE>>> emitter)
      throws Exception {
      String stageName;
      // TODO Use input to get stageName
      if (emitter instanceof TransformDetail) {
        stageName = ((TransformDetail) emitter).getPrevStage();
      } else {
        stageName = ((TransformDetail) ((TrackedEmitter) emitter).getEmitter()).getPrevStage();
      }
      JOIN_KEY key = joiner.joinOn(stageName, input);
      MapTaggedOutputWritable<JOIN_VALUE> output = new MapTaggedOutputWritable(stageName, input);
      emitter.emit(new KeyValue<>(keyConversion.toWritable(key), output));
    }
  }

  /**
   * A Transformation that uses an join's merge method based on type of the join.
   *
   * @param <JOIN_KEY> type of join key
   * @param <JOIN_VALUE> type of the join value
   * @param <OUT> type of the output of joiner
   * @param <REDUCE_KEY> type of the reduce key=
   */
  private static class ReducerJoinerTransformation<JOIN_KEY, JOIN_VALUE, OUT, REDUCE_KEY extends WritableComparable>
    implements Transformation<KeyValue<REDUCE_KEY, Iterator<MapTaggedOutputWritable<JOIN_VALUE>>>, OUT> {
    private final Joiner<JOIN_KEY, JOIN_VALUE, OUT> joiner;
    private final WritableConversion<JOIN_KEY, REDUCE_KEY> keyConversion;

    ReducerJoinerTransformation(Joiner<JOIN_KEY, JOIN_VALUE, OUT> joiner, String joinKeyClassName) {
      this.joiner = joiner;
      WritableConversion<JOIN_KEY, REDUCE_KEY> keyConversion = WritableConversions.getConversion(joinKeyClassName);
      this.keyConversion = keyConversion == null ? new CastConversion<JOIN_KEY, REDUCE_KEY>() : keyConversion;
    }

    @Override
    public void transform(KeyValue<REDUCE_KEY, Iterator<MapTaggedOutputWritable<JOIN_VALUE>>> input,
                          Emitter<OUT> emitter) throws Exception {
      JOIN_KEY joinKey = keyConversion.fromWritable(input.getKey());
      JoinConfig joinConfig = joiner.getJoinConfig();
      String joinType = joinConfig.getJoinType();

      switch (joinType) {
        case "innerjoin":
          new InnerJoin<>(joiner, joinKey, input.getValue(), emitter).join();
          break;
        case "outerjoin":
          new OuterJoin<>(joiner, joinKey, input.getValue(), emitter).join();
          break;
        default:
      }
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
   * @param <OUT_KEY> type of output key for mapreduce. Must implement WritableComparable
   * @param <OUT_VAL> type of output value for mapreduce. Must implement Writable
   */
  private static class MapperAggregatorTransformation<GROUP_KEY, GROUP_VAL, OUT_KEY extends Writable,
    OUT_VAL extends Writable> implements Transformation<GROUP_VAL, KeyValue<OUT_KEY, OUT_VAL>> {
    private final Aggregator<GROUP_KEY, GROUP_VAL, ?> aggregator;
    private final DefaultEmitter<GROUP_KEY> groupKeyEmitter;
    private final WritableConversion<GROUP_KEY, OUT_KEY> keyConversion;
    private final WritableConversion<GROUP_VAL, OUT_VAL> valConversion;

    MapperAggregatorTransformation(Aggregator<GROUP_KEY, GROUP_VAL, ?> aggregator,
                                          String groupKeyClassName,
                                          String groupValClassName) {
      this.aggregator = aggregator;
      this.groupKeyEmitter = new DefaultEmitter<>();
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
   * @param <GROUP_KEY> type of group key output by the aggregator
   * @param <GROUP_VAL> type of group value used by the aggregator
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
  private static class CastConversion<T, W extends Writable> extends WritableConversion<T, W> {

    @Override
    public W toWritable(T val) {
      return (W) val;
    }

    @Override
    public T fromWritable(W val) {
      return (T) val;
    }

    // never used
    @Override
    public W toWritable() {
      throw new UnsupportedOperationException("toWritable() not supported");
    }
  }
}
