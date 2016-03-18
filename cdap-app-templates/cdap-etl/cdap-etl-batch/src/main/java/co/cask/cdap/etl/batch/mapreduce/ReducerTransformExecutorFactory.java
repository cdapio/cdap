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
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.batch.TransformExecutorFactory;
import co.cask.cdap.etl.batch.conversion.WritableConversion;
import co.cask.cdap.etl.batch.conversion.WritableConversions;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Creates transform executors for mapreduce programs.
 *
 * @param <T> the type of input for the created transform executors
 */
public class ReducerTransformExecutorFactory<T> extends TransformExecutorFactory<T> {
  private final Map<String, Map<String, String>> pluginRuntimeArgs;
  private final MapReduceTaskContext taskContext;
  private final String mapOutputKeyClassName;
  private final String mapOutputValClassName;

  public ReducerTransformExecutorFactory(MapReduceTaskContext taskContext,
                                         PipelinePluginInstantiator pluginInstantiator,
                                         Metrics metrics,
                                         Map<String, Map<String, String>> pluginRuntimeArgs) {
    super(pluginInstantiator, metrics);
    this.taskContext = taskContext;
    this.pluginRuntimeArgs = pluginRuntimeArgs;
    Reducer.Context hadoopContext = (Reducer.Context) taskContext.getHadoopContext();
    Configuration hConf = hadoopContext.getConfiguration();
    this.mapOutputKeyClassName = hConf.get(ETLMapReduce.GROUP_KEY_CLASS);
    this.mapOutputValClassName = hConf.get(ETLMapReduce.GROUP_VAL_CLASS);
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

  @SuppressWarnings("unchecked")
  @Override
  protected Transformation getTransformation(String pluginType, String stageName) throws Exception {
    if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {
      BatchAggregator<?, ?, ?> batchAggregator = pluginInstantiator.newPluginInstance(stageName);
      BatchRuntimeContext runtimeContext = createRuntimeContext(stageName);
      batchAggregator.initialize(runtimeContext);

      return new AggregatorTransformation(batchAggregator, mapOutputKeyClassName, mapOutputValClassName);
    }
    return super.getTransformation(pluginType, stageName);
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
   * @param <REDUCE_KEY> type of reduce key for mapreduce. Must implement WritableComparable
   * @param <REDUCE_VAL> type of reduce value for mapreduce. Must implement Writable
   */
  private static class AggregatorTransformation<GROUP_KEY, GROUP_VAL, OUT,
    REDUCE_KEY extends WritableComparable, REDUCE_VAL extends Writable>
    implements Transformation<KeyValue<REDUCE_KEY, Iterator<REDUCE_VAL>>, OUT> {
    private final Aggregator<GROUP_KEY, GROUP_VAL, OUT> aggregator;
    private final Function<REDUCE_KEY, GROUP_KEY> keyFunction;
    private final Function<REDUCE_VAL, GROUP_VAL> valFunction;

    public AggregatorTransformation(Aggregator<GROUP_KEY, GROUP_VAL, OUT> aggregator,
                                    String groupKeyClassName,
                                    String groupValClassName) {
      this.aggregator = aggregator;
      WritableConversion<GROUP_KEY, REDUCE_KEY> keyConversion = WritableConversions.getConversion(groupKeyClassName);
      WritableConversion<GROUP_VAL, REDUCE_VAL> valConversion = WritableConversions.getConversion(groupValClassName);
      if (keyConversion == null) {
        this.keyFunction = getIdentity();
      } else {
        this.keyFunction = keyConversion.getFromWritableFunction();
      }
      if (valConversion == null) {
        this.valFunction = getIdentity();
      } else {
        this.valFunction = valConversion.getFromWritableFunction();
      }
    }

    private <K, V> Function<K, V> getIdentity() {
      return new Function<K, V>() {
        @Nullable
        @Override
        public V apply(@Nullable K input) {
          //noinspection unchecked
          return (V) input;
        }
      };
    }

    @Override
    public void transform(KeyValue<REDUCE_KEY, Iterator<REDUCE_VAL>> input, Emitter<OUT> emitter) throws Exception {
      GROUP_KEY groupKey = keyFunction.apply(input.getKey());
      Iterator<GROUP_VAL> iter = Iterators.transform(input.getValue(), valFunction);
      aggregator.aggregate(groupKey, iter, emitter);
    }
  }
}
