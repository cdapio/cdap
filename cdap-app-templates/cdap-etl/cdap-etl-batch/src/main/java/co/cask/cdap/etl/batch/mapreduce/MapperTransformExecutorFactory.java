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
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.batch.TransformExecutorFactory;
import co.cask.cdap.etl.batch.conversion.IdentityConversion;
import co.cask.cdap.etl.batch.conversion.WritableConversion;
import co.cask.cdap.etl.batch.conversion.WritableConversions;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.common.DefaultEmitter;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Creates transform executors for mapreduce programs.
 *
 * @param <T> the type of input for the created transform executors
 */
public class MapperTransformExecutorFactory<T> extends TransformExecutorFactory<T> {
  private final Map<String, Map<String, String>> pluginRuntimeArgs;
  private final MapReduceTaskContext taskContext;
  private final String mapOutputKeyClassName;
  private final String mapOutputValClassName;

  public MapperTransformExecutorFactory(MapReduceTaskContext taskContext,
                                        PipelinePluginInstantiator pluginInstantiator,
                                        Metrics metrics,
                                        Map<String, Map<String, String>> pluginRuntimeArgs) {
    super(pluginInstantiator, metrics);
    this.taskContext = taskContext;
    this.pluginRuntimeArgs = pluginRuntimeArgs;
    Mapper.Context hadoopContext = (Mapper.Context) taskContext.getHadoopContext();
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
      return new AggregatorTransformation(batchAggregator,
                                          new DefaultStageMetrics(metrics, stageName),
                                          mapOutputKeyClassName,
                                          mapOutputValClassName);
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
   * @param <OUT_KEY> type of output key for mapreduce. Must implement WritableComparable
   * @param <OUT_VAL> type of output value for mapreduce. Must implement Writable
   */
  private static class AggregatorTransformation<GROUP_KEY, GROUP_VAL, OUT_KEY extends Writable,
    OUT_VAL extends Writable> implements Transformation<GROUP_VAL, KeyValue<OUT_KEY, OUT_VAL>> {
    private final Aggregator<GROUP_KEY, GROUP_VAL, ?> aggregator;
    private final StageMetrics stageMetrics;
    private final WritableConversion<GROUP_KEY, OUT_KEY> keyConversion;
    private final WritableConversion<GROUP_VAL, OUT_VAL> valConversion;

    public AggregatorTransformation(Aggregator<GROUP_KEY, GROUP_VAL, ?> aggregator,
                                    StageMetrics stageMetrics,
                                    String groupKeyClassName,
                                    String groupValClassName) {
      this.aggregator = aggregator;
      this.stageMetrics = stageMetrics;
      WritableConversion<GROUP_KEY, OUT_KEY> keyConversion = WritableConversions.getConversion(groupKeyClassName);
      WritableConversion<GROUP_VAL, OUT_VAL> valConversion = WritableConversions.getConversion(groupValClassName);
      this.keyConversion = keyConversion == null ? new IdentityConversion<GROUP_KEY, OUT_KEY>() : keyConversion;
      this.valConversion = valConversion == null ? new IdentityConversion<GROUP_VAL, OUT_VAL>() : valConversion;
    }

    @Override
    public void transform(GROUP_VAL input, Emitter<KeyValue<OUT_KEY, OUT_VAL>> emitter) throws Exception {
      DefaultEmitter<GROUP_KEY> groupKeyEmitter = new DefaultEmitter<>(stageMetrics);
      aggregator.groupBy(input, groupKeyEmitter);
      for (GROUP_KEY groupKey : groupKeyEmitter.getEntries()) {
        emitter.emit(new KeyValue<>(keyConversion.toWritable(groupKey), valConversion.toWritable(input)));
      }
    }
  }

}
