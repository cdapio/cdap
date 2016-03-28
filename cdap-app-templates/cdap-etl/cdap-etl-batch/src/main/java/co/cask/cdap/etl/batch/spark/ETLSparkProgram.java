/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.spark;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.batch.PipelinePluginInstantiator;
import co.cask.cdap.etl.batch.TransformExecutorFactory;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.common.TransformExecutor;
import co.cask.cdap.etl.common.TransformResponse;
import co.cask.cdap.etl.planner.StageInfo;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Spark program to run an ETL pipeline.
 */
public class ETLSparkProgram implements JavaSparkProgram {

  private static final Logger LOG = LoggerFactory.getLogger(ETLSparkProgram.class);

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>()).create();


  @Override
  public void run(SparkContext context) throws Exception {
    BatchPhaseSpec phaseSpec = GSON.fromJson(context.getSpecification().getProperty(Constants.PIPELINEID),
                                             BatchPhaseSpec.class);
    Set<StageInfo> aggregators = phaseSpec.getPhase().getStagesOfType(BatchAggregator.PLUGIN_TYPE);
    String aggregatorName = null;
    if (!aggregators.isEmpty()) {
      aggregatorName = aggregators.iterator().next().getName();
    }

    SparkBatchSourceFactory sourceFactory;
    SparkBatchSinkFactory sinkFactory;
    Integer numPartitions;
    try (InputStream is = new FileInputStream(context.getTaskLocalizationContext().getLocalFile("ETLSpark.config"))) {
      sourceFactory = SparkBatchSourceFactory.deserialize(is);
      sinkFactory = SparkBatchSinkFactory.deserialize(is);
      numPartitions = new DataInputStream(is).readInt();
    }

    JavaPairRDD<Object, Object> rdd = sourceFactory.createRDD(context, Object.class, Object.class);
    JavaPairRDD<String, Object> resultRDD;
    if (aggregatorName != null) {
      JavaPairRDD<Object, Object> preGroupRDD = rdd.flatMapToPair(new PreGroupFunction(context, aggregatorName));
      JavaPairRDD<Object, Iterable<Object>> groupedRDD =
        numPartitions < 0 ? preGroupRDD.groupByKey() : preGroupRDD.groupByKey(numPartitions);
      resultRDD = groupedRDD.flatMapToPair(new MapFunction<Iterable<Object>>(context, aggregatorName)).cache();
    } else {
      resultRDD = rdd.flatMapToPair(new MapFunction<>(context, null)).cache();
    }

    Set<StageInfo> stagesOfTypeMLLib = phaseSpec.getPhase().getStagesOfType(SparkSink.PLUGIN_TYPE);
    Set<String> namesOfTypeMLLib = new HashSet<>();

    for (StageInfo stageInfo : stagesOfTypeMLLib) {
      namesOfTypeMLLib.add(stageInfo.getName());
    }

    for (final String sinkName : phaseSpec.getPhase().getSinks()) {

      JavaPairRDD<String, Object> filteredResultRDD = resultRDD.filter(new Function<Tuple2<String, Object>, Boolean>() {
        @Override
        public Boolean call(Tuple2<String, Object> v1) throws Exception {
          return v1._1().equals(sinkName);
        }
      });

      if (namesOfTypeMLLib.contains(sinkName)) {
        JavaRDD<Object> values = filteredResultRDD.map(new Function<Tuple2<String, Object>, Object>() {
          @Override
          public Object call(Tuple2<String, Object> input) throws Exception {
            return input._2();
          }
        });

        SparkSink sparkSink = context.getPluginContext().newPluginInstance(sinkName);
        BasicSparkPluginContext sparkPluginContext = new BasicSparkPluginContext(context, null, sinkName);
        sparkSink.run(sparkPluginContext, values);
      } else {

        JavaPairRDD<Object, Object> sinkRDD =
          filteredResultRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Object>, Object, Object>() {
            @Override
            public Iterable<Tuple2<Object, Object>> call(Tuple2<String, Object> input) throws Exception {
              List<Tuple2<Object, Object>> result = new ArrayList<>();
              KeyValue<Object, Object> keyValue = (KeyValue<Object, Object>) input._2();
              result.add(new Tuple2<>(keyValue.getKey(), keyValue.getValue()));
              return result;
            }
          });
        sinkFactory.writeFromRDD(sinkRDD, context, sinkName, Object.class, Object.class);
      }
    }
  }


  /**
   * Base function that knows how to set up a transform executor and run it.
   * Subclasses are responsible for massaging the output of the transform executor into the expected output,
   * and for configuring the transform executor with the right part of the pipeline.
   */
  public abstract static class TransformExecutorFunction<KEY_IN, VAL_IN, KEY_OUT, VAL_OUT>
    implements PairFlatMapFunction<Tuple2<KEY_IN, VAL_IN>, KEY_OUT, VAL_OUT> {

    protected final PluginContext pluginContext;
    protected final Metrics metrics;
    protected final long logicalStartTime;
    protected final Map<String, String> runtimeArgs;
    protected final String pipelineStr;
    private transient TransformExecutor<KeyValue<KEY_IN, VAL_IN>> transformExecutor;

    public TransformExecutorFunction(SparkContext sparkContext) {
      this.pluginContext = sparkContext.getPluginContext();
      this.metrics = sparkContext.getMetrics();
      this.logicalStartTime = sparkContext.getLogicalStartTime();
      this.runtimeArgs = sparkContext.getRuntimeArguments();
      this.pipelineStr = sparkContext.getSpecification().getProperty(Constants.PIPELINEID);
    }

    @Override
    public Iterable<Tuple2<KEY_OUT, VAL_OUT>> call(Tuple2<KEY_IN, VAL_IN> tuple) throws Exception {
      if (transformExecutor == null) {
        // TODO: There is no way to call destroy() method on Transform
        // In fact, we can structure transform in a way that it doesn't need destroy
        // All current usage of destroy() in transform is actually for Source/Sink, which is actually
        // better do it in prepareRun and onRunFinish, which happen outside of the Job execution (true for both
        // Spark and MapReduce).
        BatchPhaseSpec phaseSpec = GSON.fromJson(pipelineStr, BatchPhaseSpec.class);
        PipelinePluginInstantiator pluginInstantiator = new PipelinePluginInstantiator(pluginContext, phaseSpec);
        transformExecutor = initialize(phaseSpec, pluginInstantiator);
      }
      TransformResponse response = transformExecutor.runOneIteration(new KeyValue<>(tuple._1(), tuple._2()));
      Iterable<Tuple2<KEY_OUT, VAL_OUT>> output = getOutput(response);
      transformExecutor.resetEmitter();
      return output;
    }

    protected abstract Iterable<Tuple2<KEY_OUT, VAL_OUT>> getOutput(TransformResponse transformResponse);

    protected abstract TransformExecutor<KeyValue<KEY_IN, VAL_IN>> initialize(
      BatchPhaseSpec phaseSpec, PipelinePluginInstantiator pluginInstantiator) throws Exception;
  }

  /**
   * Performs all transforms before an aggregator plugin. Outputs tuples whose keys are the group key and values
   * are the group values that result by calling the aggregator's groupBy method.
   */
  public static final class PreGroupFunction extends TransformExecutorFunction<Object, Object, Object, Object> {
    private final String aggregatorName;

    public PreGroupFunction(SparkContext sparkContext, @Nullable String aggregatorName) {
      super(sparkContext);
      this.aggregatorName = aggregatorName;
    }

    @Override
    protected Iterable<Tuple2<Object, Object>> getOutput(TransformResponse transformResponse) {
      List<Tuple2<Object, Object>> result = new ArrayList<>();
      for (Map.Entry<String, Collection<Object>> transformedEntry : transformResponse.getSinksResults().entrySet()) {
        for (Object output : transformedEntry.getValue()) {
          result.add((Tuple2<Object, Object>) output);
        }
      }
      return result;
    }

    @Override
    protected TransformExecutor<KeyValue<Object, Object>> initialize(BatchPhaseSpec phaseSpec,
                                                                     PipelinePluginInstantiator pluginInstantiator)
      throws Exception {

      TransformExecutorFactory<KeyValue<Object, Object>> transformExecutorFactory =
        new SparkTransformExecutorFactory<>(pluginContext, pluginInstantiator, metrics,
                                            logicalStartTime, runtimeArgs, true);
      PipelinePhase pipelinePhase = phaseSpec.getPhase().subsetTo(ImmutableSet.of(aggregatorName));
      return transformExecutorFactory.create(pipelinePhase);
    }
  }

  /**
   * Performs all transforms that happen after an aggregator, or if there is no aggregator at all.
   * Outputs tuples whose first item is the name of the sink that is being written to, and second item is
   * the key-value that should be written to that sink
   */
  public static final class MapFunction<T> extends TransformExecutorFunction<Object, T, String, Object> {
    @Nullable
    private final String aggregatorName;

    public MapFunction(SparkContext sparkContext, @Nullable String aggregatorName) {
      super(sparkContext);
      this.aggregatorName = aggregatorName;
    }

    @Override
    protected Iterable<Tuple2<String, Object>> getOutput(TransformResponse transformResponse) {
      List<Tuple2<String, Object>> result = new ArrayList<>();
      for (Map.Entry<String, Collection<Object>> transformedEntry : transformResponse.getSinksResults().entrySet()) {
        String sinkName = transformedEntry.getKey();
        for (Object outputRecord : transformedEntry.getValue()) {
          result.add(new Tuple2<>(sinkName, outputRecord));
        }
      }
      return result;
    }

    @Override
    protected TransformExecutor<KeyValue<Object, T>> initialize(BatchPhaseSpec phaseSpec,
                                                                PipelinePluginInstantiator pluginInstantiator)
      throws Exception {
      TransformExecutorFactory<KeyValue<Object, T>> transformExecutorFactory =
        new SparkTransformExecutorFactory<>(pluginContext, pluginInstantiator, metrics,
                                            logicalStartTime, runtimeArgs, false);

      PipelinePhase pipelinePhase = phaseSpec.getPhase();
      if (aggregatorName != null) {
        pipelinePhase = pipelinePhase.subsetFrom(ImmutableSet.of(aggregatorName));
      }

      return transformExecutorFactory.create(pipelinePhase);
    }
  }
}
