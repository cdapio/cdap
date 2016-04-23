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

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.SparkCompute;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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
public class ETLSparkProgram implements JavaSparkMain, TxRunnable {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>()).create();

  private transient JavaSparkContext jsc;
  private transient JavaSparkExecutionContext sec;

  @Override
  public void run(final JavaSparkExecutionContext sec) throws Exception {
    this.jsc = new JavaSparkContext();
    this.sec = sec;

    // Execution the whole pipeline in one long transaction. This is because the Spark execution
    // currently share the same contract and API as the MapReduce one.
    // The API need to expose DatasetContext, hence it needs to be exeucted inside a transaction
    sec.execute(this);
  }

  @Override
  public void run(DatasetContext datasetContext) throws Exception {

    BatchPhaseSpec phaseSpec = GSON.fromJson(sec.getSpecification().getProperty(Constants.PIPELINEID),
                                             BatchPhaseSpec.class);
    Set<StageInfo> aggregators = phaseSpec.getPhase().getStagesOfType(BatchAggregator.PLUGIN_TYPE);
    String aggregatorName = null;
    if (!aggregators.isEmpty()) {
      aggregatorName = aggregators.iterator().next().getName();
    }

    SparkBatchSourceFactory sourceFactory;
    SparkBatchSinkFactory sinkFactory;
    Integer numPartitions;
    try (InputStream is = new FileInputStream(sec.getLocalizationContext().getLocalFile("ETLSpark.config"))) {
      sourceFactory = SparkBatchSourceFactory.deserialize(is);
      sinkFactory = SparkBatchSinkFactory.deserialize(is);
      numPartitions = new DataInputStream(is).readInt();
    }

    JavaPairRDD<Object, Object> rdd = sourceFactory.createRDD(sec, jsc, Object.class, Object.class);
    JavaPairRDD<String, Object> resultRDD = doTransform(sec, jsc, datasetContext, phaseSpec, rdd,
                                                        aggregatorName, numPartitions);

    Set<StageInfo> stagesOfTypeSparkSink = phaseSpec.getPhase().getStagesOfType(SparkSink.PLUGIN_TYPE);
    Set<String> namesOfTypeSparkSink = new HashSet<>();

    for (StageInfo stageInfo : stagesOfTypeSparkSink) {
      namesOfTypeSparkSink.add(stageInfo.getName());
    }

    for (final String sinkName : phaseSpec.getPhase().getSinks()) {

      JavaPairRDD<String, Object> filteredResultRDD = resultRDD.filter(
        new Function<Tuple2<String, Object>, Boolean>() {
          @Override
          public Boolean call(Tuple2<String, Object> v1) throws Exception {
            return v1._1().equals(sinkName);
          }
        });

      if (namesOfTypeSparkSink.contains(sinkName)) {
        SparkSink sparkSink = sec.getPluginContext().newPluginInstance(sinkName);
        sparkSink.run(new BasicSparkExecutionPluginContext(sec, jsc, datasetContext, sinkName),
                      filteredResultRDD.values());
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
        sinkFactory.writeFromRDD(sinkRDD, sec, sinkName, Object.class, Object.class);
      }
    }
  }

  private JavaPairRDD<String, Object> doTransform(JavaSparkExecutionContext sec, JavaSparkContext jsc,
                                                  DatasetContext datasetContext, BatchPhaseSpec phaseSpec,
                                                  JavaPairRDD<Object, Object> input,
                                                  String aggregatorName, int numPartitions) throws Exception {

    Set<StageInfo> sparkComputes = phaseSpec.getPhase().getStagesOfType(SparkCompute.PLUGIN_TYPE);
    if (sparkComputes.isEmpty()) {
      // if this is not a phase with SparkCompute, do regular transform logic
      if (aggregatorName != null) {
        JavaPairRDD<Object, Object> preGroupRDD =
          input.flatMapToPair(new PreGroupFunction(sec, aggregatorName));
        JavaPairRDD<Object, Iterable<Object>> groupedRDD =
          numPartitions < 0 ? preGroupRDD.groupByKey() : preGroupRDD.groupByKey(numPartitions);
        return groupedRDD.flatMapToPair(new MapFunction<Iterable<Object>>(sec, null, aggregatorName, false)).cache();
      } else {
        return input.flatMapToPair(new MapFunction<>(sec, null, null, false)).cache();
      }
    }

    // otherwise, special casing for SparkCompute type:

    // there should only be no other plugins of type Transform, because of how Smart Workflow breaks up the phases
    Set<StageInfo> stagesOfTypeTransform = phaseSpec.getPhase().getStagesOfType(Transform.PLUGIN_TYPE);
    Preconditions.checkArgument(stagesOfTypeTransform.isEmpty(),
                                "Found non-empty set of transform plugins when expecting none: %s",
                                stagesOfTypeTransform);

    // Smart Workflow should guarantee that only 1 SparkCompute exists per phase. This can be improved in the future
    // for efficiency.
    Preconditions.checkArgument(sparkComputes.size() == 1, "Expected only 1 SparkCompute: %s", sparkComputes);

    String sparkComputeName = Iterables.getOnlyElement(sparkComputes).getName();


    Set<String> sourceStages = phaseSpec.getPhase().getSources();
    Preconditions.checkArgument(sourceStages.size() == 1, "Expected only 1 source stage: %s", sourceStages);

    String sourceStageName = Iterables.getOnlyElement(sourceStages);

    Set<String> sourceNextStages = phaseSpec.getPhase().getStageOutputs(sourceStageName);
    Preconditions.checkArgument(sourceNextStages.size() == 1,
                                "Expected only 1 stage after source stage: %s", sourceNextStages);

    Preconditions.checkArgument(sparkComputeName.equals(Iterables.getOnlyElement(sourceNextStages)),
                                "Expected the single stage after the source stage to be the spark compute: %s",
                                sparkComputeName);


    // phase starting from source to SparkCompute
    PipelinePhase sourcePhase = phaseSpec.getPhase().subsetTo(ImmutableSet.of(sparkComputeName));
    String sourcePipelineStr =
      GSON.toJson(new BatchPhaseSpec(phaseSpec.getPhaseName(), sourcePhase, phaseSpec.getResources(),
                                     phaseSpec.isStageLoggingEnabled(), phaseSpec.getConnectorDatasets()));

    JavaPairRDD<String, Object> sourceTransformed =
      input.flatMapToPair(new MapFunction<>(sec, sourcePipelineStr, null, true)).cache();

    SparkCompute sparkCompute =
      new PipelinePluginInstantiator(sec.getPluginContext(), phaseSpec).newPluginInstance(sparkComputeName);
    JavaRDD<Object> sparkComputed =
      sparkCompute.transform(new BasicSparkExecutionPluginContext(sec, jsc, datasetContext, sparkComputeName),
                             sourceTransformed.values());

    // phase starting from SparkCompute to sink(s)
    PipelinePhase sinkPhase = phaseSpec.getPhase().subsetFrom(ImmutableSet.of(sparkComputeName));
    String sinkPipelineStr =
      GSON.toJson(new BatchPhaseSpec(phaseSpec.getPhaseName(), sinkPhase, phaseSpec.getResources(),
                                     phaseSpec.isStageLoggingEnabled(), phaseSpec.getConnectorDatasets()));

    JavaPairRDD<String, Object> sinkTransformedValues =
      sparkComputed.flatMapToPair(new SingleTypeRDDMapFunction(sec, sinkPipelineStr)).cache();
    return sinkTransformedValues;
  }

  /**
   * Base function that knows how to set up a transform executor and run it.
   * Subclasses are responsible for massaging the output of the transform executor into the expected output,
   * and for configuring the transform executor with the right part of the pipeline.
   *
   * @param <IN> type of the input
   * @param <EXECUTOR_IN> type of the executor input
   * @param <KEY_OUT> type of the output key
   * @param <VAL_OUT> type of the output value
   */
  public abstract static class TransformExecutorFunction<IN, EXECUTOR_IN, KEY_OUT, VAL_OUT>
    implements PairFlatMapFunction<IN, KEY_OUT, VAL_OUT> {

    protected final PluginContext pluginContext;
    protected final Metrics metrics;
    protected final long logicalStartTime;
    protected final Map<String, String> runtimeArgs;
    protected final String pipelineStr;
    private transient TransformExecutor<EXECUTOR_IN> transformExecutor;

    public TransformExecutorFunction(JavaSparkExecutionContext sec, @Nullable String pipelineStr) {
      this.pluginContext = sec.getPluginContext();
      this.metrics = sec.getMetrics();
      this.logicalStartTime = sec.getLogicalStartTime();
      this.runtimeArgs = sec.getRuntimeArguments();
      this.pipelineStr = pipelineStr != null ?
        pipelineStr : sec.getSpecification().getProperty(Constants.PIPELINEID);
    }

    @Override
    public Iterable<Tuple2<KEY_OUT, VAL_OUT>> call(IN input) throws Exception {
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
      TransformResponse response = transformExecutor.runOneIteration(computeInputForExecutor(input));
      Iterable<Tuple2<KEY_OUT, VAL_OUT>> output = getOutput(response);
      transformExecutor.resetEmitter();
      return output;
    }

    protected abstract Iterable<Tuple2<KEY_OUT, VAL_OUT>> getOutput(TransformResponse transformResponse);

    protected abstract TransformExecutor<EXECUTOR_IN> initialize(
      BatchPhaseSpec phaseSpec, PipelinePluginInstantiator pluginInstantiator) throws Exception;

    protected abstract EXECUTOR_IN computeInputForExecutor(IN input);
  }

  /**
   * Performs all transforms before an aggregator plugin. Outputs tuples whose keys are the group key and values
   * are the group values that result by calling the aggregator's groupBy method.
   */
  public static final class PreGroupFunction
    extends TransformExecutorFunction<Tuple2<Object, Object>, KeyValue<Object, Object>, Object, Object> {
    private final String aggregatorName;

    public PreGroupFunction(JavaSparkExecutionContext sec, @Nullable String aggregatorName) {
      super(sec, null);
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

    @Override
    protected KeyValue<Object, Object> computeInputForExecutor(Tuple2<Object, Object> input) {
      return new KeyValue<>(input._1(), input._2());
    }
  }

  /**
   * Performs all transforms that happen after an aggregator, or if there is no aggregator at all.
   * Outputs tuples whose first item is the name of the sink that is being written to, and second item is
   * the key-value that should be written to that sink
   *
   * @param <T> type of the map output value
   */
  public static final class MapFunction<T> extends SingleTypeRDDMapFunction<Tuple2<Object, T>, KeyValue<Object, T>> {

    @Nullable
    private final String aggregatorName;
    private final boolean isBeforeBreak;

    public MapFunction(JavaSparkExecutionContext sec, String pipelineStr, String aggregatorName,
                       boolean isBeforeBreak) {
      super(sec, pipelineStr);
      this.aggregatorName = aggregatorName;
      this.isBeforeBreak = isBeforeBreak;
    }

    @Override
    protected TransformExecutor<KeyValue<Object, T>> initialize(BatchPhaseSpec phaseSpec,
                                                                PipelinePluginInstantiator pluginInstantiator)
      throws Exception {
      TransformExecutorFactory<KeyValue<Object, T>> transformExecutorFactory =
        new SparkTransformExecutorFactory<>(pluginContext, pluginInstantiator, metrics,
                                            logicalStartTime, runtimeArgs, isBeforeBreak);

      PipelinePhase pipelinePhase = phaseSpec.getPhase();
      if (aggregatorName != null) {
        pipelinePhase = pipelinePhase.subsetFrom(ImmutableSet.of(aggregatorName));
      }

      return transformExecutorFactory.create(pipelinePhase);
    }

    @Override
    protected KeyValue<Object, T> computeInputForExecutor(Tuple2<Object, T> input) {
      return new KeyValue<>(input._1(), input._2());
    }
  }

  /**
   * Used for the transform after a SparkCompute. Otherwise, MapFunction only operates on RDD of JavaPairRDD.
   * In other words, it does not handle translation from Tuple to KeyValue, but directly sends the RDD type
   * to the TransformExecutor.
   * This allows operations on JavaRDD of single type. Handles no aggregation functionality, because it should not
   * be used in a phase with aggregations.
   *
   * @param <IN> type of the input
   * @param <EXECUTOR_IN> type of the input to the executor
   */
  public static class SingleTypeRDDMapFunction<IN, EXECUTOR_IN>
    extends TransformExecutorFunction<IN, EXECUTOR_IN, String, Object> {

    public SingleTypeRDDMapFunction(JavaSparkExecutionContext sec, String pipelineStr) {
      super(sec, pipelineStr);
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
    protected TransformExecutor<EXECUTOR_IN> initialize(BatchPhaseSpec phaseSpec,
                                                        PipelinePluginInstantiator pluginInstantiator)
      throws Exception {

      TransformExecutorFactory<EXECUTOR_IN> transformExecutorFactory =
        new SparkTransformExecutorFactory<>(pluginContext, pluginInstantiator, metrics,
                                            logicalStartTime, runtimeArgs, false);

      PipelinePhase pipelinePhase = phaseSpec.getPhase();
      return transformExecutorFactory.create(pipelinePhase);
    }

    @Override
    protected EXECUTOR_IN computeInputForExecutor(IN input) {
      // by default, have IN same as EXECUTOR_IN
      return (EXECUTOR_IN) input;
    }
  }
}
