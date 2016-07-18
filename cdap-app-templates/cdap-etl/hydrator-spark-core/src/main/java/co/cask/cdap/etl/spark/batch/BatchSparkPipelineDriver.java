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

package co.cask.cdap.etl.spark.batch;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.etl.api.JoinElement;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultStageMetrics;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.spark.SparkPipelineDriver;
import co.cask.cdap.etl.spark.function.AggregatorAggregateFunction;
import co.cask.cdap.etl.spark.function.AggregatorGroupByFunction;
import co.cask.cdap.etl.spark.function.BatchSinkFunction;
import co.cask.cdap.etl.spark.function.BatchSourceFunction;
import co.cask.cdap.etl.spark.function.InitialJoinFunction;
import co.cask.cdap.etl.spark.function.JoinFlattenFunction;
import co.cask.cdap.etl.spark.function.JoinMergeFunction;
import co.cask.cdap.etl.spark.function.JoinOnFunction;
import co.cask.cdap.etl.spark.function.LeftJoinFlattenFunction;
import co.cask.cdap.etl.spark.function.OuterJoinFlattenFunction;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;
import co.cask.cdap.etl.spark.function.TransformFunction;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Batch Spark pipeline driver.
 */
public class BatchSparkPipelineDriver extends SparkPipelineDriver<JavaRDD<Object>>
  implements JavaSparkMain, TxRunnable {
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private transient JavaSparkContext jsc;
  private transient JavaSparkExecutionContext sec;
  private transient SparkBatchSourceFactory sourceFactory;
  private transient SparkBatchSinkFactory sinkFactory;
  private transient DatasetContext datasetContext;
  private transient Map<String, Integer> stagePartitions;

  @Override
  protected JavaRDD<Object> union(JavaRDD<Object> input1, JavaRDD<Object> input2) {
    return input1.union(input2);
  }

  @Override
  protected void cache(JavaRDD<Object> data) {
    data.cache();
  }

  @Override
  protected JavaRDD<Object> getSource(String stageName, PluginFunctionContext pluginFunctionContext) {
    return sourceFactory.createRDD(sec, jsc, stageName, Object.class, Object.class)
        .flatMap(new BatchSourceFunction(pluginFunctionContext));
  }

  @Override
  protected JavaRDD<Object> handleTransform(String stageName, JavaRDD<Object> inputData, TransformFunction function) {
    return inputData.flatMap(function);
  }

  @Override
  protected void handleBatchSink(String stageName, JavaRDD<Object> inputData,
                                 BatchSinkFunction sinkFunction) {
    JavaPairRDD<Object, Object> sinkRDD = inputData.flatMapToPair(sinkFunction);
    sinkFactory.writeFromRDD(sinkRDD, sec, stageName, Object.class, Object.class);
  }

  @Override
  protected JavaRDD<Object> handleSparkCompute(String stageName, JavaRDD<Object> inputData,
                                               SparkCompute<Object, Object> plugin) throws Exception {
    SparkExecutionPluginContext sparkPluginContext =
      new BasicSparkExecutionPluginContext(sec, jsc, datasetContext, stageName);

    // TODO: figure out how to do this in a better way...
    long recordsIn = inputData.cache().count();
    StageMetrics stageMetrics = new DefaultStageMetrics(sec.getMetrics(), stageName);
    stageMetrics.gauge("records.in", recordsIn);

    JavaRDD<Object> computedRDD = plugin.transform(sparkPluginContext, inputData).cache();
    long recordsOut = computedRDD.count();
    stageMetrics.gauge("records.out", recordsOut);
    return computedRDD;
  }

  @Override
  protected void handleSparkSink(String stageName, JavaRDD<Object> inputData,
                                 SparkSink<Object> plugin) throws Exception {
    SparkExecutionPluginContext sparkPluginContext =
      new BasicSparkExecutionPluginContext(sec, jsc, datasetContext, stageName);

    // TODO: figure out how to do this in a better way...
    long recordsIn = inputData.cache().count();
    StageMetrics stageMetrics = new DefaultStageMetrics(sec.getMetrics(), stageName);
    stageMetrics.gauge("records.in", recordsIn);

    plugin.run(sparkPluginContext, inputData);
  }

  @Override
  protected JavaRDD<Object> handleAggregator(String stageName, JavaRDD<Object> inputData,
                                             AggregatorGroupByFunction groupByFunction,
                                             AggregatorAggregateFunction aggregateFunction) {
    Integer partitions = stagePartitions.get(stageName);
    JavaPairRDD<Object, Object> keyedRDD = inputData.flatMapToPair(groupByFunction);
    JavaPairRDD<Object, Iterable<Object>> groupedRDD = partitions == null ?
      keyedRDD.groupByKey() : keyedRDD.groupByKey(partitions);
    return groupedRDD.flatMap(aggregateFunction);
  }

  @Override
  protected JavaRDD<Object> handleJoin(String stageName,
                                       final BatchJoiner<Object, Object, Object> joiner,
                                       Map<String, JavaRDD<Object>> inputData,
                                       PluginFunctionContext pluginFunctionContext) throws Exception {

    Map<String, JavaPairRDD<Object, Object>> preJoinRDDs = new HashMap<>();
    for (Map.Entry<String, JavaRDD<Object>> inputRDDEntry : inputData.entrySet()) {
      String inputStage = inputRDDEntry.getKey();
      JavaRDD<Object> inputRDD = inputRDDEntry.getValue();
      preJoinRDDs.put(inputStage,
                      inputRDD.flatMapToPair(new JoinOnFunction(pluginFunctionContext, inputStage)));
    }

    Set<String> remainingInputs = new HashSet<>();
    remainingInputs.addAll(inputData.keySet());

    Integer numPartitions = stagePartitions.get(stageName);

    JavaPairRDD<Object, List<JoinElement<Object>>> joinedInputs = null;
    // inner join on required inputs
    for (final String inputStageName : joiner.getJoinConfig().getRequiredInputs()) {
      JavaPairRDD<Object, Object> preJoinRDD = preJoinRDDs.get(inputStageName);

      if (joinedInputs == null) {
        joinedInputs = preJoinRDD.mapValues(new InitialJoinFunction(inputStageName));
      } else {
        JoinFlattenFunction joinFlattenFunction = new JoinFlattenFunction(inputStageName);
        joinedInputs = numPartitions == null ?
          joinedInputs.join(preJoinRDD).mapValues(joinFlattenFunction) :
          joinedInputs.join(preJoinRDD, numPartitions).mapValues(joinFlattenFunction);
      }
      remainingInputs.remove(inputStageName);
    }

    // outer join on non-required inputs
    boolean isFullOuter = joinedInputs == null;
    for (final String inputStageName : remainingInputs) {
      JavaPairRDD<Object, Object> preJoinRDD = preJoinRDDs.get(inputStageName);

      if (joinedInputs == null) {
        joinedInputs = preJoinRDD.mapValues(new InitialJoinFunction(inputStageName));
      } else {
        if (isFullOuter) {
          OuterJoinFlattenFunction outerJoinFlattenFunction = new OuterJoinFlattenFunction(inputStageName);

          joinedInputs = numPartitions == null ?
            joinedInputs.fullOuterJoin(preJoinRDD).mapValues(outerJoinFlattenFunction) :
            joinedInputs.fullOuterJoin(preJoinRDD, numPartitions).mapValues(outerJoinFlattenFunction);
        } else {
          LeftJoinFlattenFunction joinFlattenFunction = new LeftJoinFlattenFunction(inputStageName);

          joinedInputs = numPartitions == null ?
            joinedInputs.leftOuterJoin(preJoinRDD).mapValues(joinFlattenFunction) :
            joinedInputs.leftOuterJoin(preJoinRDD, numPartitions).mapValues(joinFlattenFunction);
        }
      }
    }

    // should never happen, but removes warnings
    if (joinedInputs == null) {
      throw new IllegalStateException("There are no inputs into join stage " + stageName);
    }

    return joinedInputs.flatMap(new JoinMergeFunction(pluginFunctionContext)).cache();
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    this.jsc = new JavaSparkContext();
    this.sec = sec;

    // Execution the whole pipeline in one long transaction. This is because the Spark execution
    // currently share the same contract and API as the MapReduce one.
    // The API need to expose DatasetContext, hence it needs to be executed inside a transaction
    sec.execute(this);
  }

  @Override
  public void run(DatasetContext context) throws Exception {
    BatchPhaseSpec phaseSpec = GSON.fromJson(sec.getSpecification().getProperty(Constants.PIPELINEID),
                                             BatchPhaseSpec.class);

    try (InputStream is = new FileInputStream(sec.getLocalizationContext().getLocalFile("ETLSpark.config"))) {
      sourceFactory = SparkBatchSourceFactory.deserialize(is);
      sinkFactory = SparkBatchSinkFactory.deserialize(is);
      DataInputStream dataInputStream = new DataInputStream(is);
      stagePartitions = GSON.fromJson(dataInputStream.readUTF(), MAP_TYPE);
    }
    datasetContext = context;
    runPipeline(phaseSpec.getPhase(), BatchSource.PLUGIN_TYPE, sec);
  }
}
