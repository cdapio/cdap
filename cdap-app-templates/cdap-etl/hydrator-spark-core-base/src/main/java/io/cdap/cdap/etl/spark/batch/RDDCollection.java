/*
 * Copyright © 2016-2023 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.exception.WrappedException;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.api.AlertPublisher;
import io.cdap.cdap.etl.api.AlertPublisherContext;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.lineage.AccessType;
import io.cdap.cdap.etl.api.streaming.Windower;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultAlertPublisherContext;
import io.cdap.cdap.etl.common.DefaultStageMetrics;
import io.cdap.cdap.etl.common.ExternalDatasets;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.common.TrackedIterator;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.SparkPairCollection;
import io.cdap.cdap.etl.spark.SparkPipelineRuntime;
import io.cdap.cdap.etl.spark.function.AggregatorAggregateFunction;
import io.cdap.cdap.etl.spark.function.AggregatorFinalizeFunction;
import io.cdap.cdap.etl.spark.function.AggregatorGroupByFunction;
import io.cdap.cdap.etl.spark.function.AggregatorInitializeFunction;
import io.cdap.cdap.etl.spark.function.AggregatorMergePartitionFunction;
import io.cdap.cdap.etl.spark.function.AggregatorMergeValueFunction;
import io.cdap.cdap.etl.spark.function.AggregatorReduceGroupByFunction;
import io.cdap.cdap.etl.spark.function.CountingFunction;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import io.cdap.cdap.etl.spark.function.MultiOutputTransformFunction;
import io.cdap.cdap.etl.spark.function.MultiSinkFunction;
import io.cdap.cdap.etl.spark.function.PluginFunctionContext;
import io.cdap.cdap.etl.spark.function.TransformFunction;
import io.cdap.cdap.etl.spark.join.JoinExpressionRequest;
import io.cdap.cdap.etl.spark.join.JoinRequest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * Implementation of {@link SparkCollection} that is backed by a JavaRDD.
 *
 * @param <T> type of object in the collection
 */
public class RDDCollection<T> implements BatchCollection<T> {
  private static final Logger LOG = LoggerFactory.getLogger(RDDCollection.class);
  private static final Gson GSON = new Gson();
  protected final JavaSparkExecutionContext sec;
  protected final JavaSparkContext jsc;
  protected final SQLContext sqlContext;
  protected final DatasetContext datasetContext;
  protected final SparkBatchSinkFactory sinkFactory;
  protected final JavaRDD<T> rdd;
  protected final FunctionCache.Factory functionCacheFactory;
  protected final boolean useDatasetAggregation;

  public RDDCollection(JavaSparkExecutionContext sec, FunctionCache.Factory functionCacheFactory,
                              JavaSparkContext jsc, SQLContext sqlContext,
                              DatasetContext datasetContext, SparkBatchSinkFactory sinkFactory,
                              JavaRDD<T> rdd) {
    this.sec = sec;
    this.jsc = jsc;
    this.sqlContext = sqlContext;
    this.datasetContext = datasetContext;
    this.sinkFactory = sinkFactory;
    this.functionCacheFactory = functionCacheFactory;
    this.rdd = rdd;
    this.useDatasetAggregation = Boolean.parseBoolean(
      sec.getRuntimeArguments().getOrDefault(Constants.DATASET_AGGREGATE_ENABLED, Boolean.TRUE.toString()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaRDD<T> getUnderlying() {
    return rdd;
  }

  @Override
  public SparkCollection<T> cache() {
    SparkConf sparkConf = jsc.getConf();
    if (sparkConf.getBoolean(Constants.SPARK_PIPELINE_AUTOCACHE_ENABLE_FLAG, true)) {
      String cacheStorageLevelString = sparkConf.get(Constants.SPARK_PIPELINE_CACHING_STORAGE_LEVEL,
                                                     Constants.DEFAULT_CACHING_STORAGE_LEVEL);
      StorageLevel cacheStorageLevel = StorageLevel.fromString(cacheStorageLevelString);
      return wrap(rdd.persist(cacheStorageLevel));
    } else {
      return wrap(rdd);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public SparkCollection<T> union(SparkCollection<T> other) {
    return wrap(rdd.union((JavaRDD<T>) other.getUnderlying()));
  }

  @Override
  public SparkCollection<RecordInfo<Object>> transform(StageSpec stageSpec, StageStatisticsCollector collector) {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    return flatMap(stageSpec, new TransformFunction<T>(
      pluginFunctionContext, functionCacheFactory.newCache()));
  }

  @Override
  public SparkCollection<RecordInfo<Object>> multiOutputTransform(StageSpec stageSpec,
                                                                  StageStatisticsCollector collector) {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    return flatMap(stageSpec,new MultiOutputTransformFunction<T>(
      pluginFunctionContext, functionCacheFactory.newCache()));
  }

  @Override
  public <U> SparkCollection<U> map(Function<T, U> function) {
    return wrap(rdd.map(function));
  }

  @Override
  public <U> SparkCollection<U> flatMap(StageSpec stageSpec, FlatMapFunction<T, U> function) {
    return wrap(rdd.flatMap(function));
  }

  @Override
  public SparkCollection<RecordInfo<Object>> aggregate(StageSpec stageSpec, @Nullable Integer partitions,
                                                       StageStatisticsCollector collector) {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    PairFlatMapFunction<T, Object, T> groupByFunction = new AggregatorGroupByFunction<>(
      pluginFunctionContext, functionCacheFactory.newCache());

    JavaPairRDD<Object, T> keyedCollection = rdd.flatMapToPair(groupByFunction);

    JavaPairRDD<Object, Iterable<T>> groupedCollection = partitions == null
      ? keyedCollection.groupByKey() : keyedCollection.groupByKey(partitions);

    FlatMapFunction<Tuple2<Object, Iterable<T>>, RecordInfo<Object>> sparkAggregateFunction =
      new AggregatorAggregateFunction<>(pluginFunctionContext, functionCacheFactory.newCache());

    return wrap(groupedCollection.flatMap(sparkAggregateFunction));
  }

  @SuppressWarnings("unchecked")
  @Override
  public SparkCollection<T> join(JoinRequest joinRequest) {
    return (SparkCollection<T>) toDataframeCollection(
        joinRequest.getLeftSchema()).join(joinRequest);
  }

  @SuppressWarnings("unchecked")
  @Override
  public SparkCollection<T> join(JoinExpressionRequest joinRequest) {
    return (SparkCollection<T>) toDataframeCollection(
        joinRequest.getLeft().getSchema()).join(joinRequest);
  }

  @Override
  public DataframeCollection toDataframeCollection(Schema schema) {
    StructType sparkSchema = DataFrames.toDataType(schema);
    JavaRDD<Row> rowRDD = ((JavaRDD<StructuredRecord>) rdd)
        .map(record -> DataFrames.toRow(record, sparkSchema));
    Dataset<Row> dataframe = sqlContext.createDataFrame(rowRDD.rdd(), sparkSchema);
    return new DataframeCollection(
        schema, dataframe, sec, jsc, sqlContext, datasetContext,
        sinkFactory, functionCacheFactory);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> reduceAggregate(StageSpec stageSpec, @Nullable Integer partitions,
                                                             StageStatisticsCollector collector) {

    if (useDatasetAggregation) {
      return OpaqueDatasetCollection.fromRdd(
              rdd, sec, jsc, sqlContext, datasetContext, sinkFactory, functionCacheFactory)
          .reduceAggregate(stageSpec, partitions, collector);
    }

    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    PairFlatMapFunction<T, Object, T> groupByFunction = new AggregatorReduceGroupByFunction<>(
      pluginFunctionContext, functionCacheFactory.newCache());

    JavaPairRDD<Object, T> keyedCollection = rdd.flatMapToPair(groupByFunction);

    Function<T, Object> initializeFunction = new AggregatorInitializeFunction<>(
      pluginFunctionContext, functionCacheFactory.newCache());
    Function2<Object, T, Object> mergeValueFunction = new AggregatorMergeValueFunction<>(
      pluginFunctionContext, functionCacheFactory.newCache());
    Function2<Object, Object, Object> mergePartitionFunction =
      new AggregatorMergePartitionFunction<>(pluginFunctionContext, functionCacheFactory.newCache());
    JavaPairRDD<Object, Object> groupedCollection = partitions == null
      ? keyedCollection.combineByKey(initializeFunction, mergeValueFunction, mergePartitionFunction) :
      keyedCollection.combineByKey(initializeFunction, mergeValueFunction, mergePartitionFunction, partitions);

    FlatMapFunction<Tuple2<Object, Object>, RecordInfo<Object>> postFunction =
      new AggregatorFinalizeFunction<>(pluginFunctionContext, functionCacheFactory.newCache());

    return wrap(groupedCollection.flatMap(postFunction));
  }

  @Override
  public <K, V> SparkPairCollection<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function) {
    return new PairRDDCollection<>(sec, functionCacheFactory, jsc,
                                   sqlContext, datasetContext, sinkFactory,
                                   rdd.flatMapToPair(function));
  }

  @Override
  public <U> SparkCollection<U> compute(StageSpec stageSpec, SparkCompute<T, U> compute) throws Exception {
    String stageName = stageSpec.getName();
    PipelineRuntime pipelineRuntime = new SparkPipelineRuntime(sec);
    SparkExecutionPluginContext sparkPluginContext =
      new BasicSparkExecutionPluginContext(sec, jsc, datasetContext, pipelineRuntime, stageSpec);
    compute.initialize(sparkPluginContext);

    JavaRDD<T> countedInput = rdd.map(new CountingFunction<T>(stageName, sec.getMetrics(),
                                                              Constants.Metrics.RECORDS_IN, null));
    SparkConf sparkConf = jsc.getConf();

    return wrap(compute.transform(sparkPluginContext, countedInput)
                  .map(new CountingFunction<U>(stageName, sec.getMetrics(), Constants.Metrics.RECORDS_OUT,
                                               sec.getDataTracer(stageName))));
  }

  @Override
  public Runnable createStoreTask(StageSpec stageSpec, PairFlatMapFunction<T, Object, Object> sinkFunction) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          JavaPairRDD<Object, Object> sinkRDD = rdd.flatMapToPair(sinkFunction);
          for (String outputName : sinkFactory.writeFromRDD(sinkRDD, sec, stageSpec.getName())) {
            recordLineage(outputName);
          }
        } catch (Exception e) {
          List<Throwable> causalChain = Throwables.getCausalChain(e);
          for(Throwable cause : causalChain) {
            if (cause instanceof WrappedException) {
              String stageName = ((WrappedException) cause).getStageName();
              LOG.error("Stage: {}", stageName);
              throw new WrappedException(e, stageName);
            }
          }
          MDC.put("Failed_Stage", stageSpec.getName());
          LOG.error("Stage: {}", stageSpec.getName());
          throw new WrappedException(e, stageSpec.getName());
        }
      }
    };
  }

  @Override
  public Runnable createMultiStoreTask(PhaseSpec phaseSpec, Set<String> group, Set<String> sinks,
                                       Map<String, StageStatisticsCollector> collectors) {
    return new Runnable() {
      @Override
      public void run() {
        PairFlatMapFunction<T, String, KeyValue<Object, Object>> multiSinkFunction =
          (PairFlatMapFunction<T, String, KeyValue<Object, Object>>)
            new MultiSinkFunction(sec, phaseSpec, group, collectors);
        JavaPairRDD<String, KeyValue<Object, Object>> taggedOutput = rdd.flatMapToPair(multiSinkFunction);
        for (String outputName : sinkFactory.writeCombinedRDD(taggedOutput, sec, sinks)) {
          recordLineage(outputName);
        }
      }
    };
  }

  private void recordLineage(String name) {
    try {
      ExternalDatasets.registerLineage(sec.getAdmin(), name, AccessType.WRITE, null,
                                       () -> datasetContext.getDataset(name));
    } catch (DatasetManagementException e) {
      LOG.warn("Unable to register dataset lineage for {}", name);
    }
  }

  @Override
  public Runnable createStoreTask(final StageSpec stageSpec, final SparkSink<T> sink) {
    return new Runnable() {
      @Override
      public void run() {
        String stageName = stageSpec.getName();
        PipelineRuntime pipelineRuntime = new SparkPipelineRuntime(sec);
        SparkExecutionPluginContext sparkPluginContext =
          new BasicSparkExecutionPluginContext(sec, jsc, datasetContext, pipelineRuntime, stageSpec);

        JavaRDD<T> countedRDD =
          rdd.map(new CountingFunction<T>(stageName, sec.getMetrics(), Constants.Metrics.RECORDS_IN, null));
        SparkConf sparkConf = jsc.getConf();
        try {
          sink.run(sparkPluginContext, countedRDD);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  @Override
  public void publishAlerts(StageSpec stageSpec, StageStatisticsCollector collector) throws Exception {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    AlertPublisher alertPublisher = pluginFunctionContext.createPlugin();
    PipelineRuntime pipelineRuntime = new SparkPipelineRuntime(sec);

    AlertPublisherContext alertPublisherContext =
      new DefaultAlertPublisherContext(pipelineRuntime, stageSpec, sec.getMessagingContext(), sec.getAdmin());
    alertPublisher.initialize(alertPublisherContext);
    StageMetrics stageMetrics = new DefaultStageMetrics(sec.getMetrics(), stageSpec.getName());
    TrackedIterator<Alert> trackedAlerts =
      new TrackedIterator<>(((JavaRDD<Alert>) rdd).collect().iterator(), stageMetrics, Constants.Metrics.RECORDS_IN);
    alertPublisher.publish(trackedAlerts);
    alertPublisher.destroy();
  }

  @Override
  public SparkCollection<T> window(StageSpec stageSpec, Windower windower) {
    throw new UnsupportedOperationException("Windowing is not supported on RDDs.");
  }

  protected <U> RDDCollection<U> wrap(JavaRDD<U> rdd) {
    return new RDDCollection<>(sec, functionCacheFactory, jsc, sqlContext, datasetContext, sinkFactory, rdd);
  }
}
