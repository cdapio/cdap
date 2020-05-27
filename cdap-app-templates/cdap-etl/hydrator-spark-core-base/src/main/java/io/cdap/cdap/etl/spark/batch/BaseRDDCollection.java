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

package io.cdap.cdap.etl.spark.batch;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.api.AlertPublisher;
import io.cdap.cdap.etl.api.AlertPublisherContext;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.streaming.Windower;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultAlertPublisherContext;
import io.cdap.cdap.etl.common.DefaultStageMetrics;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.common.TrackedIterator;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.SparkPairCollection;
import io.cdap.cdap.etl.spark.SparkPipelineRuntime;
import io.cdap.cdap.etl.spark.function.AggregatorAggregateFunction;
import io.cdap.cdap.etl.spark.function.AggregatorGroupByFunction;
import io.cdap.cdap.etl.spark.function.AggregatorPostReduceFunction;
import io.cdap.cdap.etl.spark.function.AggregatorReduceFunction;
import io.cdap.cdap.etl.spark.function.AggregatorReduceGroupByFunction;
import io.cdap.cdap.etl.spark.function.CountingFunction;
import io.cdap.cdap.etl.spark.function.FlatMapFunc;
import io.cdap.cdap.etl.spark.function.MultiOutputTransformFunction;
import io.cdap.cdap.etl.spark.function.PairFlatMapFunc;
import io.cdap.cdap.etl.spark.function.PluginFunctionContext;
import io.cdap.cdap.etl.spark.function.TransformFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import javax.annotation.Nullable;


/**
 * Implementation of {@link SparkCollection} that is backed by a JavaRDD. Spark1 and Spark2 implementations need to be
 * separate because DataFrames are not compatible between Spark1 and Spark2. In Spark2, DataFrame is just a
 * Dataset of Row. In Spark1, DataFrame is its own class. Spark1 and Spark2 also have different methods that they
 * support on a DataFrame/Dataset.
 *
 * @param <T> type of object in the collection
 */
public abstract class BaseRDDCollection<T> implements SparkCollection<T> {
  private static final Gson GSON = new Gson();
  protected final JavaSparkExecutionContext sec;
  protected final JavaSparkContext jsc;
  protected final SQLContext sqlContext;
  protected final DatasetContext datasetContext;
  protected final SparkBatchSinkFactory sinkFactory;
  protected final JavaRDD<T> rdd;

  protected BaseRDDCollection(JavaSparkExecutionContext sec, JavaSparkContext jsc, SQLContext sqlContext,
                              DatasetContext datasetContext, SparkBatchSinkFactory sinkFactory, JavaRDD<T> rdd) {
    this.sec = sec;
    this.jsc = jsc;
    this.sqlContext = sqlContext;
    this.datasetContext = datasetContext;
    this.sinkFactory = sinkFactory;
    this.rdd = rdd;
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
    return wrap(rdd.flatMap(Compat.convert(new TransformFunction<T>(pluginFunctionContext))));
  }

  @Override
  public SparkCollection<RecordInfo<Object>> multiOutputTransform(StageSpec stageSpec,
                                                                  StageStatisticsCollector collector) {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    return wrap(rdd.flatMap(Compat.convert(new MultiOutputTransformFunction<T>(pluginFunctionContext))));
  }

  @Override
  public <U> SparkCollection<U> flatMap(StageSpec stageSpec, FlatMapFunction<T, U> function) {
    return wrap(rdd.flatMap(function));
  }

  @Override
  public SparkCollection<RecordInfo<Object>> aggregate(StageSpec stageSpec, @Nullable Integer partitions,
                                                       StageStatisticsCollector collector) {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    PairFlatMapFunc<T, Object, T> groupByFunction = new AggregatorGroupByFunction<>(pluginFunctionContext);
    PairFlatMapFunction<T, Object, T> sparkGroupByFunction = Compat.convert(groupByFunction);

    JavaPairRDD<Object, T> keyedCollection = rdd.flatMapToPair(sparkGroupByFunction);

    JavaPairRDD<Object, Iterable<T>> groupedCollection = partitions == null ?
      keyedCollection.groupByKey() : keyedCollection.groupByKey(partitions);

    FlatMapFunc<Tuple2<Object, Iterable<T>>, RecordInfo<Object>> aggregateFunction =
      new AggregatorAggregateFunction<>(pluginFunctionContext);
    FlatMapFunction<Tuple2<Object, Iterable<T>>, RecordInfo<Object>> sparkAggregateFunction =
      Compat.convert(aggregateFunction);

    return wrap(groupedCollection.flatMap(sparkAggregateFunction));
  }

  @Override
  public SparkCollection<RecordInfo<Object>> reduceAggregate(StageSpec stageSpec, @Nullable Integer partitions,
                                                             StageStatisticsCollector collector) {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    PairFlatMapFunc<T, Object, T> groupByFunction = new AggregatorReduceGroupByFunction<>(pluginFunctionContext);
    PairFlatMapFunction<T, Object, T> sparkGroupByFunction = Compat.convert(groupByFunction);

    JavaPairRDD<Object, T> keyedCollection = rdd.flatMapToPair(sparkGroupByFunction);

    Function2<T, T, T> reduceFunction = new AggregatorReduceFunction<>(pluginFunctionContext);
    JavaPairRDD<Object, T> groupedCollection = partitions == null ?
      keyedCollection.reduceByKey(reduceFunction) : keyedCollection.reduceByKey(reduceFunction, partitions);

    FlatMapFunc<Tuple2<Object, T>, RecordInfo<Object>> postFunction =
      new AggregatorPostReduceFunction<>(pluginFunctionContext);
    FlatMapFunction<Tuple2<Object, T>, RecordInfo<Object>> postReduceFunction = Compat.convert(postFunction);

    return wrap(groupedCollection.flatMap(postReduceFunction));
  }

  @Override
  public <K, V> SparkPairCollection<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function) {
    return new PairRDDCollection<>(sec, jsc, sqlContext, datasetContext, sinkFactory, rdd.flatMapToPair(function));
  }

  @Override
  public <U> SparkCollection<U> compute(StageSpec stageSpec, SparkCompute<T, U> compute) throws Exception {
    String stageName = stageSpec.getName();
    PipelineRuntime pipelineRuntime = new SparkPipelineRuntime(sec);
    SparkExecutionPluginContext sparkPluginContext =
      new BasicSparkExecutionPluginContext(sec, jsc, datasetContext, pipelineRuntime, stageSpec);
    compute.initialize(sparkPluginContext);

    JavaRDD<T> countedInput = rdd.map(new CountingFunction<T>(stageName, sec.getMetrics(), "records.in", null));
    SparkConf sparkConf = jsc.getConf();
    if (sparkConf.getBoolean(Constants.SPARK_PIPELINE_AUTOCACHE_ENABLE_FLAG, true)) {
      countedInput = countedInput.cache();
    }

    return wrap(compute.transform(sparkPluginContext, countedInput)
                  .map(new CountingFunction<U>(stageName, sec.getMetrics(), "records.out",
                                               sec.getDataTracer(stageName))));
  }

  @Override
  public Runnable createStoreTask(final StageSpec stageSpec,
                                  final PairFlatMapFunction<T, Object, Object> sinkFunction) {
    return new Runnable() {
      @Override
      public void run() {
        JavaPairRDD<Object, Object> sinkRDD = rdd.flatMapToPair(sinkFunction);
        sinkFactory.writeFromRDD(sinkRDD, sec, stageSpec.getName(), Object.class, Object.class);
      }
    };
  }

  @Override
  public Runnable createStoreTask(final StageSpec stageSpec, final SparkSink<T> sink) throws Exception {
    return new Runnable() {
      @Override
      public void run() {
        String stageName = stageSpec.getName();
        PipelineRuntime pipelineRuntime = new SparkPipelineRuntime(sec);
        SparkExecutionPluginContext sparkPluginContext =
          new BasicSparkExecutionPluginContext(sec, jsc, datasetContext, pipelineRuntime, stageSpec);

        JavaRDD<T> countedRDD =
          rdd.map(new CountingFunction<T>(stageName, sec.getMetrics(), "records.in", null));
        SparkConf sparkConf = jsc.getConf();
        if (sparkConf.getBoolean(Constants.SPARK_PIPELINE_AUTOCACHE_ENABLE_FLAG, true)) {
          countedRDD = countedRDD.cache();
        }
        try {
          sink.run(sparkPluginContext, countedRDD);
        } catch (Exception e) {
          Throwables.propagate(e);
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
    return new RDDCollection<>(sec, jsc, sqlContext, datasetContext, sinkFactory, rdd);
  }
}
