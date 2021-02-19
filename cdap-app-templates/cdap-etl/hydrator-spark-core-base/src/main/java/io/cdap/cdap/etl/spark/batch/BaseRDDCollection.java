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
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.api.AlertPublisher;
import io.cdap.cdap.etl.api.AlertPublisherContext;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinField;
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
import io.cdap.cdap.etl.spark.Compat;
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
import io.cdap.cdap.etl.spark.function.FlatMapFunc;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import io.cdap.cdap.etl.spark.function.MultiOutputTransformFunction;
import io.cdap.cdap.etl.spark.function.MultiSinkFunction;
import io.cdap.cdap.etl.spark.function.PairFlatMapFunc;
import io.cdap.cdap.etl.spark.function.PluginFunctionContext;
import io.cdap.cdap.etl.spark.function.TransformFunction;
import io.cdap.cdap.etl.spark.join.JoinExpressionRequest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Map;
import java.util.Set;
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
  private static final Logger LOG = LoggerFactory.getLogger(BaseRDDCollection.class);
  private static final Gson GSON = new Gson();
  protected final JavaSparkExecutionContext sec;
  protected final JavaSparkContext jsc;
  protected final SQLContext sqlContext;
  protected final DatasetContext datasetContext;
  protected final SparkBatchSinkFactory sinkFactory;
  protected final JavaRDD<T> rdd;
  protected final FunctionCache.Factory functionCacheFactory;

  protected BaseRDDCollection(JavaSparkExecutionContext sec, FunctionCache.Factory functionCacheFactory,
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
    return wrap(rdd.flatMap(Compat.convert(new TransformFunction<T>(
      pluginFunctionContext, functionCacheFactory.newCache()))));
  }

  @Override
  public SparkCollection<RecordInfo<Object>> multiOutputTransform(StageSpec stageSpec,
                                                                  StageStatisticsCollector collector) {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    return wrap(rdd.flatMap(Compat.convert(new MultiOutputTransformFunction<T>(
      pluginFunctionContext, functionCacheFactory.newCache()))));
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
    PairFlatMapFunc<T, Object, T> groupByFunction = new AggregatorGroupByFunction<>(
      pluginFunctionContext, functionCacheFactory.newCache());
    PairFlatMapFunction<T, Object, T> sparkGroupByFunction = Compat.convert(groupByFunction);

    JavaPairRDD<Object, T> keyedCollection = rdd.flatMapToPair(sparkGroupByFunction);

    JavaPairRDD<Object, Iterable<T>> groupedCollection = partitions == null ?
      keyedCollection.groupByKey() : keyedCollection.groupByKey(partitions);

    FlatMapFunc<Tuple2<Object, Iterable<T>>, RecordInfo<Object>> aggregateFunction =
      new AggregatorAggregateFunction<>(pluginFunctionContext, functionCacheFactory.newCache());
    FlatMapFunction<Tuple2<Object, Iterable<T>>, RecordInfo<Object>> sparkAggregateFunction =
      Compat.convert(aggregateFunction);

    return wrap(groupedCollection.flatMap(sparkAggregateFunction));
  }

  @Override
  public SparkCollection<RecordInfo<Object>> reduceAggregate(StageSpec stageSpec, @Nullable Integer partitions,
                                                             StageStatisticsCollector collector) {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    PairFlatMapFunc<T, Object, T> groupByFunction = new AggregatorReduceGroupByFunction<>(
      pluginFunctionContext, functionCacheFactory.newCache());
    PairFlatMapFunction<T, Object, T> sparkGroupByFunction = Compat.convert(groupByFunction);

    JavaPairRDD<Object, T> keyedCollection = rdd.flatMapToPair(sparkGroupByFunction);

    Function<T, Object> initializeFunction = new AggregatorInitializeFunction<>(
      pluginFunctionContext, functionCacheFactory.newCache());
    Function2<Object, T, Object> mergeValueFunction = new AggregatorMergeValueFunction<>(
      pluginFunctionContext, functionCacheFactory.newCache());
    Function2<Object, Object, Object> mergePartitionFunction =
      new AggregatorMergePartitionFunction<>(pluginFunctionContext, functionCacheFactory.newCache());
    JavaPairRDD<Object, Object> groupedCollection = partitions == null ?
      keyedCollection.combineByKey(initializeFunction, mergeValueFunction, mergePartitionFunction) :
      keyedCollection.combineByKey(initializeFunction, mergeValueFunction, mergePartitionFunction, partitions);

    FlatMapFunc<Tuple2<Object, Object>, RecordInfo<Object>> postFunction =
      new AggregatorFinalizeFunction<>(pluginFunctionContext, functionCacheFactory.newCache());
    FlatMapFunction<Tuple2<Object, Object>, RecordInfo<Object>> postReduceFunction = Compat.convert(postFunction);

    return wrap(groupedCollection.flatMap(postReduceFunction));
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
        JavaPairRDD<Object, Object> sinkRDD = rdd.flatMapToPair(sinkFunction);
        for (String outputName : sinkFactory.writeFromRDD(sinkRDD, sec, stageSpec.getName())) {
          recordLineage(outputName);
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
            Compat.convert(new MultiSinkFunction(sec, phaseSpec, group, collectors));
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
  public Runnable createStoreTask(final StageSpec stageSpec, final SparkSink<T> sink) throws Exception {
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
    return new RDDCollection<>(sec, functionCacheFactory, jsc, sqlContext, datasetContext, sinkFactory, rdd);
  }

  protected Column eq(Column left, Column right, boolean isNullSafe) {
    if (isNullSafe) {
      return left.eqNullSafe(right);
    }
    return left.equalTo(right);
  }

  static String getSQL(JoinExpressionRequest join) {
    JoinCondition.OnExpression condition = join.getCondition();
    Map<String, String> datasetAliases = condition.getDatasetAliases();
    String leftName = join.getLeft().getStage();
    String leftAlias = datasetAliases.getOrDefault(leftName, leftName);
    String rightName = join.getRight().getStage();
    String rightAlias = datasetAliases.getOrDefault(rightName, rightName);

    StringBuilder query = new StringBuilder("SELECT ");
    // SELECT /*+ BROADCAST(t1), BROADCAST(t2) */
    // see https://spark.apache.org/docs/3.0.0/sql-ref-syntax-qry-select-hints.html for more info on join hints
    if (join.getLeft().isBroadcast() && join.getRight().isBroadcast()) {
      query.append("/*+ BROADCAST(").append(leftAlias).append("), BROADCAST(").append(rightAlias).append(") */ ");
    } else if (join.getLeft().isBroadcast()) {
      query.append("/*+ BROADCAST(").append(leftAlias).append(") */ ");
    } else if (join.getRight().isBroadcast()) {
      query.append("/*+ BROADCAST(").append(rightAlias).append(") */ ");
    }

    for (JoinField field : join.getFields()) {
      String outputName = field.getAlias() == null ? field.getFieldName() : field.getAlias();
      String datasetName = datasetAliases.getOrDefault(field.getStageName(), field.getStageName());
      // `datasetName`.`fieldName` as outputName
      query.append("`").append(datasetName).append("`.`")
        .append(field.getFieldName()).append("` as ").append(outputName).append(", ");
    }
    // remove trailing ', '
    query.setLength(query.length() - 2);

    String joinType;
    boolean leftRequired = join.getLeft().isRequired();
    boolean rightRequired = join.getRight().isRequired();
    if (leftRequired && rightRequired) {
      joinType = "JOIN";
    } else if (leftRequired && !rightRequired) {
      joinType = "LEFT OUTER JOIN";
    } else if (!leftRequired && rightRequired) {
      joinType = "RIGHT OUTER JOIN";
    } else {
      joinType = "FULL OUTER JOIN";
    }

    // FROM `leftDataset` as `leftAlias` JOIN `rightDataset` as `rightAlias`
    query.append(" FROM `").append(leftName).append("` as `").append(leftAlias).append("` ");
    query.append(joinType).append(" `").append(rightName).append("` as `").append(rightAlias).append("` ");
    // ON [expr]
    query.append(" ON ").append(condition.getExpression());
    return query.toString();
  }
}
