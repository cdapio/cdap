/*
 * Copyright © 2023 Cask Data, Inc.
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

import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.DelegatingSparkCollection;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.SparkPipelineRuntime;
import io.cdap.cdap.etl.spark.function.DatasetAggregationAccumulator;
import io.cdap.cdap.etl.spark.function.DatasetAggregationFinalizeFunction;
import io.cdap.cdap.etl.spark.function.DatasetAggregationGetKeyFunction;
import io.cdap.cdap.etl.spark.function.DatasetAggregationReduceFunction;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import io.cdap.cdap.etl.spark.function.PluginFunctionContext;
import javax.annotation.Nullable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 * This is an abstract class for {@link SparkCollection} that operate with Spark DataFrame/DataSet
 * API. It has 2 children: {@link DataframeCollection} that uses a DataFrame inside and
 * can be used to run SparkSQL and {@link OpaqueDatasetCollection} that use a DataSet with
 * serializing encoder and can be used to run regular plugins or perform platform transformations
 * (e.g. storing {@link io.cdap.cdap.etl.spark.streaming.function.RecordInfoWrapper}).
 */
public abstract class DatasetCollection<T> extends DelegatingSparkCollection<T>
    implements BatchCollection<T>{
  private static final Encoder KRYO_OBJECT_ENCODER = Encoders.kryo(Object.class);
  private static final Encoder KRYO_TUPLE_ENCODER = Encoders.tuple(
      KRYO_OBJECT_ENCODER, KRYO_OBJECT_ENCODER);
  private static final Encoder JAVA_OBJECT_ENCODER = Encoders.javaSerialization(Object.class);
  private static final Encoder JAVA_TUPLE_ENCODER = Encoders.tuple(
      JAVA_OBJECT_ENCODER, JAVA_OBJECT_ENCODER);

  protected final JavaSparkExecutionContext sec;
  protected final JavaSparkContext jsc;
  protected final SQLContext sqlContext;
  protected final DatasetContext datasetContext;
  protected final SparkBatchSinkFactory sinkFactory;
  protected final FunctionCache.Factory functionCacheFactory;
  protected final boolean useKryoForDatasets;
  protected final boolean ignorePartitionsDuringDatasetAggregation;

  protected DatasetCollection(JavaSparkExecutionContext sec, JavaSparkContext jsc,
      SQLContext sqlContext, DatasetContext datasetContext, SparkBatchSinkFactory sinkFactory,
      FunctionCache.Factory functionCacheFactory) {
    this.sec = sec;
    this.jsc = jsc;
    this.sqlContext = sqlContext;
    this.datasetContext = datasetContext;
    this.sinkFactory = sinkFactory;
    this.functionCacheFactory = functionCacheFactory;
    this.useKryoForDatasets = Boolean.parseBoolean(sec.getRuntimeArguments()
        .getOrDefault(Constants.DATASET_KRYO_ENABLED, Boolean.TRUE.toString()));
    this.ignorePartitionsDuringDatasetAggregation = Boolean.parseBoolean(sec.getRuntimeArguments()
        .getOrDefault(Constants.DATASET_AGGREGATE_IGNORE_PARTITIONS, Boolean.TRUE.toString()));
  }

  @Override
  protected synchronized SparkCollection<T> getDelegate() {
    return new RDDCollection<>(
          sec, functionCacheFactory, jsc, sqlContext, datasetContext, sinkFactory, getJavaRDD());
  }

  protected JavaRDD<T> getJavaRDD() {
    return getDataset().toJavaRDD();
  }

  /**
   * Provides an access to Spark Dataset of required type (e.g.
   * {@link io.cdap.cdap.api.data.format.StructuredRecord}). May do implicit type conversion
   * from spark-specific type (e.g. {@link org.apache.spark.sql.Row} to the required type.
   * @return spark DataSet
   */
  protected abstract Dataset<T> getDataset();

  /**
   * helper function to provide a generified encoder for any serializable type
   * @param useKryoForDatasets
   */
  protected static <V> Encoder<V> objectEncoder(boolean useKryoForDatasets) {
    return useKryoForDatasets ? KRYO_OBJECT_ENCODER : JAVA_OBJECT_ENCODER;
  }

  /**
   * helper function to provide a generified encoder for tuple of two serializable types
   */
  protected <V1, V2> Encoder<Tuple2<V1, V2>> tupleEncoder() {
    return useKryoForDatasets ? KRYO_TUPLE_ENCODER : JAVA_TUPLE_ENCODER;
  }

  /**
   * helper function to provide a generified encoder for any serializable type
   */
  protected  <V> Encoder<V> objectEncoder() {
    return objectEncoder(useKryoForDatasets);
  }

  @Override
  public <U> SparkCollection<U> map(Function<T, U> function) {
    MapFunction<T, U> mapFunction = function::call;
    Dataset<U> dataset = getDataset().map(mapFunction, objectEncoder());
    return wrap(dataset);
  }

  @Override
  public <U> SparkCollection<U> flatMap(StageSpec stageSpec, FlatMapFunction<T, U> function) {
    return OpaqueDatasetCollection.fromDataset(
        getDataset().flatMap(function, objectEncoder()), sec, jsc, sqlContext,
        datasetContext, sinkFactory, functionCacheFactory);
  }


  @Override
  public SparkCollection<T> cache() {
    SparkConf sparkConf = jsc.getConf();
    if (sparkConf.getBoolean(Constants.SPARK_PIPELINE_AUTOCACHE_ENABLE_FLAG, true)) {
      String cacheStorageLevelString = sparkConf.get(Constants.SPARK_PIPELINE_CACHING_STORAGE_LEVEL,
          Constants.DEFAULT_CACHING_STORAGE_LEVEL);
      StorageLevel cacheStorageLevel = StorageLevel.fromString(cacheStorageLevelString);
      return cache(cacheStorageLevel);
    } else {
      return this;
    }
  }

  protected DatasetCollection<T> cache(StorageLevel cacheStorageLevel) {
    return wrap(getDataset().persist(cacheStorageLevel));
  }

  @Override
  public SparkCollection union(SparkCollection other) {
    if (other instanceof DatasetCollection) {
      return wrap(getDataset().unionAll(((DatasetCollection) other).getDataset()));
    }
    return super.union(other);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> reduceAggregate(StageSpec stageSpec,
      @Nullable Integer partitions, StageStatisticsCollector collector) {
    return reduceDatasetAggregate(stageSpec, partitions, collector);
  }

  /**
   * Performs reduce aggregate using Dataset API. This allows SPARK to perform various optimizations that
   * are not available when working on the RDD level.
   */
  private <GROUP_KEY, AGG_VALUE> SparkCollection<RecordInfo<Object>> reduceDatasetAggregate(
      StageSpec stageSpec, @Nullable Integer partitions, StageStatisticsCollector collector) {
    PluginFunctionContext pluginFunctionContext = new PluginFunctionContext(stageSpec, sec, collector);
    DatasetAggregationGetKeyFunction<GROUP_KEY, T, AGG_VALUE> groupByFunction = new DatasetAggregationGetKeyFunction<>(
        pluginFunctionContext, functionCacheFactory.newCache());
    DatasetAggregationReduceFunction<T, AGG_VALUE> reduceFunction = new DatasetAggregationReduceFunction<>(
        pluginFunctionContext, functionCacheFactory.newCache());
    DatasetAggregationFinalizeFunction<GROUP_KEY, T, AGG_VALUE, ?> postFunction =
        new DatasetAggregationFinalizeFunction<>(pluginFunctionContext, functionCacheFactory.newCache());
    MapFunction<Tuple2<GROUP_KEY, DatasetAggregationAccumulator<T, AGG_VALUE>>, GROUP_KEY> keyFromTuple = Tuple2::_1;
    MapFunction<Tuple2<GROUP_KEY, DatasetAggregationAccumulator<T, AGG_VALUE>>,
        DatasetAggregationAccumulator<T, AGG_VALUE>> valueFromTuple = Tuple2::_2;

    Dataset<RecordInfo<Object>> groupedDataset = getDataset()
        .flatMap(groupByFunction, tupleEncoder())
        .groupByKey(keyFromTuple, objectEncoder())
        .mapValues(valueFromTuple, objectEncoder())
        .reduceGroups(reduceFunction)
        .flatMap(postFunction, objectEncoder());

    if (!ignorePartitionsDuringDatasetAggregation && partitions != null) {
      groupedDataset = groupedDataset.coalesce(partitions);
    }

    return OpaqueDatasetCollection.fromDataset(
        groupedDataset, sec, jsc, sqlContext, datasetContext, sinkFactory, functionCacheFactory);
  }

  private <U> OpaqueDatasetCollection<U> wrap(Dataset<U> dataset) {
    return OpaqueDatasetCollection.fromDataset(
        dataset, sec, jsc, sqlContext,
        datasetContext, sinkFactory, functionCacheFactory);
  }
}
