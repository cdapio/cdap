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

package io.cdap.cdap.etl.spark;

import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.streaming.Windower;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.join.JoinRequest;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Abstraction over different types of spark collections with common shared operations on those collections.
 * For example, both JavaRDD and JavaDStream support the flatMap operation, but don't share a higher interface.
 * This allows us to perform a common operation and have the implementation take care of whether it happens
 * on an RDD, DStream, DataFrame, etc.
 *
 * Also handles other Hydrator specific operations, such as performing a SparkCompute operation.
 *
 * @param <T> type of elements in the spark collection
 */
public interface SparkCollection<T> {

  <C> C getUnderlying();

  SparkCollection<T> cache();

  SparkCollection<T> union(SparkCollection<T> other);

  SparkCollection<RecordInfo<Object>> transform(StageSpec stageSpec, StageStatisticsCollector collector);

  SparkCollection<RecordInfo<Object>> multiOutputTransform(StageSpec stageSpec, StageStatisticsCollector collector);

  <U> SparkCollection<U> map(Function<T, U> function);

  <U> SparkCollection<U> flatMap(StageSpec stageSpec, FlatMapFunction<T, U> function);

  SparkCollection<RecordInfo<Object>> aggregate(StageSpec stageSpec, @Nullable Integer partitions,
                                                StageStatisticsCollector collector);

  SparkCollection<RecordInfo<Object>> reduceAggregate(StageSpec stageSpec, @Nullable Integer partitions,
                                                      StageStatisticsCollector collector);

  <K, V> SparkPairCollection<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function);

  <U> SparkCollection<U> compute(StageSpec stageSpec, SparkCompute<T, U> compute) throws Exception;

  Runnable createStoreTask(StageSpec stageSpec, PairFlatMapFunction<T, Object, Object> sinkFunction);

  Runnable createMultiStoreTask(PhaseSpec phaseSpec, Set<String> group, Set<String> sinks,
                                Map<String, StageStatisticsCollector> collectors);

  Runnable createStoreTask(StageSpec stageSpec, SparkSink<T> sink) throws Exception;

  void publishAlerts(StageSpec stageSpec, StageStatisticsCollector collector) throws Exception;

  SparkCollection<T> window(StageSpec stageSpec, Windower windower);

  SparkCollection<T> join(JoinRequest joinRequest);
}
