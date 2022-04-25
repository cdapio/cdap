/*
 * Copyright © 2022 Cask Data, Inc.
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


import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.streaming.Windower;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.SparkPairCollection;
import io.cdap.cdap.etl.spark.join.JoinExpressionRequest;
import io.cdap.cdap.etl.spark.join.JoinRequest;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Spark Collection implementation that takes a list of Suppliers for SparkCollection as input, and tries to consume
 * each supplier in order until a valid Spark Collection is found.
 * <p>
 * The resolution of the underlying SparkCollection is done lazily once the underlying collection is needed
 *
 * @param <T> class for the underlying SparkCollection records.
 */
public class LazySparkCollection<T> implements SparkCollection<T>, FutureCollection<T>, WrappableCollection<T> {
  private final Supplier<SparkCollection<T>>[] suppliers;
  private SparkCollection<T> resolved;

  public LazySparkCollection(Supplier<SparkCollection<T>>... suppliers) {
    this.suppliers = suppliers;
    this.resolved = null;
  }

  public SparkCollection<T> resolve() {
    // If this collection has already been resolved, return this instance.
    if (resolved != null) {
      return resolved;
    }

    // Consume suppliers in order until a valid SparkCollection is found
    for (Supplier<SparkCollection<T>> supplier : suppliers) {
      SparkCollection<T> collection = supplier.get();
      if (collection != null) {
        resolved = collection;
        return resolved;
      }
    }

    throw new IllegalArgumentException("Unable to resolve SparkCollection from suppliers");
  }

  @Override
  public <C> C getUnderlying() {
    return resolve().getUnderlying();
  }

  @Override
  public SparkCollection<T> cache() {
    return resolve().cache();
  }

  @Override
  public SparkCollection<T> union(SparkCollection<T> other) {
    return resolve().union(other);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> transform(StageSpec stageSpec, StageStatisticsCollector collector) {
    return resolve().transform(stageSpec, collector);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> multiOutputTransform(StageSpec stageSpec,
                                                                  StageStatisticsCollector collector) {
    return resolve().multiOutputTransform(stageSpec, collector);
  }

  @Override
  public <U> SparkCollection<U> map(Function<T, U> function) {
    return resolve().map(function);
  }

  @Override
  public <U> SparkCollection<U> flatMap(StageSpec stageSpec, FlatMapFunction<T, U> function) {
    return resolve().flatMap(stageSpec, function);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> aggregate(StageSpec stageSpec,
                                                       @Nullable Integer partitions,
                                                       StageStatisticsCollector collector) {
    return resolve().aggregate(stageSpec, partitions, collector);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> reduceAggregate(StageSpec stageSpec,
                                                             @Nullable Integer partitions,
                                                             StageStatisticsCollector collector) {
    return resolve().reduceAggregate(stageSpec, partitions, collector);
  }

  @Override
  public <K, V> SparkPairCollection<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function) {
    return resolve().flatMapToPair(function);
  }

  @Override
  public <U> SparkCollection<U> compute(StageSpec stageSpec, SparkCompute<T, U> compute) throws Exception {
    return resolve().compute(stageSpec, compute);
  }

  @Override
  public Runnable createStoreTask(StageSpec stageSpec, PairFlatMapFunction<T, Object, Object> sinkFunction) {
    return resolve().createStoreTask(stageSpec, sinkFunction);
  }

  @Override
  public Runnable createMultiStoreTask(PhaseSpec phaseSpec,
                                       Set<String> group,
                                       Set<String> sinks, Map<String,
    StageStatisticsCollector> collectors) {
    return resolve().createMultiStoreTask(phaseSpec, group, sinks, collectors);
  }

  @Override
  public Runnable createStoreTask(StageSpec stageSpec, SparkSink<T> sink) throws Exception {
    return resolve().createStoreTask(stageSpec, sink);
  }

  @Override
  public void publishAlerts(StageSpec stageSpec, StageStatisticsCollector collector) throws Exception {
    resolve().publishAlerts(stageSpec, collector);
  }

  @Override
  public SparkCollection<T> window(StageSpec stageSpec, Windower windower) {
    return resolve().window(stageSpec, windower);
  }

  @Override
  public SparkCollection<T> join(JoinRequest joinRequest) {
    return resolve().join(joinRequest);
  }

  @Override
  public SparkCollection<T> join(JoinExpressionRequest joinRequest) {
    return resolve().join(joinRequest);
  }

  @Override
  @SuppressWarnings("raw,unchecked")
  public WrappedCollection<T, ?> wrap(java.util.function.Function<SparkCollection<T>, SparkCollection<?>> mapper) {
    return new WrappedLazySparkCollection(this, mapper);
  }
}
