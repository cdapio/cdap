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

package io.cdap.cdap.etl.spark;

import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.api.relational.RelationalTransform;
import io.cdap.cdap.etl.api.streaming.Windower;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.StageStatisticsCollector;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.function.CountingFunction;
import io.cdap.cdap.etl.spark.join.JoinExpressionRequest;
import io.cdap.cdap.etl.spark.join.JoinRequest;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

/**
 * A helper class to write collections that mostly delegate to underlying one
 */
public abstract class DelegatingSparkCollection<T> implements SparkCollection<T>{

  protected abstract SparkCollection<T> getDelegate();

  @Override
  public SparkCollection<RecordInfo<Object>> transform(
      StageSpec stageSpec, StageStatisticsCollector collector) {
    return getDelegate().transform(stageSpec, collector);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> multiOutputTransform(StageSpec stageSpec,
                                                                  StageStatisticsCollector collector) {
    return getDelegate().transform(stageSpec, collector);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> aggregate(StageSpec stageSpec,
                                                       @Nullable Integer partitions,
                                                       StageStatisticsCollector collector) {
    return getDelegate().aggregate(stageSpec, partitions, collector);
  }

  @Override
  public SparkCollection<RecordInfo<Object>> reduceAggregate(StageSpec stageSpec,
                                                             @Nullable Integer partitions,
                                                             StageStatisticsCollector collector) {
    return getDelegate().reduceAggregate(stageSpec, partitions, collector);
  }

  @Override
  public <K, V> SparkPairCollection<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function) {
    return getDelegate().flatMapToPair(function);
  }

  @Override
  public <C> C getUnderlying() {
    return getDelegate().getUnderlying();
  }

  @Override
  public SparkCollection<T> cache() {
    return getDelegate().cache();
  }

  @Override
  public SparkCollection union(SparkCollection other) {
    return getDelegate().union(other);
  }

  @Override
  public Runnable createStoreTask(StageSpec stageSpec,
                                  SparkSink sink) throws Exception {
    return getDelegate().createStoreTask(stageSpec, sink);
  }

  @Override
  public void publishAlerts(StageSpec stageSpec,
                            StageStatisticsCollector collector) throws Exception {
    getDelegate().publishAlerts(stageSpec, collector);
  }

  @Override
  public SparkCollection window(StageSpec stageSpec,
                                   Windower windower) {
    return getDelegate().window(stageSpec, windower);
  }

  @Override
  public <U> Optional<SparkCollection<U>> tryRelationalTransform(StageSpec stageSpec,
      RelationalTransform transform) {
    return getDelegate().tryRelationalTransform(stageSpec, transform);
  }

  @Override
  public <U> SparkCollection<U> map(Function<T, U> function) {
    return getDelegate().map(function);
  }

  @Override
  public <U> SparkCollection<U> flatMap(StageSpec stageSpec, FlatMapFunction<T, U> function) {
    return getDelegate().flatMap(stageSpec, function);
  }

  @Override
  public <U> SparkCollection<U> compute(StageSpec stageSpec, SparkCompute<T, U> compute)
      throws Exception {
    return getDelegate().compute(stageSpec, compute);
  }

  @Override
  public Runnable createStoreTask(StageSpec stageSpec,
      PairFlatMapFunction<T, Object, Object> sinkFunction) {
    return getDelegate().createStoreTask(stageSpec, sinkFunction);
  }

  @Override
  public Runnable createMultiStoreTask(PhaseSpec phaseSpec, Set<String> group, Set<String> sinks,
      Map<String, StageStatisticsCollector> collectors) {
    return getDelegate().createMultiStoreTask(phaseSpec, group, sinks, collectors);
  }

  @Override
  public SparkCollection<T> join(JoinRequest joinRequest) {
    return getDelegate().join(joinRequest);
  }

  @Override
  public SparkCollection<T> join(JoinExpressionRequest joinRequest) {
    return getDelegate().join(joinRequest);
  }
}
