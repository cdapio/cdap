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

package io.cdap.cdap.etl.spark.streaming;

import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.SparkPairCollection;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

/**
 * JavaPairDStream backed {@link SparkPairCollection}
 *
 * @param <K> type of key in the collection
 * @param <V> type of value in the collection
 */
public class PairDStreamCollection<K, V> implements SparkPairCollection<K, V> {
  private final JavaSparkExecutionContext sec;
  private final FunctionCache.Factory functionCacheFactory;
  private final JavaPairDStream<K, V> pairStream;

  public PairDStreamCollection(JavaSparkExecutionContext sec,
                               FunctionCache.Factory functionCacheFactory,
                               JavaPairDStream<K, V> pairStream) {
    this.sec = sec;
    this.functionCacheFactory = functionCacheFactory;
    this.pairStream = pairStream;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaPairDStream<K, V> getUnderlying() {
    return pairStream;
  }

  @Override
  public <T> SparkCollection<T> flatMap(FlatMapFunction<Tuple2<K, V>, T> function) {
    return new DStreamCollection<>(sec, functionCacheFactory, pairStream.flatMap(function));
  }

  @Override
  public <T> SparkPairCollection<K, T> mapValues(Function<V, T> function) {
    return wrap(pairStream.mapValues(function));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> SparkPairCollection<K, Tuple2<V, T>> join(SparkPairCollection<K, T> other) {
    return wrap(pairStream.join((JavaPairDStream<K, T>) other.getUnderlying()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> SparkPairCollection<K, Tuple2<V, T>> join(SparkPairCollection<K, T> other, int numPartitions) {
    return wrap(pairStream.join((JavaPairDStream<K, T>) other.getUnderlying(), numPartitions));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> SparkPairCollection<K, Tuple2<V, Optional<T>>> leftOuterJoin(SparkPairCollection<K, T> other) {
    return wrap(pairStream.leftOuterJoin((JavaPairDStream<K, T>) other.getUnderlying()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> SparkPairCollection<K, Tuple2<V, Optional<T>>> leftOuterJoin(SparkPairCollection<K, T> other,
                                                                          int numPartitions) {
    return wrap(
      pairStream.leftOuterJoin((JavaPairDStream<K, T>) other.getUnderlying(), numPartitions));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> SparkPairCollection<K, Tuple2<Optional<V>, Optional<T>>> fullOuterJoin(SparkPairCollection<K, T> other) {
    return wrap(pairStream.fullOuterJoin((JavaPairDStream<K, T>) other.getUnderlying()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> SparkPairCollection<K, Tuple2<Optional<V>, Optional<T>>> fullOuterJoin(SparkPairCollection<K, T> other,
                                                                                    int numPartitions) {
    return wrap(
      pairStream.fullOuterJoin((JavaPairDStream<K, T>) other.getUnderlying(), numPartitions));
  }

  private <T, U> PairDStreamCollection<T, U> wrap(JavaPairDStream<T, U> pairStream) {
    return new PairDStreamCollection<>(sec, functionCacheFactory, pairStream);
  }
}
