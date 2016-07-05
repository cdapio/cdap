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

package co.cask.cdap.etl.spark;

import com.google.common.base.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * Abstraction over different types of spark pair collections with common shared operations on those collections.
 * For example, both JavaPairRDD and JavaPairDStream support the flatMap operation, but don't share a higher interface.
 * This allows us to perform a common operation and have the implementation take care of whether it happens
 * on a PairRDD, PairDStream, or whatever other object is being used.
 *
 * @param <K> type of key in the collection
 * @param <V> type of value in the collection
 */
public interface SparkPairCollection<K, V> {

  <C> C getUnderlying();

  <T> SparkCollection<T> flatMap(FlatMapFunction<Tuple2<K, V>, T> function);

  <T> SparkPairCollection<K, T> mapValues(Function<V, T> function);

  SparkPairCollection<K, Iterable<V>> groupByKey();

  SparkPairCollection<K, Iterable<V>> groupByKey(int numPartitions);

  <T> SparkPairCollection<K, Tuple2<V, T>> join(SparkPairCollection<K, T> other);

  <T> SparkPairCollection<K, Tuple2<V, T>> join(SparkPairCollection<K, T> other, int numPartitions);

  <T> SparkPairCollection<K, Tuple2<V, Optional<T>>> leftOuterJoin(SparkPairCollection<K, T> other);

  <T> SparkPairCollection<K, Tuple2<V, Optional<T>>> leftOuterJoin(SparkPairCollection<K, T> other,
                                                                   int numPartitions);

  <T> SparkPairCollection<K, Tuple2<Optional<V>, Optional<T>>> fullOuterJoin(SparkPairCollection<K, T> other);

  <T> SparkPairCollection<K, Tuple2<Optional<V>, Optional<T>>> fullOuterJoin(SparkPairCollection<K, T> other,
                                                                             int numPartitions);
}
