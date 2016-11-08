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

import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.streaming.Windower;
import co.cask.cdap.etl.planner.StageInfo;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;

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

  <U> SparkCollection<U> flatMap(StageInfo stageInfo, FlatMapFunction<T, U> function);

  <U> SparkCollection<U> aggregate(StageInfo stageInfo, @Nullable Integer partitions);

  <K, V> SparkPairCollection<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function);

  <U> SparkCollection<U> compute(StageInfo stageInfo, SparkCompute<T, U> compute) throws Exception;

  void store(StageInfo stageInfo, PairFlatMapFunction<T, Object, Object> sinkFunction);

  void store(StageInfo stageInfo, SparkSink<T> sink) throws Exception;

  SparkCollection<T> window(StageInfo stageInfo, Windower windower);
}
