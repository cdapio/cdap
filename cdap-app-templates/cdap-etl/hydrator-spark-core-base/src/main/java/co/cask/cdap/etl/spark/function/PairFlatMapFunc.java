/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.spark.function;

import scala.Tuple2;

import java.io.Serializable;

/**
 * A function that returns zero or more key-value pair records from each input record.
 * Used instead of Spark's PairFlatMapFunction because that API is different in Spark1 and Spark2.
 *
 * @param <T> type of input record
 * @param <K> type of output key
 * @param <V> type of output value
 */
public interface PairFlatMapFunc<T, K, V> extends Serializable {

  Iterable<Tuple2<K, V>> call(T t) throws Exception;
}
