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

package co.cask.cdap.etl.spark;

import co.cask.cdap.etl.spark.function.FlatMapFunc;
import co.cask.cdap.etl.spark.function.PairFlatMapFunc;
import com.google.common.base.Optional;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;

/**
 * Utility class to handle incompatibilities between Spark1 and Spark2. All hydrator-spark-core modules must have this
 * class with the exact same method signatures. Incompatibilities are in a few places. Should not contain any
 * classes from Spark streaming.
 *
 * FlatMapFunction and PairFlatMapFunction in Spark2 was changed to return an Iterator instead of an Iterable
 * This class contains convert methods to change a FlatMapFunc and PairFlatMapFunc into the corresponding Spark version
 * specific class.
 *
 * Outer join methods in Spark1 use guava's Optional whereas Spark2 uses its own Optional.
 */
public final class Compat {
  public static final String SPARK_COMPAT = "spark1_2.10";

  private Compat() {

  }

  public static <T, R> FlatMapFunction<T, R> convert(final FlatMapFunc<T, R> func) {
    return new FlatMapFunction<T, R>() {
      @Override
      public Iterable<R> call(T t) throws Exception {
        return func.call(t);
      }
    };
  }

  public static <T, K, V> PairFlatMapFunction<T, K, V> convert(final PairFlatMapFunc<T, K, V> func) {
    return new PairFlatMapFunction<T, K, V>() {
      @Override
      public Iterable<Tuple2<K, V>> call(T t) throws Exception {
        return func.call(t);
      }
    };
  }

  public static <T> void foreachRDD(JavaDStream<T> stream, final Function2<JavaRDD<T>, Time, Void> func) {
    stream.foreachRDD(func);
  }

  public static <K, V1, V2> JavaPairRDD<K, Tuple2<V1, Optional<V2>>> leftOuterJoin(JavaPairRDD<K, V1> left,
                                                                                   JavaPairRDD<K, V2> right) {
    return left.leftOuterJoin(right);
  }

  public static <K, V1, V2> JavaPairRDD<K, Tuple2<V1, Optional<V2>>> leftOuterJoin(JavaPairRDD<K, V1> left,
                                                                                   JavaPairRDD<K, V2> right,
                                                                                   int numPartitions) {
    return left.leftOuterJoin(right, numPartitions);
  }

  public static <K, V1, V2> JavaPairRDD<K, Tuple2<Optional<V1>, Optional<V2>>> fullOuterJoin(JavaPairRDD<K, V1> left,
                                                                                             JavaPairRDD<K, V2> right) {
    return left.fullOuterJoin(right);
  }

  public static <K, V1, V2> JavaPairRDD<K, Tuple2<Optional<V1>, Optional<V2>>> fullOuterJoin(JavaPairRDD<K, V1> left,
                                                                                             JavaPairRDD<K, V2> right,
                                                                                             int numPartitions) {
    return left.fullOuterJoin(right, numPartitions);
  }
}
