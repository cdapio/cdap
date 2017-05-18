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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Utility class to handle incompatibilities between Spark1 and Spark2. All hydrator-spark-core modules must have this
 * class with the exact same method signatures. Incompatibilities are in a few places.
 *
 * FlatMapFunction and PairFlatMapFunction in Spark2 was changed to return an Iterator instead of an Iterable
 * This class contains convert methods to change a FlatMapFunc and PairFlatMapFunc into the corresponding Spark version
 * specific class.
 *
 * DStream.foreachRDD() no longer takes a Function2 in Spark2 in favor of VoidFunction2.
 * In Spark1.2, a Function2 must be used because VoidFunction2 was not yet introduced.
 *
 * Outer join methods in Spark1 use guava's Optional whereas Spark2 uses its own Optional.
 */
public final class Compat {

  private Compat() {

  }

  public static <T, R> FlatMapFunction<T, R> convert(final FlatMapFunc<T, R> func) {
    return new FlatMapFunction<T, R>() {
      @Override
      public Iterator<R> call(T t) throws Exception {
        return func.call(t).iterator();
      }
    };
  }

  public static <T, K, V> PairFlatMapFunction<T, K, V> convert(final PairFlatMapFunc<T, K, V> func) {
    return new PairFlatMapFunction<T, K, V>() {
      @Override
      public Iterator<Tuple2<K, V>> call(T t) throws Exception {
        return func.call(t).iterator();
      }
    };
  }

  public static <T> void foreachRDD(JavaDStream<T> stream, final Function2<JavaRDD<T>, Time, Void> func) {
    stream.foreachRDD(new VoidFunction2<JavaRDD<T>, Time>() {
      @Override
      public void call(JavaRDD<T> v1, Time v2) throws Exception {
        func.call(v1, v2);
      }
    });
  }

  public static <K, V1, V2> JavaPairRDD<K, Tuple2<V1, Optional<V2>>> leftOuterJoin(JavaPairRDD<K, V1> left,
                                                                                   JavaPairRDD<K, V2> right) {
    return left.leftOuterJoin(right).mapValues(new ConvertOptional<V1, V2>());
  }

  public static <K, V1, V2> JavaPairRDD<K, Tuple2<V1, Optional<V2>>> leftOuterJoin(JavaPairRDD<K, V1> left,
                                                                                   JavaPairRDD<K, V2> right,
                                                                                   int numPartitions) {
    return left.leftOuterJoin(right, numPartitions).mapValues(new ConvertOptional<V1, V2>());
  }

  public static <K, V1, V2> JavaPairRDD<K, Tuple2<Optional<V1>, Optional<V2>>> fullOuterJoin(JavaPairRDD<K, V1> left,
                                                                                             JavaPairRDD<K, V2> right) {
    return left.fullOuterJoin(right).mapValues(new ConvertOptional2<V1, V2>());
  }

  public static <K, V1, V2> JavaPairRDD<K, Tuple2<Optional<V1>, Optional<V2>>> fullOuterJoin(JavaPairRDD<K, V1> left,
                                                                                             JavaPairRDD<K, V2> right,
                                                                                             int numPartitions) {
    return left.fullOuterJoin(right, numPartitions).mapValues(new ConvertOptional2<V1, V2>());
  }

  public static <K, V1, V2> JavaPairDStream<K, Tuple2<V1, Optional<V2>>> leftOuterJoin(JavaPairDStream<K, V1> left,
                                                                                       JavaPairDStream<K, V2> right) {
    return left.leftOuterJoin(right).mapValues(new ConvertOptional<V1, V2>());
  }

  public static <K, V1, V2> JavaPairDStream<K, Tuple2<V1, Optional<V2>>> leftOuterJoin(JavaPairDStream<K, V1> left,
                                                                                       JavaPairDStream<K, V2> right,
                                                                                       int numPartitions) {
    return left.leftOuterJoin(right, numPartitions).mapValues(new ConvertOptional<V1, V2>());
  }

  public static <K, V1, V2> JavaPairDStream<K, Tuple2<Optional<V1>, Optional<V2>>> fullOuterJoin(
    JavaPairDStream<K, V1> left, JavaPairDStream<K, V2> right) {
    return left.fullOuterJoin(right).mapValues(new ConvertOptional2<V1, V2>());
  }

  public static <K, V1, V2> JavaPairDStream<K, Tuple2<Optional<V1>, Optional<V2>>> fullOuterJoin(
    JavaPairDStream<K, V1> left, JavaPairDStream<K, V2> right, int numPartitions) {
    return left.fullOuterJoin(right, numPartitions).mapValues(new ConvertOptional2<V1, V2>());
  }

  private static <T> Optional<T> convert(org.apache.spark.api.java.Optional<T> opt) {
    return opt.isPresent() ? Optional.of(opt.get()) : Optional.<T>absent();
  }

  private static class ConvertOptional<T, U> implements
    Function<Tuple2<T, org.apache.spark.api.java.Optional<U>>, Tuple2<T, Optional<U>>> {

    @Override
    public Tuple2<T, Optional<U>> call(Tuple2<T, org.apache.spark.api.java.Optional<U>> in) throws Exception {
      return new Tuple2<>(in._1(), convert(in._2()));
    }
  }

  private static class ConvertOptional2<T, U> implements
    Function<Tuple2<org.apache.spark.api.java.Optional<T>, org.apache.spark.api.java.Optional<U>>,
             Tuple2<Optional<T>, Optional<U>>> {

    @Override
    public Tuple2<Optional<T>, Optional<U>> call(Tuple2<org.apache.spark.api.java.Optional<T>,
                                                 org.apache.spark.api.java.Optional<U>> in) throws Exception {
      return new Tuple2<>(convert(in._1()), convert(in._2()));
    }
  }
}
