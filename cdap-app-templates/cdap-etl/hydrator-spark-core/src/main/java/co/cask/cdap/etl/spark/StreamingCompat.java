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

import com.google.common.base.Optional;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * Utility class to handle incompatibilities between Spark1 and Spark2 streaming.
 * All hydrator-spark-core modules must have this class with the exact same method signatures.
 * Incompatibilities are in a few places.
 *
 * DStream.foreachRDD() no longer takes a Function2 in Spark2 in favor of VoidFunction2.
 * In Spark1.2, a Function2 must be used because VoidFunction2 was not yet introduced.
 *
 * Outer join methods in Spark1 use guava's Optional whereas Spark2 uses its own Optional.
 *
 * JavaStreamingContext.getOrCreate() does not use a JavaStreamingContextFactory in Spark2, but requires it in Spark1.2.
 */
public final class StreamingCompat {

  private StreamingCompat() {

  }

  public static JavaStreamingContext getOrCreate(String checkpointDir, Function0<JavaStreamingContext> contextFunc) {
    return JavaStreamingContext.getOrCreate(checkpointDir, contextFunc);
  }

  public static <T> void foreachRDD(JavaDStream<T> stream, final Function2<JavaRDD<T>, Time, Void> func) {
    stream.foreachRDD(func);
  }

  public static <K, V1, V2> JavaPairDStream<K, Tuple2<V1, Optional<V2>>> leftOuterJoin(JavaPairDStream<K, V1> left,
                                                                                       JavaPairDStream<K, V2> right) {
    return left.leftOuterJoin(right);
  }

  public static <K, V1, V2> JavaPairDStream<K, Tuple2<V1, Optional<V2>>> leftOuterJoin(JavaPairDStream<K, V1> left,
                                                                                       JavaPairDStream<K, V2> right,
                                                                                       int numPartitions) {
    return left.leftOuterJoin(right, numPartitions);
  }

  public static <K, V1, V2> JavaPairDStream<K, Tuple2<Optional<V1>, Optional<V2>>> fullOuterJoin(
    JavaPairDStream<K, V1> left, JavaPairDStream<K, V2> right) {
    return left.fullOuterJoin(right);
  }

  public static <K, V1, V2> JavaPairDStream<K, Tuple2<Optional<V1>, Optional<V2>>> fullOuterJoin(
    JavaPairDStream<K, V1> left, JavaPairDStream<K, V2> right, int numPartitions) {
    return left.fullOuterJoin(right, numPartitions);
  }

}
