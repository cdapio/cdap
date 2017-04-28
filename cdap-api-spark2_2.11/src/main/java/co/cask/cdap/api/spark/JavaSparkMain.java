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

package co.cask.cdap.api.spark;

import co.cask.cdap.api.annotation.Beta;

import java.io.NotSerializableException;
import java.io.Serializable;

/**
 * A Java interface for Spark program to implement. It provides access to {@link JavaSparkExecutionContext} for
 * interacting with CDAP.
 * <p>
 * <pre><code>
 * public class JavaSparkTest extends JavaSparkMain {
 *
 *   {@literal @}Override
 *   public void run(JavaSparkExecutionContext sec) throws Exception {
 *     JavaSparkContext sc = new JavaSparkContext();
 *
 *     // Create a RDD from stream "input", with event body decoded as UTF-8 String
 *     JavaRDD&lt;String&gt; streamRDD = sec.fromStreamAsStringPair("input").values();
 *
 *     // Create a RDD from dataset "lookup", which represents a lookup table from String to Long
 *     JavaPairRDD&lt;String, Long&gt; lookupRDD = sec.fromDataset("lookup");
 *
 *     // Join the "input" stream with the "lookup" dataset and save it to "output" dataset
 *     JavaPairRDD&lt;String, Long&gt; resultRDD = streamRDD
 *       .mapToPair(new PairFunction&lt;String, String, String&gt;() {
 *         {@literal @}Override
 *         public Tuple2&lt;String, String&gt; call(String s) throws Exception {
 *           return Tuple2.apply(s, s);
 *         }
 *       })
 *       .join(lookupRDD)
 *       .mapValues(new Function&lt;Tuple2&lt;String, Long&gt;, Long&gt;() {
 *         {@literal @}Override
 *         public Long call(Tuple2&lt;String, Long&gt; v1) throws Exception {
 *           return v1._2;
 *         }
 *       });
 *
 *     sec.saveAsDataset(resultRDD, "output");
 *   }
 * }
 * </pre></code>
 * </p>
 *
 * This interface extends serializable because the closures are anonymous class in Java and Spark Serializes the
 * closures before sending it to worker nodes. This serialization of inner anonymous class expects the outer
 * containing class to be serializable else {@link NotSerializableException} is thrown. Having this interface
 * serializable gives a neater API.
 */
@Beta
public interface JavaSparkMain extends Serializable {

  /**
   * This method will be called when the Spark program starts.
   *
   * @param sec the context for interacting with CDAP
   */
  void run(JavaSparkExecutionContext sec) throws Exception;
}
