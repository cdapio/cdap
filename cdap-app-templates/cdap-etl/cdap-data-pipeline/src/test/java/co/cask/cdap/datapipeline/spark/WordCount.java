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

package co.cask.cdap.datapipeline.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Spark Word count program in java
 */
public class WordCount {

  public static void main(String[] args) throws Exception {
    String inputFile = args[0];
    String outputFile = args[1];
    // Create a Java Spark Context.
    SparkConf conf = new SparkConf().setAppName("wordCount");
    JavaSparkContext sc = new JavaSparkContext(conf);
    // Load our input data, assuming each line is one word
    JavaRDD<String> words = sc.textFile(inputFile);
    // Transform into word and count.
    JavaRDD<String> counts = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String x) {
          return new Tuple2<>(x, 1);
        }
      })
      .reduceByKey(
        new Function2<Integer, Integer, Integer>() {
          public Integer call(Integer x, Integer y) {
            return x + y;
          }
        })
      .map(
        new Function<Tuple2<String, Integer>, String>() {
          @Override
          public String call(Tuple2<String, Integer> input) throws Exception {
            return input._1() + " " + input._2();
          }
        });
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile(outputFile);
  }
}
