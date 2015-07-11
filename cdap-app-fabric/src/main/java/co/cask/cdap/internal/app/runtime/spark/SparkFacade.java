/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Interface to provide abstraction for accessing either scala or java version of the Apache SparkContext.
 */
interface SparkFacade {

  /**
   * Creates a Spark RDD for the given {@link InputFormat} class.
   *
   * @param inputFormatClass the {@link InputFormat} class
   * @param keyClass the key type
   * @param valueClass the value type
   * @param hConf the hadoop configuration. This may be modified with additional properties for creating
   *              the RDD; hence the caller should make a copy of the hadoop configuration before passing it.
   * @param <R> type of the resulting RDD
   * @param <K> type of the key
   * @param <V> type of the value
   * @return a Spark RDD
   */
  <R, K, V> R createRDD(Class<? extends InputFormat> inputFormatClass,
                        Class<K> keyClass, Class<V> valueClass, Configuration hConf);

  /**
   * Writes the RDD into dataset.
   *
   * @param rdd the RDD to write to dataset
   * @param outputFormatClass the {@link OutputFormat} class
   * @param keyClass the key type
   * @param valueClass the value type
   * @param hConf the hadoop configuration
   * @param <R> type of the RDD
   * @param <K> type of the key
   * @param <V> type of the value
   */
  <R, K, V> void saveAsDataset(R rdd, Class<? extends OutputFormat> outputFormatClass,
                               Class<K> keyClass, Class<V> valueClass, Configuration hConf);

  /**
   * Returns the Apache {@link SparkContext} or {@link JavaSparkContext}, depending on the language used in the
   * user Spark program.
   */
  <T> T getContext();

  /**
   * Stops the Apache {@link SparkContext} or {@link JavaSparkContext}, depending on the language used.
   */
  void stop();
}
