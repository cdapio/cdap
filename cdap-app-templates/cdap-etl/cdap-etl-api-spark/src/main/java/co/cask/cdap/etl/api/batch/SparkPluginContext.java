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

package co.cask.cdap.etl.api.batch;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.stream.StreamEventDecoder;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.Serializable;
import java.util.Map;

/**
 * Context passed to spark plugin types.
 */
@Beta
public interface SparkPluginContext extends BatchContext {

  /**
   * Returns the logical start time of this Spark job. Logical start time is the time when this Spark
   * job is supposed to start if this job is started by the scheduler. Otherwise it would be the current time when the
   * job runs.
   *
   * @return Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC).
   */
  long getLogicalStartTime();

  /**
   * Create a Spark RDD that uses {@link Dataset} as input source
   *
   * @param datasetName the name of the {@link Dataset} to be read as an RDD
   * @param kClass      the key class
   * @param vClass      the value class
   * @param <K>        type of RDD key
   * @param <V>        type of RDD value
   * @return the RDD created from Dataset
   * @throws UnsupportedOperationException if the SparkContext is not yet initialized
   */
  <K, V> JavaPairRDD<K, V> readFromDataset(String datasetName, Class<? extends K> kClass, Class<? extends V> vClass);

  /**
   * Create a Spark RDD that uses {@link Dataset} instantiated using the provided arguments as an input source.
   *
   * @param datasetName the name of the {@link Dataset} to be read as an RDD
   * @param kClass      the key class
   * @param vClass      the value class
   * @param datasetArgs arguments for the dataset
   * @param <K>        type of RDD key
   * @param <V>        type of RDD value
   * @return the RDD created from Dataset
   * @throws UnsupportedOperationException if the SparkContext is not yet initialized
   */
  <K, V> JavaPairRDD<K, V> readFromDataset(String datasetName, Class<? extends K> kClass, Class<? extends V> vClass,
                                           Map<String, String> datasetArgs);

  /**
   * Writes a Spark RDD to {@link Dataset}
   *
   * @param rdd         the rdd to be stored
   * @param datasetName the name of the {@link Dataset} where the RDD should be stored
   * @param kClass      the key class
   * @param vClass      the value class
   * @param <K>        type of RDD key
   * @param <V>        type of RDD value
   * @throws UnsupportedOperationException if the SparkContext is not yet initialized
   */
  <K, V> void writeToDataset(JavaPairRDD<K, V> rdd, String datasetName,
                             Class<? extends K> kClass, Class<? extends V> vClass);

  /**
   * Writes a Spark RDD to {@link Dataset} instantiated using the provided arguments.
   *
   * @param rdd         the rdd to be stored
   * @param datasetName the name of the {@link Dataset} where the RDD should be stored
   * @param kClass      the key class
   * @param vClass      the value class
   * @param datasetArgs arguments for the dataset
   * @param <K>        type of RDD key
   * @param <V>        type of RDD value
   * @throws UnsupportedOperationException if the SparkContext is not yet initialized
   */
  <K, V> void writeToDataset(JavaPairRDD<K, V> rdd, String datasetName,
                             Class<? extends K> kClass, Class<? extends V> vClass,
                             Map<String, String> datasetArgs);

  /**
   * Create a Spark RDD that uses complete {@link Stream} as input source
   *
   * @param streamName the name of the {@link Stream} to be read as an RDD
   * @param vClass     the value class
   * @param <V>        type of RDD value
   * @return the RDD created from {@link Stream}
   */
  <V> JavaPairRDD<LongWritable, V> readFromStream(String streamName, Class<? extends V> vClass);

  /**
   * Create a Spark RDD that uses {@link Stream} as input source
   *
   * @param streamName the name of the {@link Stream} to be read as an RDD
   * @param vClass     the value class
   * @param startTime  the starting time of the stream to be read in milliseconds. To read from the starting of the
   *                   stream set this to 0
   * @param endTime    the ending time of the streams to be read in milliseconds. To read up to the end of the stream
   *                   set this to Long.MAX_VALUE
   * @param <K>        type of RDD key
   * @param <V>        type of RDD value
   * @return the RDD created from {@link Stream}
   */
  <K, V> JavaPairRDD<K, V> readFromStream(String streamName, Class<? extends V> vClass, long startTime, long endTime);

  /**
   * Create a Spark RDD that uses {@link Stream} as input source according to the given {@link StreamEventDecoder}
   *
   * @param streamName  the name of the {@link Stream} to be read as an RDD
   * @param vClass      the value class
   * @param startTime   the starting time of the stream to be read in milliseconds. To read from the starting of the
   *                    stream set this to 0
   * @param endTime     the ending time of the streams to be read in milliseconds. To read up to the end of the stream
   *                    set this to Long.MAX_VALUE
   * @param decoderType the decoder to use while reading streams
   * @param <V>        type of RDD value
   * @return the RDD created from {@link Stream}
   */
  <V> JavaPairRDD<LongWritable, V> readFromStream(String streamName, Class<? extends V> vClass,
                                                  long startTime, long endTime,
                                                  Class<? extends StreamEventDecoder> decoderType);

  /**
   * Create a Spark RDD that uses {@link Stream} as input source according to the given {@link StreamBatchReadable}.
   *
   * @param stream a {@link StreamBatchReadable} containing information on the stream to read from
   * @param vClass the value class
   * @param <V>        type of RDD value
   * @return the RDD created from {@link Stream}
   */
  <V> JavaPairRDD<LongWritable, V> readFromStream(StreamBatchReadable stream, Class<? extends V> vClass);

  /**
   * Returns
   * <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.api.java.JavaSparkContext">
   * JavaSparkContext</a> or
   * <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkContext">SparkContext</a>
   * depending on user's job type.
   *
   * @param <T> the type of Spark Context
   * @return the Spark Context
   */
  <T> T getOriginalSparkContext();

  /**
   * Returns a {@link Serializable} {@link PluginContext} which can be used to request for plugins instances. The
   * instance returned can also be used in Spark program's closures.
   *
   * @return A {@link Serializable} {@link PluginContext}.
   */
  PluginContext getPluginContext();

  /**
   * Sets a
   * <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkConf">SparkConf</a>
   * to be used for the Spark execution. Only configurations set inside the
   * {@link SparkSink#prepareRun} call will affect the Spark execution.
   *
   * @param <T> the SparkConf type
   */
  <T> void setSparkConf(T sparkConf);

}
