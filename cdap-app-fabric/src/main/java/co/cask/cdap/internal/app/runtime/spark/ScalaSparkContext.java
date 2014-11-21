/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.data.stream.StreamInputFormat;
import co.cask.cdap.internal.app.runtime.spark.dataset.SparkDatasetInputFormat;
import co.cask.cdap.internal.app.runtime.spark.dataset.SparkDatasetOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.NewHadoopRDD;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.IOException;

/**
 * A concrete implementation of {@link AbstractSparkContext} which is used if the user's spark job is written in Scala.
 */
class ScalaSparkContext extends AbstractSparkContext {

  private final SparkContext originalSparkContext;

  public ScalaSparkContext(BasicSparkContext basicSparkContext) {
    super(basicSparkContext);
    this.originalSparkContext = new SparkContext(getSparkConf());
    originalSparkContext.addSparkListener(new SparkProgramListener());
  }

  /**
   * Gets a {@link Dataset} as a {@link NewHadoopRDD}
   *
   * @param datasetName the name of the {@link Dataset} to be read as an RDD
   * @param kClass      the key class
   * @param vClass      the value class
   * @param <T>         type of the RDD
   * @return the {@link NewHadoopRDD} created from the dataset to be read
   * @throws {@link IllegalArgumentException} if the dataset to be read does not implements {@link BatchReadable}
   */
  @Override
  public <T> T readFromDataset(String datasetName, Class<?> kClass, Class<?> vClass) {
    Configuration hConf = setInputDataset(datasetName);
    return (T) originalSparkContext.newAPIHadoopFile(datasetName, SparkDatasetInputFormat.class, kClass, vClass, hConf);
  }

  /**
   * Stores a {@link RDD} to {@link Dataset}
   *
   * @param rdd         the {@link RDD} to be stored
   * @param datasetName the name of the {@link Dataset} where the RDD should be stored
   * @param kClass      the key class
   * @param vClass      the value class
   * @param <T>         type of RDD
   */
  @Override
  public <T> void writeToDataset(T rdd, String datasetName, Class<?> kClass, Class<?> vClass) {
    writeToDatasetHelper(rdd, datasetName, kClass, vClass);
  }

  /**
   * Gets a {@link Stream} as a {@link NewHadoopRDD}
   *
   * @param streamName  the name of the {@link Stream} to be read as an RDD
   * @param vClass      the value class
   * @param startTime   the starting time of the stream to be read
   * @param endTime     the ending time of the streams to be read
   * @param decoderType the decoder to use while reading streams
   * @return the RDD created from {@link Stream}
   */
  @Override
  public <T> T readFromStream(String streamName, Class<?> vClass, long startTime, long endTime,
                              Class<? extends StreamEventDecoder> decoderType) {
      Configuration hConf;
      try {
        if (decoderType == null) {
          hConf = setStreamInputDataset(new StreamBatchReadable(streamName, startTime, endTime), vClass);
        } else {
          hConf = setStreamInputDataset(new StreamBatchReadable(streamName, startTime, endTime, decoderType), vClass);
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to set input to specified stream: " + streamName);
      }
      return (T) originalSparkContext.newAPIHadoopFile(streamName, StreamInputFormat.class, LongWritable.class, vClass,
                                                       hConf);
  }

  private <T, K, V> void writeToDatasetHelper(T rdd, String datasetName, Class<K> kClass, Class<V> vClass) {
    Configuration hConf = setOutputDataset(datasetName);
    ClassTag<K> kClassTag = ClassTag$.MODULE$.apply(kClass);
    ClassTag<V> vClassTag = ClassTag$.MODULE$.apply(vClass);
    PairRDDFunctions pairRDD = new PairRDDFunctions<K, V>((RDD<Tuple2<K, V>>) rdd, kClassTag, vClassTag, null);
    pairRDD.saveAsNewAPIHadoopFile(datasetName, kClass, vClass, SparkDatasetOutputFormat.class, hConf);
  }

  /**
   * Return a {@link SparkContext} object
   *
   * @param <T> type of Apache Spark Context which is {@link SparkContext} here
   * @return the {@link SparkContext}
   */
  @Override
  public <T> T getOriginalSparkContext() {
    return (T) originalSparkContext;
  }
}
