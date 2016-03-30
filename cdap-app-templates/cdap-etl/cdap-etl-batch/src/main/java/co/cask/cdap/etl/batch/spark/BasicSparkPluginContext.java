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

package co.cask.cdap.etl.batch.spark;

import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Map;

/**
 * Implementation of SparkPluginContext that delegates to a SparkContext.
 */
public class BasicSparkPluginContext extends AbstractSparkBatchContext implements SparkPluginContext {

  private final SparkContext sparkContext;

  public BasicSparkPluginContext(SparkContext sparkContext, LookupProvider lookupProvider, String stageId) {
    super(sparkContext, lookupProvider, stageId);
    this.sparkContext = sparkContext;
  }

  @Override
  public <V> JavaPairRDD<LongWritable, V> readFromStream(StreamBatchReadable stream, Class<? extends V> vClass) {
    return sparkContext.readFromStream(stream, vClass);
  }

  @Override
  public <V> JavaPairRDD<LongWritable, V> readFromStream(String streamName, Class<? extends V> vClass,
                                                         long startTime, long endTime,
                                                         Class<? extends StreamEventDecoder> decoderType) {
    return sparkContext.readFromStream(streamName, vClass, startTime, endTime, decoderType);
  }

  @Override
  public <K, V> JavaPairRDD<K, V> readFromStream(String streamName, Class<? extends V> vClass,
                                                 long startTime, long endTime) {
    return sparkContext.readFromStream(streamName, vClass, startTime, endTime);
  }

  @Override
  public <V> JavaPairRDD<LongWritable, V> readFromStream(String streamName, Class<? extends V> vClass) {
    return sparkContext.readFromStream(streamName, vClass);
  }

  @Override
  public <K, V> void writeToDataset(JavaPairRDD<K, V> rdd, String datasetName,
                                    Class<? extends K> kClass, Class<? extends V> vClass,
                                    Map<String, String> datasetArgs) {
    sparkContext.writeToDataset(rdd, datasetName, kClass, vClass, datasetArgs);
  }

  @Override
  public <K, V> void writeToDataset(JavaPairRDD<K, V> rdd, String datasetName,
                                    Class<? extends K> kClass, Class<? extends V> vClass) {
    sparkContext.writeToDataset(rdd, datasetName, kClass, vClass);
  }

  @Override
  public <K, V> JavaPairRDD<K, V> readFromDataset(String datasetName,
                                                  Class<? extends K> kClass, Class<? extends V> vClass,
                                                  Map<String, String> datasetArgs) {
    return sparkContext.readFromDataset(datasetName, kClass, vClass, datasetArgs);
  }

  @Override
  public <K, V> JavaPairRDD<K, V> readFromDataset(String datasetName,
                                                  Class<? extends K> kClass, Class<? extends V> vClass) {
    return sparkContext.readFromDataset(datasetName, kClass, vClass);
  }

  @Override
  public <T> T getOriginalSparkContext() {
    return sparkContext.getOriginalSparkContext();
  }

  @Override
  public PluginContext getPluginContext() {
    return sparkContext.getPluginContext();
  }

  @Override
  public <T> void setSparkConf(T sparkConf) {
    sparkContext.setSparkConf(sparkConf);
  }
}
