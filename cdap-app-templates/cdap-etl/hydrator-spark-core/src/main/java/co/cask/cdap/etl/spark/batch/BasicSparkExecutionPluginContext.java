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

package co.cask.cdap.etl.spark.batch;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.common.AbstractTransformContext;
import co.cask.cdap.etl.planner.StageInfo;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of {@link SparkExecutionPluginContext} by delegating to {@link JavaSparkExecutionContext}.
 */
public class BasicSparkExecutionPluginContext extends AbstractTransformContext implements SparkExecutionPluginContext {

  private final JavaSparkExecutionContext sec;
  private final JavaSparkContext jsc;
  private final DatasetContext datasetContext;

  public BasicSparkExecutionPluginContext(JavaSparkExecutionContext sec,
                                          JavaSparkContext jsc, DatasetContext datasetContext,
                                          StageInfo stageInfo) {
    super(sec.getPluginContext(), sec.getServiceDiscoverer(), sec.getMetrics(), null, stageInfo);
    this.sec = sec;
    this.jsc = jsc;
    this.datasetContext = datasetContext;
  }

  @Override
  public long getLogicalStartTime() {
    return sec.getLogicalStartTime();
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return sec.getRuntimeArguments();
  }

  @Override
  public <K, V> JavaPairRDD<K, V> fromDataset(String datasetName) {
    return sec.fromDataset(datasetName);
  }

  @Override
  public <K, V> JavaPairRDD<K, V> fromDataset(String datasetName, Map<String, String> arguments) {
    return sec.fromDataset(datasetName, arguments);
  }

  @Override
  public <K, V> JavaPairRDD<K, V> fromDataset(String datasetName, Map<String, String> arguments,
                                              @Nullable Iterable<? extends Split> splits) {
    return sec.fromDataset(datasetName, arguments, splits);
  }

  @Override
  public JavaRDD<StreamEvent> fromStream(String streamName) {
    return sec.fromStream(streamName);
  }

  @Override
  public JavaRDD<StreamEvent> fromStream(String streamName, long startTime, long endTime) {
    return sec.fromStream(streamName, startTime, endTime);
  }

  @Override
  public <V> JavaPairRDD<Long, V> fromStream(String streamName, Class<V> valueType) {
    return sec.fromStream(streamName, valueType);
  }

  @Override
  public <V> JavaPairRDD<Long, V> fromStream(String streamName, long startTime, long endTime, Class<V> valueType) {
    return sec.fromStream(streamName, startTime, endTime, valueType);
  }

  @Override
  public <K, V> JavaPairRDD<K, V> fromStream(String streamName, long startTime, long endTime,
                                             Class<? extends StreamEventDecoder<K, V>> decoderClass,
                                             Class<K> keyType, Class<V> valueType) {
    return sec.fromStream(streamName, startTime, endTime, decoderClass, keyType, valueType);
  }

  @Override
  public <K, V> void saveAsDataset(JavaPairRDD<K, V> rdd, String datasetName) {
    sec.saveAsDataset(rdd, datasetName);
  }

  @Override
  public <K, V> void saveAsDataset(JavaPairRDD<K, V> rdd, String datasetName, Map<String, String> arguments) {
    sec.saveAsDataset(rdd, datasetName, arguments);
  }

  @Override
  public JavaSparkContext getSparkContext() {
    return jsc;
  }

  @Override
  public PluginContext getPluginContext() {
    return sec.getPluginContext();
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return datasetContext.getDataset(name);
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name) throws DatasetInstantiationException {
    return datasetContext.getDataset(namespace, name);
  }

  @Override
  public <T extends Dataset> T getDataset(String name,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    return datasetContext.getDataset(name, arguments);
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    return datasetContext.getDataset(namespace, name, arguments);
  }

  @Override
  public void releaseDataset(Dataset dataset) {
    datasetContext.releaseDataset(dataset);
  }

  @Override
  public void discardDataset(Dataset dataset) {
    datasetContext.discardDataset(dataset);
  }

  @Override
  public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
    throw new UnsupportedOperationException("Lookup not supported in Spark");
  }
}
