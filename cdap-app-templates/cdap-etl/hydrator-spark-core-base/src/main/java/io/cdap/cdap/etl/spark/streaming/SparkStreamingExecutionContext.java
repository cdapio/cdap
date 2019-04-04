/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.streaming;

import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.dynamic.SparkInterpreter;
import io.cdap.cdap.etl.api.Lookup;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.common.AbstractTransformContext;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.NoLookupProvider;
import io.cdap.cdap.etl.spark.SparkPipelineRuntime;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of {@link SparkExecutionPluginContext} by delegating to {@link JavaSparkExecutionContext}.
 */
public class SparkStreamingExecutionContext extends AbstractTransformContext implements SparkExecutionPluginContext {

  private final JavaSparkExecutionContext sec;
  private final JavaSparkContext jsc;
  private final long batchTime;

  public SparkStreamingExecutionContext(JavaSparkExecutionContext sec, JavaSparkContext jsc,
                                        long batchTime, StageSpec stageSpec) {
    super(new SparkPipelineRuntime(sec, batchTime), stageSpec, NoLookupProvider.INSTANCE);
    this.sec = sec;
    this.jsc = jsc;
    this.batchTime = batchTime;
  }

  @Override
  public long getLogicalStartTime() {
    return batchTime;
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
  public SparkInterpreter createSparkInterpreter() throws IOException {
    return sec.createInterpreter();
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    throw new UnsupportedOperationException("Not supported in Spark Streaming.");
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name) throws DatasetInstantiationException {
    throw new UnsupportedOperationException("Not supported in Spark Streaming.");
  }

  @Override
  public <T extends Dataset> T getDataset(String name,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    throw new UnsupportedOperationException("Not supported in Spark Streaming.");
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    throw new UnsupportedOperationException("Not supported in Spark Streaming.");
  }

  @Override
  public void releaseDataset(Dataset dataset) {
    throw new UnsupportedOperationException("Not supported in Spark Streaming.");
  }

  @Override
  public void discardDataset(Dataset dataset) {
    throw new UnsupportedOperationException("Not supported in Spark Streaming.");
  }

  @Override
  public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
    throw new UnsupportedOperationException("Not supported in Spark Streaming.");
  }

  @Override
  public void record(List<FieldOperation> operations) {
    throw new UnsupportedOperationException("Not supported in Spark Streaming.");
  }
}
