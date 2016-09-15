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

import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.stream.StreamBatchReadable;

import java.util.Map;
import java.util.Set;

/**
 * Stores all the information of {@link SparkBatchSinkFactory} and stagePartitions
 */
public class SparkBatchSourceSinkFactoryInfo {
  private final Map<String, StreamBatchReadable> streamBatchReadables;
  private final Map<String, InputFormatProvider> inputFormatProviders;
  private final Map<String, DatasetInfo> sourceDatasetInfos;
  private final Map<String, Set<String>> sourceInputs;

  private final Map<String, OutputFormatProvider> outputFormatProviders;
  private final Map<String, DatasetInfo> sinkDatasetInfos;
  private final Map<String, Set<String>> sinkOutputs;
  private final Map<String, Integer> stagePartitions;

  public SparkBatchSourceSinkFactoryInfo(SparkBatchSourceFactory sparkBatchSourceFactory,
                                         SparkBatchSinkFactory sparkBatchSinkFactory,
                                         Map<String, Integer> stagePartitions) {
    this.streamBatchReadables = sparkBatchSourceFactory.getStreamBatchReadables();
    this.inputFormatProviders = sparkBatchSourceFactory.getInputFormatProviders();
    this.sourceDatasetInfos = sparkBatchSourceFactory.getDatasetInfos();
    this.sourceInputs = sparkBatchSourceFactory.getSourceInputs();

    this.outputFormatProviders = sparkBatchSinkFactory.getOutputFormatProviders();
    this.sinkDatasetInfos = sparkBatchSinkFactory.getDatasetInfos();
    this.sinkOutputs = sparkBatchSinkFactory.getSinkOutputs();
    this.stagePartitions = stagePartitions;
  }

  public Map<String, StreamBatchReadable> getStreamBatchReadables() {
    return streamBatchReadables;
  }

  public Map<String, InputFormatProvider> getInputFormatProviders() {
    return inputFormatProviders;
  }

  public Map<String, DatasetInfo> getSourceDatasetInfos() {
    return sourceDatasetInfos;
  }

  public Map<String, Set<String>> getSourceInputs() {
    return sourceInputs;
  }

  public Map<String, OutputFormatProvider> getOutputFormatProviders() {
    return outputFormatProviders;
  }

  public Map<String, DatasetInfo> getSinkDatasetInfos() {
    return sinkDatasetInfos;
  }

  public Map<String, Set<String>> getSinkOutputs() {
    return sinkOutputs;
  }

  public Map<String, Integer> getStagePartitions() {
    return stagePartitions;
  }
}
