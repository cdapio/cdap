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
import co.cask.cdap.api.data.stream.StreamBatchReadable;

import java.util.Map;
import java.util.Set;

/**
 * Stores all the information of {@link SparkBatchSourceFactory}
 */
public class SparkBatchSourceFactoryInfo {

  private final Map<String, StreamBatchReadable> streamBatchReadables;
  private final Map<String, InputFormatProvider> inputFormatProviders;
  private final Map<String, DatasetInfo> datasetInfos;
  private final Map<String, Set<String>> sourceInputs;

  public SparkBatchSourceFactoryInfo(Map<String, StreamBatchReadable> streamBatchReadables,
                                     Map<String, InputFormatProvider> inputFormatProviders,
                                     Map<String, DatasetInfo> datasetInfos, Map<String, Set<String>> sourceInputs) {
    this.streamBatchReadables = streamBatchReadables;
    this.inputFormatProviders = inputFormatProviders;
    this.datasetInfos = datasetInfos;
    this.sourceInputs = sourceInputs;
  }

  public Map<String, StreamBatchReadable> getStreamBatchReadables() {
    return streamBatchReadables;
  }

  public Map<String, InputFormatProvider> getInputFormatProviders() {
    return inputFormatProviders;
  }

  public Map<String, DatasetInfo> getDatasetInfos() {
    return datasetInfos;
  }

  public Map<String, Set<String>> getSourceInputs() {
    return sourceInputs;
  }
}
