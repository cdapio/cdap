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

package io.cdap.cdap.internal.app.runtime.batch.dataset;

import io.cdap.cdap.api.data.batch.BatchWritable;
import io.cdap.cdap.api.data.batch.DatasetOutputCommitter;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.dataset.DataSetException;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.common.conf.ConfigurationUtil;
import io.cdap.cdap.internal.app.runtime.batch.MapReduceBatchWritableOutputFormat;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * A {@link OutputFormatProvider} that provides information for batch job to write to {@link
 * Dataset}.
 */
public class DatasetOutputFormatProvider implements OutputFormatProvider, DatasetOutputCommitter {

  private final String outputFormatClassName;
  private final Map<String, String> configuration;
  private final Dataset dataset;

  public DatasetOutputFormatProvider(String namespace, String datasetName,
      Map<String, String> datasetArgs, Dataset dataset) {
    if (dataset instanceof OutputFormatProvider) {
      this.outputFormatClassName = ((OutputFormatProvider) dataset).getOutputFormatClassName();
      this.configuration = ((OutputFormatProvider) dataset).getOutputFormatConfiguration();
    } else if (dataset instanceof BatchWritable) {
      this.outputFormatClassName = MapReduceBatchWritableOutputFormat.class.getName();
      this.configuration = createDatasetConfiguration(namespace, datasetName, datasetArgs);
    } else {
      throw new IllegalArgumentException("Dataset '" + dataset
          + "' is neither OutputFormatProvider nor BatchWritable.");
    }
    this.dataset = dataset;
  }

  @Override
  public String getOutputFormatClassName() {
    return outputFormatClassName;
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return configuration;
  }

  private Map<String, String> createDatasetConfiguration(String namespace, String datasetName,
      Map<String, String> datasetArgs) {
    Configuration hConf = new Configuration();
    hConf.clear();
    AbstractBatchWritableOutputFormat.setDataset(hConf, namespace, datasetName, datasetArgs);
    return ConfigurationUtil.toMap(hConf);
  }

  @Override
  public void onSuccess() throws DataSetException {
    if (dataset instanceof DatasetOutputCommitter) {
      ((DatasetOutputCommitter) dataset).onSuccess();
    }
  }

  @Override
  public void onFailure() throws DataSetException {
    if (dataset instanceof DatasetOutputCommitter) {
      ((DatasetOutputCommitter) dataset).onFailure();
    }
  }
}
