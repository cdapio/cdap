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

package co.cask.cdap.internal.app.runtime.batch.dataset;

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.common.conf.ConfigurationUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link InputFormatProvider} that provides {@link InputFormat} for read through Dataset.
 */
public class DatasetInputFormatProvider implements InputFormatProvider {

  private final String datasetName;
  private final Map<String, String> datasetArgs;
  private final Dataset dataset;
  private final List<Split> splits;
  private final Class<? extends AbstractBatchReadableInputFormat> batchReadableInputFormat;

  public DatasetInputFormatProvider(String datasetName, Map<String, String> datasetArgs,
                                    Dataset dataset, @Nullable List<Split> splits,
                                    Class<? extends AbstractBatchReadableInputFormat> batchReadableInputFormat) {
    this.datasetName = datasetName;
    this.datasetArgs = ImmutableMap.copyOf(datasetArgs);
    this.dataset = dataset;
    this.splits = splits == null ? null : ImmutableList.copyOf(splits);
    this.batchReadableInputFormat = batchReadableInputFormat;
  }

  @Override
  public String getInputFormatClassName() {
    return dataset instanceof InputFormatProvider
      ? ((InputFormatProvider) dataset).getInputFormatClassName()
      : batchReadableInputFormat.getName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    if (dataset instanceof InputFormatProvider) {
      return ((InputFormatProvider) dataset).getInputFormatConfiguration();
    }
    return createBatchReadableConfiguration();
  }

  private Map<String, String> createBatchReadableConfiguration() {
    List<Split> splits = this.splits;
    if (splits == null) {
      splits = ((BatchReadable<?, ?>) dataset).getSplits();
    }
    Configuration hConf = new Configuration();
    hConf.clear();

    try {
      AbstractBatchReadableInputFormat.setDatasetSplits(hConf, datasetName, datasetArgs, splits);
      return ConfigurationUtil.toMap(hConf);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
