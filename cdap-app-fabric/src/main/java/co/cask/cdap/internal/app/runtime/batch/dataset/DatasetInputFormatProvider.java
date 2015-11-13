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

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.common.conf.ConfigurationUtil;
import com.google.common.base.Throwables;
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
  private final List<Split> splits;
  private final DatasetContext datasetContext;

  public DatasetInputFormatProvider(String datasetName, Map<String, String> datasetArgs,
                                    @Nullable List<Split> splits, DatasetContext datasetContext) {
    this.datasetName = datasetName;
    this.datasetArgs = ImmutableMap.copyOf(datasetArgs);
    this.splits = splits == null ? null : ImmutableList.copyOf(splits);
    this.datasetContext = datasetContext;
  }

  @Override
  public String getInputFormatClassName() {
    Dataset dataset = datasetContext.getDataset(datasetName, datasetArgs);

    if (dataset instanceof InputFormatProvider) {
      return ((InputFormatProvider) dataset).getInputFormatClassName();
    }
    if (dataset instanceof BatchReadable) {
      return DataSetInputFormat.class.getName();
    }
    throw new IllegalArgumentException("Dataset '" + dataset + "' is neither InputFormatProvider or BatchReadable.");
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    Dataset dataset = datasetContext.getDataset(datasetName, datasetArgs);

    if (dataset instanceof InputFormatProvider) {
      return ((InputFormatProvider) dataset).getInputFormatConfiguration();
    }
    if (dataset instanceof BatchReadable) {
      List<Split> splits = this.splits;
      if (splits == null) {
        splits = ((BatchReadable) dataset).getSplits();
      }
      Configuration hConf = new Configuration();
      hConf.clear();

      try {
        DataSetInputFormat.setDatasetSplits(hConf, datasetName, datasetArgs, splits);
        return ConfigurationUtil.toMap(hConf);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    throw new IllegalArgumentException("Dataset '" + dataset + "' is neither InputFormatProvider or BatchReadable.");
  }
}
