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

package co.cask.cdap.hive.datasets;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.hive.context.CConfCodec;
import co.cask.cdap.hive.context.ConfigurationUtil;
import com.google.common.base.Throwables;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * HiveStorageHandler to access Datasets.
 */
public class DatasetStorageHandler extends DefaultStorageHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetStorageHandler.class);

  @SuppressWarnings("unchecked")
  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return DatasetInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    // Even if writes are not allowed, we must return an output format because it is used during table creation.
    if (writesEnabled()) {
      return DatasetOutputFormat.class;
    } else {
      return SequenceFileOutputFormat.class;
    }
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return DatasetSerDe.class;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    configureTableJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    if (writesEnabled()) {
      configureTableJobProperties(tableDesc, jobProperties);
    } else {
      throw new UnsupportedOperationException("Writing to datasets through Hive is not enabled.");
    }
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc,
                                          Map<String, String> jobProperties) {
    // NOTE: the jobProperties map will be put in the jobConf passed to the DatasetOutputFormat/DatasetInputFormat.
    // Hive ensures that the properties of the right table will be passed at the right time to those classes.
    String datasetName = tableDesc.getProperties().getProperty(Constants.Explore.DATASET_NAME);
    jobProperties.put(Constants.Explore.DATASET_NAME, datasetName);
    LOG.debug("Got dataset {} for external table {}", datasetName, tableDesc.getTableName());
  }

  private boolean writesEnabled() {
    try {
      CConfiguration cConf = ConfigurationUtil.get(getConf(), Constants.Explore.CCONF_KEY, CConfCodec.INSTANCE);
      return cConf.getBoolean(Constants.Explore.WRITES_ENABLED);
    } catch (IOException e) {
      LOG.error("Unable to get CDAP Configuration to check if writes are enabled.", e);
      throw Throwables.propagate(e);
    }
  }
}

