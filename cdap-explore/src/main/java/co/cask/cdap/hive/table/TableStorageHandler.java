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

package co.cask.cdap.hive.table;

import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * HiveStorageHandler to access a {@link Table}.
 */
public class TableStorageHandler extends DefaultStorageHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TableStorageHandler.class);

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveTableInputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return TableSerDe.class;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    configureTableJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc,
                                          Map<String, String> jobProperties) {
    // NOTE: the jobProperties map will be put in the jobConf passed to the HiveTableInputFormat.
    // Hive ensures that the properties of the right table will be passed at the right time to those classes.
    String tableName = tableDesc.getProperties().getProperty(Constants.Explore.TABLE_DATASET_NAME);
    jobProperties.put(Constants.Explore.TABLE_DATASET_NAME, tableName);
    LOG.debug("Got CDAP table {} for external table {}", tableName, tableDesc.getTableName());
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc,
                                           Map<String, String> jobProperties) {
    // throw the exception here instead of in getOutputFormatClass because that method is called on table creation.
    throw new UnsupportedOperationException("Writing to CDAP core table datasets through Hive is not supported");
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return null;
  }
}

