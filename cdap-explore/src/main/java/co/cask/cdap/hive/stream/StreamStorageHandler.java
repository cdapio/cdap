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

package co.cask.cdap.hive.stream;

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
 * HiveStorageHandler to access Streams.
 */
public class StreamStorageHandler extends DefaultStorageHandler {
  private static final Logger LOG = LoggerFactory.getLogger(StreamStorageHandler.class);

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveStreamInputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return StreamSerDe.class;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    configureTableJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc,
                                          Map<String, String> jobProperties) {
    // NOTE: the jobProperties map will be put in the jobConf passed to the StreamInputFormat.
    // Hive ensures that the properties of the right table will be passed at the right time to those classes.
    String streamName = tableDesc.getProperties().getProperty(Constants.Explore.STREAM_NAME);
    jobProperties.put(Constants.Explore.STREAM_NAME, streamName);
    LOG.debug("Got stream {} for external table {}", streamName, tableDesc.getTableName());
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc,
                                           Map<String, String> jobProperties) {
    // throw the exception here instead of in getOutputFormatClass because that method is called on table creation.
    throw new UnsupportedOperationException("Writing to streams through Hive is not supported");
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return null;
  }
}

