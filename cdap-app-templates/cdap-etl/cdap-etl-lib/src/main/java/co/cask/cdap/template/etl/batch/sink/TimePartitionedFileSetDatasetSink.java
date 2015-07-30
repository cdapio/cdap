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

package co.cask.cdap.template.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import com.google.common.collect.Maps;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * TPFS Batch Sink class that stores sink data
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class TimePartitionedFileSetDatasetSink<KEY_OUT, VAL_OUT>
  extends BatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {

  private static final String SCHEMA_DESC = "The Parquet schema of the record being written to the Sink as a JSON " +
    "Object.";
  private static final String TPFS_NAME_DESC = "Name of the Time Partitioned FileSet Dataset to which the records " +
    "have to be written. If it doesn't exist, it will be created.";
  private static final String BASE_PATH_DESC = "The base path for the time partitioned fileset. Defaults to the " +
    "name of the dataset.";

  protected final TPFSSinkConfig tpfsSinkConfig;

  protected TimePartitionedFileSetDatasetSink(TPFSSinkConfig tpfsSinkConfig) {
    this.tpfsSinkConfig = tpfsSinkConfig;
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Map<String, String> sinkArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setOutputPartitionTime(sinkArgs, context.getLogicalStartTime());
    TimePartitionedFileSet sink = context.getDataset(tpfsSinkConfig.name, sinkArgs);
    context.setOutput(tpfsSinkConfig.name, sink);
  }

  /**
   * Config for TimePartitionedFileSetDatasetSink
   */
  public static class TPFSSinkConfig extends PluginConfig {

    @Description(TPFS_NAME_DESC)
    protected String name;

    @Description(SCHEMA_DESC)
    protected String schema;

    @Description(BASE_PATH_DESC)
    @Nullable
    protected String basePath;
  }
}
