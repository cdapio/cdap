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

package co.cask.cdap.etl.batch.sink;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.HashMap;
import java.util.Map;

/**
 * TPFS Batch Sink class that stores sink data
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class TimePartitionedFileSetSink<KEY_OUT, VAL_OUT>
  extends BatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {

  protected static final String TPFS_NAME_DESC = "Name of the Time Partitioned FileSet Dataset to which the records " +
    "are written to. If it doesn't exist, it will be created.";
  protected static final String BASE_PATH_DESC = "The base path for the time partitioned fileset. Defaults to the " +
    "name of the dataset.";
  protected static final String PATH_FORMAT_DESC = "The format for the path; for example: " +
    "'yyyy-MM-dd/HH-mm' will create a file path ending in the format of 2015-01-01/20-42. " +
    "The string provided will be provided to SimpleDataFormat. " +
    "If left blank, then the partitions will be of the form 2015-01-01/20-42.142017372000. " +
    "Note that each partition must have a unique file path or a runtime exception will be thrown.";
  protected static final String TIME_ZONE_DESC = "The time zone to format the partition. " +
    "This option is only used if pathFormat is set. If blank or an invalid TimeZone ID, defaults to UTC. " +
    "Note that the time zone provided must be recognized by TimeZone.getTimeZone(String); " +
    "for example: \"America/Los_Angeles\"";

  protected final TPFSSinkConfig tpfsSinkConfig;

  protected TimePartitionedFileSetSink(TPFSSinkConfig tpfsSinkConfig) {
    this.tpfsSinkConfig = tpfsSinkConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tpfsSinkConfig.filePathFormat)
                                  || Strings.isNullOrEmpty(tpfsSinkConfig.timeZone),
                                "Set the filePathFormat to set the timezone");
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Map<String, String> sinkArgs = getAdditionalTPFSArguments();
    TimePartitionedFileSetArguments.setOutputPartitionTime(sinkArgs, context.getLogicalStartTime());
    if (!Strings.isNullOrEmpty(tpfsSinkConfig.filePathFormat)) {
      TimePartitionedFileSetArguments.setOutputPathFormat(sinkArgs, tpfsSinkConfig.filePathFormat,
                                                          tpfsSinkConfig.timeZone);
    }
    context.addOutput(tpfsSinkConfig.name, sinkArgs);
  }

  /**
   * @return any additional properties that need to be set for the sink. For example, avro sink requires
   *         setting some schema output key.
   */
  protected Map<String, String> getAdditionalTPFSArguments() {
    return new HashMap<>();
  }
}
