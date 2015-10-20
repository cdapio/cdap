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

package co.cask.cdap.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.common.ETLUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Base class to read from a {@link TimePartitionedFileSet}.
 *
 * @param <KEY> type of input key
 * @param <VALUE> type of input value
 */
public abstract class TimePartitionedFileSetSource<KEY, VALUE> extends BatchSource<KEY, VALUE, StructuredRecord> {

  private final TPFSConfig config;

  /**
   * Config for TimePartitionedFileSetDatasetAvroSource
   */
  public static class TPFSConfig extends PluginConfig {
    @Description("Name of the TimePartitionedFileSet to read.")
    private String name;

    @Description("Base path for the TimePartitionedFileSet. Defaults to the name of the dataset.")
    @Nullable
    private String basePath;

    @Description("Size of the time window to read with each run of the pipeline. " +
      "The format is expected to be a number followed by an 's', 'm', 'h', or 'd' specifying the time unit, with 's' " +
      "for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. For example, a value of '5m' means each run of " +
      "the pipeline will read 5 minutes of events from the TPFS source.")
    private String duration;

    @Description("Optional delay for reading from TPFS source. The value must be " +
      "of the same format as the duration value. For example, a duration of '5m' and a delay of '10m' means each run " +
      "of the pipeline will read 5 minutes of data from 15 minutes before its logical start time to 10 minutes " +
      "before its logical start time. The default value is 0.")
    @Nullable
    private String delay;

    protected void validate() {
      // check duration and delay
      long durationInMs = ETLUtils.parseDuration(duration);
      Preconditions.checkArgument(durationInMs > 0, "Duration must be greater than 0");
      if (!Strings.isNullOrEmpty(delay)) {
        ETLUtils.parseDuration(delay);
      }
    }
  }

  public TimePartitionedFileSetSource(TPFSConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate();
    String tpfsName = config.name;
    FileSetProperties.Builder properties = FileSetProperties.builder();
    if (!Strings.isNullOrEmpty(config.basePath)) {
      properties.setBasePath(config.basePath);
    }
    addFileSetProperties(properties);

    pipelineConfigurer.createDataset(tpfsName, TimePartitionedFileSet.class.getName(), properties.build());
  }

  /**
   * Set file set specific properties, such as input/output format and explore properties.
   */
  protected abstract void addFileSetProperties(FileSetProperties.Builder properties);

  protected void setInput(BatchSourceContext context) {
    Map<String, String> runtimeArgs = context.getRuntimeArguments();
    long runtime = context.getLogicalStartTime();
    if (runtimeArgs.containsKey("runtime")) {
      runtime = Long.parseLong(runtimeArgs.get("runtime"));
    }

    long duration = ETLUtils.parseDuration(config.duration);
    long delay = Strings.isNullOrEmpty(config.delay) ? 0 : ETLUtils.parseDuration(config.delay);
    long endTime = runtime - delay;
    long startTime = endTime - duration;
    Map<String, String> sourceArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setInputStartTime(sourceArgs, startTime);
    TimePartitionedFileSetArguments.setInputEndTime(sourceArgs, endTime);
    TimePartitionedFileSet source = context.getDataset(config.name, sourceArgs);
    context.setInput(config.name, source);
  }
}
