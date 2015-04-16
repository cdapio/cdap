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

package co.cask.cdap.templates.etl.batch.sources;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.batch.BatchSourceContext;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.common.ETLUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BatchSource} for {@link Stream} to use {@link Stream} as Source.
 */
public class StreamBatchSource extends BatchSource<LongWritable, StreamEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(StreamBatchSource.class);
  public static final String STREAM_NAME = "streamName";

  public void configure(StageConfigurer configurer) {
    configurer.setName(StreamBatchSource.class.getSimpleName());
    configurer.setDescription("Use Stream as Source");
    configurer.addProperty(new Property("streamName", "Name of the stream to use as Source", true));
    configurer.addProperty(new Property("frequency", "Frequency of the schedule", false));
  }

  @Override
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    String streamName = stageConfig.getProperties().get(STREAM_NAME);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(streamName), "Stream name must be given.");
    pipelineConfigurer.addStream(new Stream(streamName));
  }

  @Override
  public void prepareJob(BatchSourceContext context) {
    long endTime = context.getLogicalStartTime();
    //TODO: Once the method to get the frequency from the schedule is added change it to use that. Then we will not
    // need the frequency as a configuration here (depends on to CDAP-2048)
    long startTime = endTime - ETLUtils.parseFrequency(context.getRuntimeArguments().get("frequency"));

    String streamName = context.getRuntimeArguments().get(STREAM_NAME);
    LOG.info("Setting input to Stream : {}", streamName);
    Schema schema = Schema.recordOf("streamEvent", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

    // TODO: This is not clean.
    context.setInput(new StreamBatchReadable(streamName, startTime, endTime).toURI().toString());
  }
}
