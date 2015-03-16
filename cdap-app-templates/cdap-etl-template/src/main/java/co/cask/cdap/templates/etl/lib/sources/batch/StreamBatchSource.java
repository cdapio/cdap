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

package co.cask.cdap.templates.etl.lib.sources.batch;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.stream.GenericStreamEventData;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.realtime.Emitter;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StreamBatchSource implements BatchSource<LongWritable, GenericStreamEventData<StructuredRecord>, String> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamBatchSource.class);

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("StreamBatchSource");
    configurer.setDescription("CDAP Streams Batch Source");
  }

  @Override
  public void prepareJob(MapReduceContext context) {
    String streamName = context.getRuntimeArguments().get("streamName");
    LOG.info("Setting input to Stream : {}", streamName);
    Schema schema = Schema.recordOf("streamEvent", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
    StreamBatchReadable.useStreamInput(context, streamName, 0L, Long.MAX_VALUE,
                                       new FormatSpecification("text", schema, Maps.<String, String>newHashMap()));
  }

  @Override
  public void initialize(MapReduceContext context) {
    // no-op (but basically initializing anything that is required for the batch source)
  }

  @Override
  public void read(LongWritable ts, GenericStreamEventData<StructuredRecord> text, Emitter<String> data) {
    data.emit((String) text.getBody().get("body"));
  }

  @Override
  public void destroy() {
    //no-op
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    LOG.info("MR Source complete : {}", succeeded);
  }
}
