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

package co.cask.cdap.templates.etl.lib.sinks.realtime;

import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.templates.etl.api.SinkContext;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.stages.AbstractRealtimeSink;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class StreamSink extends AbstractRealtimeSink<StreamEventData> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSink.class);

  private String stream;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("Stream Sink");
    configurer.setDescription("");
    configurer.setReqdProperties(Sets.newHashSet("streamName"));
  }

  @Override
  public void initialize(SinkContext context) {
    super.initialize(context);
    this.stream = context.getRuntimeArguments().get("streamName");
  }

  @Override
  public void write(StreamEventData object) {
    try {
      getContext().write(stream, object);
    } catch (IOException e) {
      LOG.error("Stream Sink failed to write to Stream : {}", e.getMessage(), e);
    }
  }
}
