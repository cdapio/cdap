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

package $package;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.realtime.DataWriter;
import co.cask.cdap.template.etl.api.realtime.RealtimeContext;
import co.cask.cdap.template.etl.api.realtime.RealtimeSink;


/**
 * Real-time sink.
 */
@Plugin(type = "sink")
@Name("Sink")
@Description("Real-time sink")
public class Sink extends RealtimeSink<StructuredRecord> {
  private final SinkConfig config;

  public Sink(SinkConfig config) {
    this.config = config;
  }

  /**
   * Config class for Sink.
   */
  public static class SinkConfig extends PluginConfig {

    @Description("Sink name")
    private String name;

  }

    @Override
  public int write(Iterable<StructuredRecord> objects, DataWriter dataWriter) throws Exception {
    // Write objects to sink
    int count = 0;
    for (StructuredRecord object : objects) {
     count++;
     // Write to output
     // dataWriter.getDataset("").write();
     // dataWriter.write("stream", data);
    }
    return count;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
     // Create any sink that is required. Datasets/Streams
    // pipelineConfigurer.addStream(new Stream(name));
    // pipelineConfigurer.createDataset(name, type, properties);
  }



  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    // No-Op do intitialize
  }

  @Override
  public void destroy() {
    super.destroy();
    // No-op do cleanup
  }
}


