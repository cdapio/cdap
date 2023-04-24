/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.etl.mock.spark.streaming;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingEventHandler;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingStateHandler;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * Mock streaming source that uses a DStream that implements {@link StreamingEventHandler}
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("MockStreamingEventSource")
public class MockStreamingEventSource extends StreamingSource<StructuredRecord>
    implements StreamingStateHandler {

  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private static final Gson GSON = new Gson();
  private static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() {
  }.getType();
  public static final String MOCK_STREAMING_EVENT_SOURCE_COMPLETED =
      "MockStreamingEventSource-completed";
  public static final String MOCK_STREAMING_EVENT_SOURCE_STARTED =
      "MockStreamingEventSource-started";
  public static final String MOCK_STREAMING_EVENT_SOURCE_RETRIED =
      "MockStreamingEventSource-retried";

  private static StreamingEventHandlerTracker tracker = new StreamingEventHandlerTracker();

  private final Conf conf;

  public MockStreamingEventSource(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    try {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(conf.schema));
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not parse schema " + conf.schema);
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    Schema schema = Schema.parseJson(conf.schema);
    List<String> recordsAsStrings = new Gson().fromJson(conf.records, STRING_LIST_TYPE);
    return new JavaDStream<>(
        new MockStreamingEventHandlerDStream(context.getSparkStreamingContext().ssc(),
            recordsAsStrings, schema, tracker),
        scala.reflect.ClassTag$.MODULE$.apply(String.class));
  }

  /**
   * Config for mock source.
   */
  private static class Conf extends PluginConfig {

    private String schema;
    private String records;

    public Conf() {
    }
  }

  public static ETLPlugin getPlugin(Schema schema, List<StructuredRecord> records)
      throws IOException {
    List<String> recordsStrs = new ArrayList<>(records.size());
    for (StructuredRecord record : records) {
      recordsStrs.add(StructuredRecordStringConverter.toJsonString(record));
    }

    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .put("schema", schema.toString())
            .put("records", GSON.toJson(recordsStrs));

    return new ETLPlugin("MockStreamingEventSource", StreamingSource.PLUGIN_TYPE,
        builder.build(), null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("schema", new PluginPropertyField("schema", "", "string", true, false));
    properties.put("records", new PluginPropertyField("records", "", "string", true, false));
    return PluginClass.builder().setName("MockStreamingEventSource")
        .setType(StreamingSource.PLUGIN_TYPE)
        .setDescription("").setClassName(MockStreamingEventSource.class.getName())
        .setProperties(properties)
        .setConfigFieldName("conf").build();
  }

  /**
   *  Streaming event handler that saves state on each event.
   */
  private static class StreamingEventHandlerTracker implements StreamingEventHandler, Serializable {

    @Override
    public void onBatchCompleted(StreamingContext streamingContext) {
      try {
        streamingContext.saveState(MOCK_STREAMING_EVENT_SOURCE_COMPLETED, "true".getBytes(
            StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onBatchStarted(StreamingContext streamingContext) {
      try {
        streamingContext.saveState(MOCK_STREAMING_EVENT_SOURCE_STARTED, "true".getBytes(
            StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onBatchRetry(StreamingContext streamingContext) {
      try {
        streamingContext.saveState(MOCK_STREAMING_EVENT_SOURCE_RETRIED, "true".getBytes(
            StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
