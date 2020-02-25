/*
 * Copyright © 2016 Cask Data, Inc.
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
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.lineage.field.FieldReadOperation;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Mock source for spark streaming unit tests.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("Mock")
public class MockSource extends StreamingSource<StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private static final Gson GSON = new Gson();
  private static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() { }.getType();

  private final Conf conf;

  public MockSource(Conf conf) {
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
  public void prepareRun(StreamingSourceContext context) throws Exception {
    Schema schema = Schema.parseJson(conf.schema);

    if (conf.referenceName != null && conf.schema != null) {
      context.registerLineage(conf.referenceName, schema);
      Schema outputSchema = Schema.parseJson(conf.schema);
      if (outputSchema.getFields() != null) {
        outputSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList())
          .forEach(field -> context.record(Collections.singletonList(
            new FieldReadOperation("Read " + field, "Read from mock source",
                                   EndPoint.of(context.getNamespace(), conf.referenceName), field))));
      }
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    Schema schema = Schema.parseJson(conf.schema);
    List<String> recordsAsStrings = new Gson().fromJson(conf.records, STRING_LIST_TYPE);
    final List<StructuredRecord> inputRecords = new ArrayList<>();
    for (String recordStr : recordsAsStrings) {
      inputRecords.add(StructuredRecordStringConverter.fromJsonString(recordStr, schema));
    }

    JavaStreamingContext jsc = context.getSparkStreamingContext();
    return jsc.receiverStream(new Receiver<StructuredRecord>(StorageLevel.MEMORY_ONLY()) {
      @Override
      public StorageLevel storageLevel() {
        return StorageLevel.MEMORY_ONLY();
      }

      @Override
      public void onStart() {
        new Thread() {

          @Override
          public void run() {
            for (StructuredRecord record : inputRecords) {
              if (isStarted()) {
                store(record);
                try {
                  TimeUnit.MILLISECONDS.sleep(conf.intervalMillis);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            }
          }

          @Override
          public void interrupt() {
            super.interrupt();
          }
        }.start();
      }

      @Override
      public void onStop() {

      }
    });
  }

  /**
   * Config for mock source.
   */
  public static class Conf extends PluginConfig {
    private String schema;
    private String records;
    @Nullable
    private Long intervalMillis;
    @Nullable
    private String referenceName;

    public Conf() {
      intervalMillis = 0L;
    }
  }

  public static ETLPlugin getPlugin(Schema schema, List<StructuredRecord> records) throws IOException {
    return getPlugin(schema, records, 0L);
  }

  public static ETLPlugin getPlugin(Schema schema, List<StructuredRecord> records,
                                    Long intervalMillis) throws IOException {
    return getPlugin(schema, records, intervalMillis, null);
  }

  public static ETLPlugin getPlugin(Schema schema, List<StructuredRecord> records,
                                    Long intervalMillis, @Nullable String referenceName) throws IOException {
    List<String> recordsStrs = new ArrayList<>(records.size());
    for (StructuredRecord record : records) {
      recordsStrs.add(StructuredRecordStringConverter.toJsonString(record));
    }

    ImmutableMap.Builder<String, String> builder =
      ImmutableMap.<String, String>builder()
        .put("schema", schema.toString())
        .put("records", GSON.toJson(recordsStrs))
        .put("intervalMillis", intervalMillis.toString());

    if (referenceName != null) {
      builder.put("referenceName", referenceName);
    }
    return new ETLPlugin("Mock", StreamingSource.PLUGIN_TYPE,
                         builder.build(), null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("schema", new PluginPropertyField("schema", "", "string", true, false));
    properties.put("records", new PluginPropertyField("records", "", "string", true, false));
    properties.put("intervalMillis", new PluginPropertyField("intervalMillis", "", "long", false, false));
    properties.put("referenceName", new PluginPropertyField("referenceName", "", "string", false, false));
    return new PluginClass(StreamingSource.PLUGIN_TYPE, "Mock", "", MockSource.class.getName(), "conf", properties);
  }
}
