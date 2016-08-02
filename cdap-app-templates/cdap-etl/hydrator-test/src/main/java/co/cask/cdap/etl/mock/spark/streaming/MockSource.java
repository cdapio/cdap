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

package co.cask.cdap.etl.mock.spark.streaming;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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

    public Conf() {
      intervalMillis = 0L;
    }
  }

  public static ETLPlugin getPlugin(Schema schema, List<StructuredRecord> records) throws IOException {
    return getPlugin(schema, records, 0L);
  }

  public static ETLPlugin getPlugin(Schema schema, List<StructuredRecord> records,
                                    Long intervalMillis) throws IOException {
    List<String> recordsStrs = new ArrayList<>(records.size());
    for (StructuredRecord record : records) {
      recordsStrs.add(StructuredRecordStringConverter.toJsonString(record));
    }
    return new ETLPlugin("Mock", StreamingSource.PLUGIN_TYPE,
                         ImmutableMap.of("schema", schema.toString(),
                                         "records", GSON.toJson(recordsStrs),
                                         "intervalMillis", intervalMillis.toString()),
                         null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("schema", new PluginPropertyField("schema", "", "string", true, false));
    properties.put("records", new PluginPropertyField("records", "", "string", true, false));
    properties.put("intervalMillis", new PluginPropertyField("intervalMillis", "", "long", false, false));
    return new PluginClass(StreamingSource.PLUGIN_TYPE, "Mock", "", MockSource.class.getName(), "conf", properties);
  }
}
