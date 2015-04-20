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

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.stream.GenericStreamEventData;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.batch.BatchSourceContext;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.common.ETLUtils;
import co.cask.cdap.templates.etl.common.Properties;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A {@link BatchSource} for {@link Stream} to use {@link Stream} as Source.
 */
public class StreamBatchSource extends BatchSource<LongWritable, Object, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(StreamBatchSource.class);
  private static final String FORMAT_SETTING_PREFIX = "format.setting.";
  private static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
    Schema.Field.of("body", Schema.of(Schema.Type.BYTES))
  );
  private Config config;
  // its possible the input records could have different schemas, though that isn't the case today.
  private Map<Schema, Schema> schemaCache = Maps.newHashMap();

  public void configure(StageConfigurer configurer) {
    configurer.setName(StreamBatchSource.class.getSimpleName());
    configurer.setDescription("Batch source for a stream. If a format is given, any property prefixed with " +
      "'format.setting.' will be passed to the format. For example, if a property with key " +
      "'format.setting.delimiter' and value '|' is given, the setting 'delimiter' with value '|' " +
      "will be passed to the format.");
    configurer.addProperty(new Property(Properties.Stream.NAME, "Name of the stream.", true));
    configurer.addProperty(new Property(
      Properties.Stream.DURATION,
      "Size of the time window to read with each run of the pipeline. The format is expected to be a number followed " +
        "by a 's', 'm', 'h', or 'd' specifying the time unit, with 's' for seconds, 'm' for minutes, 'h' for hours, " +
        "and 'd' for days. For example, a value of '5m' means each run of the pipeline will read 5 minutes of events " +
        "from the stream.",
      true));
    configurer.addProperty(new Property(
      Properties.Stream.DELAY,
      "Optional delay for reading stream events. The value must be of the same format as the duration value. " +
        "For example, a duration of '5m' and a delay of '10m' means each run of the pipeline will read events from " +
        "15 minutes before its logical start time to 10 minutes before its logical start time. The default value is 0.",
      false));
    configurer.addProperty(new Property(
      Properties.Stream.FORMAT,
      "Optional format of the stream. Any format supported by CDAP is also supported. For example, a value of 'csv' " +
        "will attempt to parse stream events as comma separated values. If no format is given, event " +
        "bodies will be treated as bytes, resulting in a three field schema: 'ts' of type long, " +
        "'headers' of type map of string to string, and 'body' of type bytes.",
      false));
    configurer.addProperty(new Property(
      Properties.Stream.SCHEMA,
      "Optional schema for the body of stream events. Schema is used in conjunction with format " +
        "to parse stream events. Some formats like the avro format require schema, while others do not. " +
        "The schema given is for the body of the stream, so the final schema of records output by the source will " +
        "contain an additional field named 'ts' for the timestamp and a field named 'headers' for the headers as " +
        "as the first and second fields of the schema.",
      false));
  }

  @Override
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    new Config(stageConfig.getProperties());
  }

  @Override
  public void prepareJob(BatchSourceContext context) {
    config = new Config(context.getRuntimeArguments());

    long endTime = context.getLogicalStartTime() - config.delay;
    long startTime = endTime - config.duration;

    LOG.info("Setting input to Stream : {}", config.name);

    StreamBatchReadable stream = config.formatSpec == null ? new StreamBatchReadable(config.name, startTime, endTime) :
      new StreamBatchReadable(config.name, startTime, endTime, config.formatSpec);
    context.setInput(stream);
  }

  @Override
  public void initialize(ETLStage stageConfig) throws Exception {
    config = new Config(stageConfig.getProperties());
  }

  @Override
  public void transform(KeyValue<LongWritable, Object> input, Emitter<StructuredRecord> emitter) throws Exception {
    // if not format spec was given, the value is a StreamEvent
    if (config.formatSpec == null) {
      StreamEvent event = (StreamEvent) input.getValue();
      Map<String, String> headers = Objects.firstNonNull(event.getHeaders(), ImmutableMap.<String, String>of());
      StructuredRecord output = StructuredRecord.builder(DEFAULT_SCHEMA)
        .set("ts", input.getKey().get())
        .set("headers", headers)
        .set("body", event.getBody())
        .build();
      emitter.emit(output);
    } else {
      // otherwise, it will be a GenericStreamEventData
      GenericStreamEventData<StructuredRecord> event = (GenericStreamEventData<StructuredRecord>) input.getValue();
      StructuredRecord record = event.getBody();
      Schema inputSchema = record.getSchema();
      Schema outputSchema = schemaCache.get(inputSchema);
      // if we haven't seen this schema before, generate the output schema (add ts and header fields)
      if (outputSchema == null) {
        List<Schema.Field> fields = Lists.newArrayList();
        fields.add(DEFAULT_SCHEMA.getField("ts"));
        fields.add(DEFAULT_SCHEMA.getField("headers"));
        fields.addAll(inputSchema.getFields());
        outputSchema = Schema.recordOf(inputSchema.getRecordName(), fields);
        schemaCache.put(inputSchema, outputSchema);
      }

      // easier to just deal with an empty map than deal with nullables, so the headers field is non-nullable.
      Map<String, String> headers = Objects.firstNonNull(event.getHeaders(), ImmutableMap.<String, String>of());
      StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
      builder.set("ts", input.getKey().get());
      builder.set("headers", headers);

      for (Schema.Field field : inputSchema.getFields()) {
        String fieldName = field.getName();
        builder.set(fieldName, record.get(fieldName));
      }
      emitter.emit(builder.build());
    }
  }

  private class Config {
    private final String name;
    private final long duration;
    private final long delay;
    private final FormatSpecification formatSpec;

    private Config(Map<String, String> properties) {
      name = properties.get(Properties.Stream.NAME);
      String durationStr = properties.get(Properties.Stream.DURATION);
      Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Stream name must be given.");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(durationStr), "Duration must be given.");

      duration = ETLUtils.parseDuration(properties.get(Properties.Stream.DURATION));
      String delayStr = properties.get(Properties.Stream.DELAY);
      delay = Strings.isNullOrEmpty(delayStr) ? 0 : ETLUtils.parseDuration(delayStr);


      String formatName = properties.get(Properties.Stream.FORMAT);
      if (!Strings.isNullOrEmpty(formatName)) {
        // try to parse the schema if there is one
        String schemaStr = properties.get(Properties.Stream.SCHEMA);
        Schema schema;
        try {
          schema = schemaStr == null ? null : Schema.parseJson(schemaStr);
        } catch (IOException e) {
          throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
        }

        // strip format.settings. from any properties and use them in the format spec
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          if (entry.getKey().startsWith(FORMAT_SETTING_PREFIX)) {
            String key = entry.getKey();
            builder.put(key.substring(FORMAT_SETTING_PREFIX.length(), key.length()), entry.getValue());
          }
        }

        formatSpec = new FormatSpecification(formatName, schema, builder.build());
      } else {
        formatSpec = null;
      }
    }
  }
}
