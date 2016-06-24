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

package co.cask.cdap.dq.testclasses;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.stream.GenericStreamEventData;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
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
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} for {@link Stream} to use {@link Stream} as Source.
 */
@SuppressWarnings("unused")
@Plugin(type = "batchsource")
@Name("Stream")
@Description("Batch source for a stream.")
public class StreamBatchSource extends BatchSource<LongWritable, Object, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(StreamBatchSource.class);
  private static final String FORMAT_SETTING_PREFIX = "format.setting.";
  private static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
    Schema.Field.of("body", Schema.of(Schema.Type.BYTES))
  );
  private static final String NAME_DESCRIPTION = "Name of the stream. Must be a valid stream name. " +
    "If it doesn't exist, it will be created.";
  private static final String DURATION_DESCRIPTION = "Size of the time window to read with each run of the pipeline. " +
    "The format is expected to be a number followed by an 's', 'm', 'h', or 'd' specifying the time unit, with 's' " +
    "for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. For example, a value of '5m' means each run of " +
    "the pipeline will read 5 minutes of events from the stream.";
  private static final String DELAY_DESCRIPTION = "Optional delay for reading stream events. The value must be " +
    "of the same format as the duration value. For example, a duration of '5m' and a delay of '10m' means each run " +
    "of the pipeline will read events from 15 minutes before its logical start time to 10 minutes before its " +
    "logical start time. The default value is 0.";
  private static final String FORMAT_DESCRIPTION = "Optional format of the stream. Any format supported by CDAP " +
    "is also supported. For example, a value of 'csv' will attempt to parse stream events as comma separated values. " +
    "If no format is given, event bodies will be treated as bytes, resulting in a three field schema: " +
    "'ts' of type long, 'headers' of type map of string to string, and 'body' of type bytes.";
  private static final String SCHEMA_DESCRIPTION = "Optional schema for the body of stream events. Schema is used " +
    "in conjunction with format to parse stream events. Some formats like the avro format require schema, " +
    "while others do not. The schema given is for the body of the stream, so the final schema of records output " +
    "by the source will contain an additional field named 'ts' for the timestamp and a field named 'headers' " +
    "for the headers as as the first and second fields of the schema.";

  private StreamBatchConfig streamBatchConfig;

  // its possible the input records could have different schemas, though that isn't the case today.
  private Map<Schema, Schema> schemaCache = Maps.newHashMap();

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    streamBatchConfig.validate();
    pipelineConfigurer.addStream(new Stream(streamBatchConfig.name));
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    long duration = ETLUtils.parseDuration(streamBatchConfig.duration);
    long delay = Strings.isNullOrEmpty(streamBatchConfig.delay) ? 0 : ETLUtils.parseDuration(streamBatchConfig.delay);
    long endTime = context.getLogicalStartTime() - delay;
    long startTime = endTime - duration;

    LOG.info("Setting input to Stream : {}", streamBatchConfig.name);

    FormatSpecification formatSpec = streamBatchConfig.getFormatSpec();

    Input stream;
    if (formatSpec == null) {
      stream = Input.ofStream(streamBatchConfig.name, startTime, endTime);
    } else {
      stream = Input.ofStream(streamBatchConfig.name, startTime, endTime, formatSpec);
    }
    context.setInput(stream);
  }

  @Override
  public void transform(KeyValue<LongWritable, Object> input, Emitter<StructuredRecord> emitter) throws Exception {
    // if not format spec was given, the value is a StreamEvent
    if (Strings.isNullOrEmpty(streamBatchConfig.format)) {
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
      @SuppressWarnings("unchecked")
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

  /**
   * {@link PluginConfig} class for {@link StreamBatchSource}
   */
  public static class StreamBatchConfig extends PluginConfig {

    @Description(NAME_DESCRIPTION)
    private String name;

    @Description(DURATION_DESCRIPTION)
    private String duration;

    @Description(DELAY_DESCRIPTION)
    @Nullable
    private String delay;

    @Description(FORMAT_DESCRIPTION)
    @Nullable
    private String format;

    @Description(SCHEMA_DESCRIPTION)
    @Nullable
    private String schema;

    private void validate() {
      // check the schema if there is one
      if (!Strings.isNullOrEmpty(schema)) {
        parseSchema();
      }
      // check duration and delay
      long durationInMs = ETLUtils.parseDuration(duration);
      Preconditions.checkArgument(durationInMs > 0, "Duration must be greater than 0");
      if (!Strings.isNullOrEmpty(delay)) {
        ETLUtils.parseDuration(delay);
      }
    }

    private FormatSpecification getFormatSpec() {
      FormatSpecification formatSpec = null;
      if (!Strings.isNullOrEmpty(format)) {
        // try to parse the schema if there is one
        Schema schemaObj = parseSchema();

        // strip format.settings. from any properties and use them in the format spec
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : getProperties().getProperties().entrySet()) {
          if (entry.getKey().startsWith(FORMAT_SETTING_PREFIX)) {
            String key = entry.getKey();
            builder.put(key.substring(FORMAT_SETTING_PREFIX.length(), key.length()), entry.getValue());
          }
        }
        formatSpec = new FormatSpecification(format, schemaObj, builder.build());
      }
      return formatSpec;
    }

    private Schema parseSchema() {
      try {
        return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
      }
    }
  }
}
