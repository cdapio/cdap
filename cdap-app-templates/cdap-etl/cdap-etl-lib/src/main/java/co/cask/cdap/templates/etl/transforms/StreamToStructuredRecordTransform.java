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

package co.cask.cdap.templates.etl.transforms;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.TransformContext;
import co.cask.cdap.templates.etl.transforms.formats.RecordFormats;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Transforms {@link StreamEvent} to {@link StructuredRecord}
 */
public class StreamToStructuredRecordTransform extends Transform<LongWritable, StreamEvent,
                                                                 LongWritable, StructuredRecord> {
  private static final String SCHEMA = "schema";
  private static final String FORMAT_NAME = "format.name";
  private static final String FORMAT_SETTING_PREFIX = "format.setting.";

  private static SchemaWrapper schemaWrapper = null;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(StreamToStructuredRecordTransform.class.getName());
    configurer.setDescription("Transforms a StreamEvent to StructuredRecord.");
    configurer.addProperty(new Property(SCHEMA, "The schema of the body of stream events", true));
    configurer.addProperty(new Property(FORMAT_NAME, "Format name: CSV, TSV etc.", true));
  }

  @Override
  public void initialize(TransformContext context) {
    super.initialize(context);
    try {
      Schema streamBodySchema = Schema.parseJson(getContext().getRuntimeArguments().get(SCHEMA));
      schemaWrapper = getSchemaWrapper(streamBodySchema);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void transform(@Nullable LongWritable inputKey, StreamEvent streamEvent,
                        Emitter<LongWritable, StructuredRecord> emitter) throws Exception {

    StructuredRecord streamEventRecord = schemaWrapper.format.read(streamEvent);

    // build the structured record with timestamp, headers and copying all the fields from stream event
    StructuredRecord.Builder builder = StructuredRecord.builder(schemaWrapper.streamSchema);
    builder.set("ts", streamEvent.getTimestamp());
    builder.set("headers", streamEvent.getHeaders());

    for (Schema.Field field : schemaWrapper.streamBodySchema.getFields()) {
      String fieldName = field.getName();
      builder.set(fieldName, streamEventRecord.get(fieldName));
    }

    emitter.emit(inputKey, builder.build());
  }

  private SchemaWrapper getSchemaWrapper(Schema streamBodySchema) throws Exception {
    // create the new schema with timestamp and headers to reatin them in StructuredRecord
    List<Schema.Field> fields = Lists.newArrayList(
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));
    fields.addAll(streamBodySchema.getFields());

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : getContext().getRuntimeArguments().entrySet()) {
      if (entry.getKey().startsWith(FORMAT_SETTING_PREFIX)) {
        String key = entry.getKey().replace(FORMAT_SETTING_PREFIX, "");
        builder.put(key, entry.getValue());
      }
    }

    RecordFormat<StreamEvent, StructuredRecord> format = RecordFormats.createInitializedFormat(
      new FormatSpecification(getContext().getRuntimeArguments().get(FORMAT_NAME), streamBodySchema,
                              builder.build()));

    return new SchemaWrapper(Schema.recordOf("streamEvent", fields), streamBodySchema, format);
  }

  // Wrapper to store the streambody schema and the stream schema
  private static class SchemaWrapper {
    private final Schema streamSchema;
    private final Schema streamBodySchema;
    private final RecordFormat<StreamEvent, StructuredRecord> format;

    private SchemaWrapper(Schema streamSchema, Schema streamBodySchema,
                          RecordFormat<StreamEvent, StructuredRecord> format) {
      this.streamSchema = streamSchema;
      this.streamBodySchema = streamBodySchema;
      this.format = format;
    }
  }

}
