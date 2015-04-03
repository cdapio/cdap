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

package co.cask.cdap.templates.etl.lib.transforms;


import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.Transform;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import formats.RecordFormats;
import org.apache.hadoop.io.LongWritable;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Transforms {@link StreamEvent} to {@link StructuredRecord}
 */
public class StreamToStructuredRecordTransform extends Transform<LongWritable, StreamEvent,
                                                                                      LongWritable, StructuredRecord> {
  private static final String SCHEMA = "schema";
  private static final String SCHEMA_TYPE = "schemaType";
  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(StreamToStructuredRecordTransform.class.getName());
    configurer.setDescription("Transforms a StreamEvent to StructuredRecord.");
    configurer.addProperty(new Property(SCHEMA, "Type of the Schema ex: CSV, TSV etc.", true));
    configurer.addProperty(new Property(SCHEMA_TYPE, "The schema of the body of stream events", true));
  }

  @Override
  public void transform(@Nullable final LongWritable inputKey, StreamEvent streamEvent,
                        final Emitter<LongWritable, StructuredRecord> emitter) throws Exception {
    // read the stream event body as a StructuredRecord
    Schema streamBodySchema = Schema.parseJson(getContext().getRuntimeArguments().get(SCHEMA));
    RecordFormat<StreamEvent, StructuredRecord> format = RecordFormats.createInitializedFormat(
      new FormatSpecification(getContext().getRuntimeArguments().get(SCHEMA_TYPE), streamBodySchema,
                              ImmutableMap.<String, String>of()));
    StructuredRecord streamEventRecord = format.read(streamEvent);

    // create the new schema with timestamp and headers to reatin them in StructuredRecord
    List<Schema.Field> fields = Lists.newArrayList(
      Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))));
    fields.addAll(streamBodySchema.getFields());
    Schema streamSchema = Schema.recordOf("streamEvent", fields);

    // build the structured record with timestamp, headers and copying all the fields from stream event
    StructuredRecord.Builder builder = StructuredRecord.builder(streamSchema);
    builder.set("ts", streamEvent.getTimestamp());
    builder.set("headers", streamEvent.getHeaders());
    for (Schema.Field field : streamBodySchema.getFields()) {
      String fieldName = field.getName();
      builder.set(fieldName, streamEventRecord.get(fieldName));
    }

    emitter.emit(inputKey, builder.build());
  }
}
