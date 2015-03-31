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
import formats.RecordFormats;
import org.apache.hadoop.io.LongWritable;

import javax.annotation.Nullable;

/**
 * Transforms {@link StreamEvent} to Avro format
 */
public class StreamConversionTransform extends Transform<LongWritable, StreamEvent, LongWritable, StructuredRecord> {
  /**
   * Configure the Transform stage. Used to provide information about the Transform.
   *
   * @param configurer {@link StageConfigurer}
   */
  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(StreamConversionTransform.class.getName());
    configurer.setDescription("Transforms a StreamEvent from the specified stream to Avro format which can be " +
                                "written to a TimePartitionedFileset");
    configurer.addProperty(new Property("schemaType", "Type of the Schema", true));
    configurer.addProperty(new Property("schema", "The schema of the stream events", true));
  }

  /**
   * Process input Key and Value and emit output using {@link Emitter}.
   *
   * @param inputKey input key, can be null if key is not available/applicable
   * @param streamEvent input value
   * @param emitter {@link Emitter} to emit data to the next stage
   * @throws Exception
   */
  @Override
  public void transform(@Nullable final LongWritable inputKey, StreamEvent streamEvent,
                        final Emitter<LongWritable, StructuredRecord> emitter) throws Exception {
    Schema bodySchema = Schema.parseJson(getContext().getRuntimeArguments().get("schema"));
    RecordFormat<StreamEvent, StructuredRecord> format = RecordFormats.createInitializedFormat(
      new FormatSpecification(getContext().getRuntimeArguments().get("schemaType"), bodySchema,
                              ImmutableMap.<String, String>of()));
    StructuredRecord record = format.read(streamEvent);
    emitter.emit(inputKey, record);
  }
}
