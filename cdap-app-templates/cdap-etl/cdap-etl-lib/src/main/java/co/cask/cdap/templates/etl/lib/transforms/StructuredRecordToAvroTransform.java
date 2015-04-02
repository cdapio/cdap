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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.conversion.avro.Converter;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.Transform;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import javax.annotation.Nullable;

/**
 * Transform for converting a StructuredRecord to Avro format
 */
public class StructuredRecordToAvroTransform extends Transform<LongWritable, StructuredRecord,
  AvroKey<GenericRecord>, NullWritable> {

  /**
   * Configure the Transform stage. Used to provide information about the Transform.
   *
   * @param configurer {@link StageConfigurer}
   */
  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(StructuredRecordToAvroTransform.class.getName());
    configurer.setDescription("Transforms a StructuredRecord to Avro format");
    configurer.addProperty(new Property("schemaType", "Type of the Schema", true));
    configurer.addProperty(new Property("schema", "The schema of the Structured record events", true));
  }

  @Override
  public void transform(@Nullable final LongWritable inputKey, StructuredRecord structuredRecord,
                        final Emitter<AvroKey<GenericRecord>, NullWritable> emitter) throws Exception {
    Schema schema = new Schema.Parser().parse(getContext().getRuntimeArguments().get("schema"));
    String headersStr = getContext().getRuntimeArguments().get("headers");
    String[] headers = headersStr == null ? new String[0] : headersStr.split(",");
    Converter converter = new Converter(schema, headers);
    GenericRecord record = converter.convert(structuredRecord, inputKey.get(), ImmutableMap.<String, String>of());
    emitter.emit(new AvroKey<GenericRecord>(record), NullWritable.get());
  }
}
