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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.Transform;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.LongWritable;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Transform {@link StructuredRecord} to {@link GenericRecord}
 */
public class StructuredRecordToGenericRecordTransform extends Transform<LongWritable, StructuredRecord,
                                                               LongWritable, GenericRecord> {


  private final Map<Integer, Schema> schemaCache = Maps.newHashMap();

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(StructuredRecordToGenericRecordTransform.class.getName());
    configurer.setDescription("Transforms a StructuredRecord to Avro format");
  }

  @Override
  public void transform(@Nullable LongWritable inputKey, StructuredRecord structuredRecord,
                        Emitter<LongWritable, GenericRecord> emitter) throws Exception {
    co.cask.cdap.api.data.schema.Schema structuredRecordSchema = structuredRecord.getSchema();

    int hashCode = structuredRecordSchema.hashCode();
    Schema avroSchema;

    if (schemaCache.containsKey(hashCode)) {
      avroSchema = schemaCache.get(hashCode);
    } else {
      avroSchema = new Schema.Parser().parse(structuredRecordSchema.toString());
      schemaCache.put(hashCode, avroSchema);
    }

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    for (Schema.Field field : avroSchema.getFields()) {
      String fieldName = field.name();
      recordBuilder.set(fieldName, structuredRecord.get(fieldName));
    }
    emitter.emit(inputKey, recordBuilder.build());
  }
}
