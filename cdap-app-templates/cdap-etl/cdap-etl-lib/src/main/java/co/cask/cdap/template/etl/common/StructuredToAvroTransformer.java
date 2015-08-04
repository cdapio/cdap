
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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Creates GenericRecords from StructuredRecords
 */
public class StructuredToAvroTransformer extends RecordConverter<StructuredRecord, GenericRecord> {

  private final Map<Integer, Schema> schemaCache;
  private final Schema outputAvroSchema;

  public StructuredToAvroTransformer(String outputSchema) {
    this.schemaCache = Maps.newHashMap();
    this.outputAvroSchema = (outputSchema != null) ? new Schema.Parser().parse(outputSchema) : null;
  }

  public GenericRecord transform(StructuredRecord structuredRecord) throws IOException {
    co.cask.cdap.api.data.schema.Schema structuredRecordSchema = structuredRecord.getSchema();

    int hashCode = structuredRecordSchema.hashCode();
    Schema avroSchema;

    if (outputAvroSchema != null) {
      avroSchema = outputAvroSchema;
    } else if (schemaCache.containsKey(hashCode)) {
      avroSchema = schemaCache.get(hashCode);
    } else {
      avroSchema = new Schema.Parser().parse(structuredRecordSchema.toString());
      schemaCache.put(hashCode, avroSchema);
    }

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    for (Schema.Field field : avroSchema.getFields()) {
      String fieldName = field.name();
      recordBuilder.set(fieldName, convertField(structuredRecord.get(fieldName), field.schema()));
    }
    return recordBuilder.build();
  }
}
