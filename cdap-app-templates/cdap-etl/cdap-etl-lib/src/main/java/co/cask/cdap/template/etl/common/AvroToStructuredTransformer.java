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

import java.io.IOException;
import java.util.Map;

/**
 * Create StructuredRecords from GenericRecords
 */
public class AvroToStructuredTransformer extends RecordConverter<GenericRecord, StructuredRecord> {

  private final Map<Integer, co.cask.cdap.api.data.schema.Schema> schemaCache = Maps.newHashMap();

  @Override
  public StructuredRecord transform(GenericRecord genericRecord) throws IOException {
    Schema genericRecordSchema = genericRecord.getSchema();

    int hashCode = genericRecordSchema.hashCode();
    co.cask.cdap.api.data.schema.Schema structuredSchema;

    if (schemaCache.containsKey(hashCode)) {
      structuredSchema = schemaCache.get(hashCode);
    } else {
      structuredSchema = co.cask.cdap.api.data.schema.Schema.parseJson(genericRecordSchema.toString());
      schemaCache.put(hashCode, structuredSchema);
    }

    StructuredRecord.Builder builder = StructuredRecord.builder(structuredSchema);
    for (Schema.Field field : genericRecordSchema.getFields()) {
      String fieldName = field.name();
      builder.set(fieldName, convertField(genericRecord.get(fieldName), field.schema()));
    }

    return builder.build();
  }
}
