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
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import java.util.Map;

/**
 * Creates StructuredRecords from GenericRecords , with caching for schemas.
 */
public class AvroToStructuredTransformer {
  private final Map<Integer, Schema> schemaCache = Maps.newHashMap();

  public StructuredRecord transform(GenericRecord genericRecord) throws Exception {
    org.apache.avro.Schema genericRecordSchema = genericRecord.getSchema();

    int hashCode = genericRecordSchema.hashCode();
    Schema structuredSchema;

    if (schemaCache.containsKey(hashCode)) {
      structuredSchema = schemaCache.get(hashCode);
    } else {
      structuredSchema = Schema.parseJson(genericRecordSchema.toString());
      schemaCache.put(hashCode, structuredSchema);
    }

    StructuredRecord.Builder builder = StructuredRecord.builder(structuredSchema);
    for (Schema.Field field: structuredSchema.getFields()) {
      String fieldName = field.getName();
      builder.set(fieldName, genericRecord.get(fieldName));
    }
    return builder.build();
  }
}
