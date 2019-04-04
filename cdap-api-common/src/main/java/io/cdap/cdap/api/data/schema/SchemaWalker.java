/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.api.data.schema;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;

/**
 * Utility to walk a schema recursively in depth-first order.
 */
public final class SchemaWalker {

  private SchemaWalker() { }

  /**
   * Walk a schema recursively in depth-first order.
   * The consumer is called on every sub-schema in the schema.
   *
   * @param schema the schema to walk
   * @param consumer a consumer for all sub-schemas, called with the field name (if any)
   *                 and the schema at each node in the schema
   */
  public static void walk(Schema schema, BiConsumer<String, Schema> consumer) {
    walk(schema.getRecordName(), schema, new HashSet<>(), consumer);
  }

  private static void walk(@Nullable String fieldName, Schema schema,
                           Set<String> knownRecords, BiConsumer<String, Schema> consumer) {
    consumer.accept(fieldName, schema);
    switch (schema.getType()) {
      case NULL:
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BYTES:
      case STRING:
      case ENUM:
        break; // done with recursion
      case ARRAY:
        walk(schema.getComponentSchema().getRecordName(), schema.getComponentSchema(), knownRecords, consumer);
        break;
      case MAP:
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        walk(mapSchema.getKey().getRecordName(), mapSchema.getKey(), knownRecords, consumer);
        walk(mapSchema.getValue().getRecordName(), mapSchema.getValue(), knownRecords, consumer);
        break;
      case RECORD:
        // because a schema may be cyclic:
        // make sure to only descend recursively if this record has not been seen yet
        if (knownRecords.add(schema.getRecordName())) {
          for (Schema.Field field : schema.getFields()) {
            walk(field.getName(), field.getSchema(), knownRecords, consumer);
          }
        }
        break;
      case UNION:
        for (Schema unionSchema : schema.getUnionSchemas()) {
          walk(unionSchema.getRecordName(), unionSchema, knownRecords, consumer);
        }
        break;
      default:
        throw new IllegalStateException("Unknown schema type " + schema.getType() + " for schema " + schema);
    }
  }
}
