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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import com.google.common.base.Preconditions;

/**
 * Transforms Rows into Records.
 */
public class RowRecordTransformer {
  private final Schema schema;
  private final Schema.Field rowField;

  public RowRecordTransformer(Schema schema, String rowFieldName) {
    validateSchema(schema);
    this.schema = schema;

    if (rowFieldName != null) {
      rowField = schema.getField(rowFieldName);
      // if row field was given, it must be present in the schema and it must be a simple type
      Preconditions.checkArgument(rowField != null, "Row field must be present in the schema.");
      Preconditions.checkArgument(rowField.getSchema().getType().isSimpleType(), "Row field must be a simple type.");
    } else {
      rowField = null;
    }
  }

  public StructuredRecord toRecord(Row row) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (rowField != null) {
      setField(builder, rowField, row.getRow());
    }

    for (Schema.Field field : schema.getFields()) {
      if (rowField != null && field.getName().equals(rowField.getName())) {
        continue;
      }
      setField(builder, field, row.get(field.getName()));
    }

    return builder.build();
  }

  // schema must be a record and must contain only simple types
  private void validateSchema(Schema schema) {
    Preconditions.checkArgument(schema.getType() == Schema.Type.RECORD, "Schema must be a record.");
    for (Schema.Field field : schema.getFields()) {
      Schema fieldSchema = field.getSchema();
      if (fieldSchema.isNullable()) {
        if (!fieldSchema.isNullableSimple()) {
          throw new IllegalArgumentException(
            String.format("Unsupported type %s for field %s.", fieldSchema.getType(), field.getName()));
        }
      } else {
        if (!fieldSchema.getType().isSimpleType()) {
          throw new IllegalArgumentException(
            String.format("Unsupported type %s for field %s.", fieldSchema.getType(), field.getName()));
        }
      }
    }
  }

  private void setField(StructuredRecord.Builder builder, Schema.Field field, byte[] fieldBytes) {
    String fieldName = field.getName();
    Schema fieldSchema = field.getSchema();
    if (fieldBytes == null) {
      if (!fieldSchema.isNullable()) {
        throw new IllegalArgumentException("null value found for non-nullable field " + fieldName);
      }
      return;
    }
    Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();

    switch (fieldType) {
      case BOOLEAN:
        builder.set(fieldName, Bytes.toBoolean(fieldBytes));
        break;
      case INT:
        builder.set(fieldName, Bytes.toInt(fieldBytes));
        break;
      case LONG:
        builder.set(fieldName, Bytes.toLong(fieldBytes));
        break;
      case FLOAT:
        builder.set(fieldName, Bytes.toFloat(fieldBytes));
        break;
      case DOUBLE:
        builder.set(fieldName, Bytes.toDouble(fieldBytes));
        break;
      case BYTES:
        builder.set(fieldName, fieldBytes);
        break;
      case STRING:
        builder.set(fieldName, Bytes.toString(fieldBytes));
        break;
      default:
        // shouldn't ever happen
        throw new IllegalArgumentException("Unsupported type " + fieldType + " for field " + fieldName);
    }
  }
}
