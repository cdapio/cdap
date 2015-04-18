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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.TransformContext;
import com.google.common.base.Preconditions;

/**
 * Transforms {@link Row} to {@link StructuredRecord}
 */
public class RowToStructuredRecordTransform extends Transform<Row, StructuredRecord> {
  private static final String SCHEMA = "schema";
  private static final String ROW_FIELD = "row.field";
  private Schema schema;
  private Schema.Field rowField;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(RowToStructuredRecordTransform.class.getSimpleName());
    configurer.setDescription("Transforms a Row to a StructuredRecord.");
    configurer.addProperty(new Property(
      SCHEMA,
      "The schema of the record. Row columns map to record fields. For example, if the schema contains a field named " +
        "'user' of type string, the value of that field will be taken from the value stored in the 'user' column. " +
        "Only simple types are allowed (boolean, int, long, float, double, bytes, string).",
      true));
    configurer.addProperty(new Property(
      ROW_FIELD,
      "Optional field name indicating that the field value should come from the row key instead of a row column. " +
        "The field name specified must be present in the schema, and must not be nullable.",
      false));
  }

  @Override
  public void initialize(TransformContext context) {
    String schemaStr = context.getRuntimeArguments().get(SCHEMA);
    Preconditions.checkArgument(schemaStr != null && !schemaStr.isEmpty(), "Schema must be specified.");
    try {
      schema = Schema.parseJson(schemaStr);
    } catch (Exception e) {
      throw new IllegalArgumentException("Schema is invalid", e);
    }
    validateSchema(schema);

    String rowFieldName = context.getRuntimeArguments().get(ROW_FIELD);
    if (rowFieldName != null) {
      rowField = schema.getField(rowFieldName);
      // if row field was given, it must be present in the schema and it must be a simple type
      Preconditions.checkArgument(rowField != null, "Row field must be present in the schema.");
      Preconditions.checkArgument(rowField.getSchema().getType().isSimpleType(), "Row field must be a simple type.");
    }
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

  @Override
  public void transform(Row row, Emitter<StructuredRecord> emitter) throws Exception {
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

    emitter.emit(builder.build());
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
