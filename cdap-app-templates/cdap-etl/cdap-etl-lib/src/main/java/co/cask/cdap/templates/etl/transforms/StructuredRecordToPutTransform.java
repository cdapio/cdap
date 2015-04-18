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
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.TransformContext;
import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

/**
 * Transforms a StructuredRecord into a Put object used to write to a Table. Each field in the record
 * will be added as a column in the Put. Only simple types are supported. The transform does not assume that
 * all input will have the same schema, but it does assume that there will always be a field present that
 * will be the row key for the table Put.
 */
public class StructuredRecordToPutTransform extends Transform<StructuredRecord, Put> {
  private static final String ROW_FIELD = "row.field";
  private String rowField;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(StructuredRecordToPutTransform.class.getSimpleName());
    configurer.setDescription("Transforms a StructuredRecord to a Put object used to write to a Table.");
    configurer.addProperty(new Property(ROW_FIELD, "the name of the record field that should be used as the row key" +
      " for the table put.", true));
  }

  @Override
  public void initialize(TransformContext context) {
    rowField = context.getRuntimeArguments().get(ROW_FIELD);
    Preconditions.checkArgument(rowField != null && !rowField.isEmpty(), "the row key must be given");
  }

  @Override
  public void transform(StructuredRecord record, Emitter<Put> emitter) throws Exception {
    Schema recordSchema = record.getSchema();
    Preconditions.checkArgument(recordSchema.getType() == Schema.Type.RECORD, "input must be a record.");
    Put output = createPut(record, recordSchema);

    for (Schema.Field field : recordSchema.getFields()) {
      if (field.getName().equals(rowField)) {
        continue;
      }
      setField(output, field, record.get(field.getName()));
    }
    emitter.emit(output);
  }

  @SuppressWarnings("ConstantConditions")
  private void setField(Put put, Schema.Field field, Object val) {
    // have to handle nulls differently. In a Put object, it's only valid to use the add(byte[], byte[])
    // for null values, as the other add methods take boolean vs Boolean, int vs Integer, etc.
    if (field.getSchema().isNullable() && val == null) {
      put.add(field.getName(), (byte[]) null);
      return;
    }
    Schema.Type type = validateAndGetType(field);

    switch (type) {
      case BOOLEAN:
        put.add(field.getName(), (Boolean) val);
        break;
      case INT:
        put.add(field.getName(), (Integer) val);
        break;
      case LONG:
        put.add(field.getName(), (Long) val);
        break;
      case FLOAT:
        put.add(field.getName(), (Float) val);
        break;
      case DOUBLE:
        put.add(field.getName(), (Double) val);
        break;
      case BYTES:
        if (val instanceof ByteBuffer) {
          put.add(field.getName(), Bytes.toBytes((ByteBuffer) val));
        } else {
          put.add(field.getName(), (byte[]) val);
        }
        break;
      case STRING:
        put.add(field.getName(), (String) val);
        break;
      default:
        throw new IllegalArgumentException("Field " + field.getName() + " is of unsupported type " + type);
    }
  }

  // get the non-nullable type of the field and check that it's a simple type.
  private Schema.Type validateAndGetType(Schema.Field field) {
    Schema.Type type;
    if (field.getSchema().isNullable()) {
      type = field.getSchema().getNonNullable().getType();
    } else {
      type = field.getSchema().getType();
    }
    Preconditions.checkArgument(type.isSimpleType(),
                                "only simple types are supported (boolean, int, long, float, double, bytes).");
    return type;
  }

  private Put createPut(StructuredRecord record, Schema recordSchema) {
    Schema.Field keyField = recordSchema.getField(rowField);
    Object val = record.get(keyField.getName());
    Preconditions.checkArgument(val != null, "Row key cannot be null.");

    Schema.Type keyType = validateAndGetType(keyField);
    switch (keyType) {
      case BOOLEAN:
        return new Put(Bytes.toBytes((Boolean) record.get(rowField)));
      case INT:
        return new Put(Bytes.toBytes((Integer) record.get(rowField)));
      case LONG:
        return new Put(Bytes.toBytes((Long) record.get(rowField)));
      case FLOAT:
        return new Put(Bytes.toBytes((Float) record.get(rowField)));
      case DOUBLE:
        return new Put(Bytes.toBytes((Double) record.get(rowField)));
      case BYTES:
        Object bytes = record.get(rowField);
        if (bytes instanceof ByteBuffer) {
          return new Put(Bytes.toBytes((ByteBuffer) bytes));
        } else {
          return new Put((byte[]) bytes);
        }
      case STRING:
        return new Put(Bytes.toBytes((String) record.get(rowField)));
      default:
        throw new IllegalArgumentException("Row key is of unsupported type " + keyType);
    }
  }
}
