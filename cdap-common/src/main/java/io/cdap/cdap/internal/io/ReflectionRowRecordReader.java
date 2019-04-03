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

package co.cask.cdap.internal.io;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Decodes an object from a {@link Row} object fetched from a {@link Table} into a {@link StructuredRecord}.
 * The schema may contain a field for the row key, which must be a non-null simple type. The name of the
 * row field must be given if the schema contains it.
 */
public class ReflectionRowRecordReader extends ReflectionRowReader<StructuredRecord> {
  // these are used since we know the type or the row key in the constructor,
  // and we don't want to have a big switch statement each time we read a row.
  private static final Map<Schema.Type, RowKeyFunction> ROW_KEY_FUNCTIONS =
    ImmutableMap.<Schema.Type, RowKeyFunction>builder()
      .put(Schema.Type.BOOLEAN, new RowKeyFunction<Boolean>() {
        @Override
        public Boolean convert(byte[] rowKey) {
          return Bytes.toBoolean(rowKey);
        }
      })
      .put(Schema.Type.BYTES, new RowKeyFunction<byte[]>() {
        @Override
        public byte[] convert(byte[] rowKey) {
          return rowKey;
        }
      })
      .put(Schema.Type.INT, new RowKeyFunction<Integer>() {
        @Override
        public Integer convert(byte[] rowKey) {
          return Bytes.toInt(rowKey);
        }
      })
      .put(Schema.Type.LONG, new RowKeyFunction<Long>() {
        @Override
        public Long convert(byte[] rowKey) {
          return Bytes.toLong(rowKey);
        }
      })
      .put(Schema.Type.FLOAT, new RowKeyFunction<Float>() {
        @Override
        public Float convert(byte[] rowKey) {
          return Bytes.toFloat(rowKey);
        }
      })
      .put(Schema.Type.DOUBLE, new RowKeyFunction<Double>() {
        @Override
        public Double convert(byte[] rowKey) {
          return Bytes.toDouble(rowKey);
        }
      })
      .put(Schema.Type.STRING, new RowKeyFunction<String>() {
        @Override
        public String convert(byte[] rowKey) {
          return Bytes.toString(rowKey);
        }
      })
      .build();
  private final String rowFieldName;
  private final RowKeyFunction rowKeyFunction;

  public ReflectionRowRecordReader(Schema schema, @Nullable String rowFieldName) {
    super(schema, TypeToken.of(StructuredRecord.class));
    this.rowFieldName = rowFieldName;
    // if row field is given, make sure the type is a non-null simple type
    if (rowFieldName != null) {
      Schema.Field rowField = schema.getField(rowFieldName);
      Preconditions.checkArgument(rowField != null, "Row field not found in schema");
      Schema.Type rowType = rowField.getSchema().getType();
      Preconditions.checkArgument(rowType != Schema.Type.NULL, "Row field cannot have null type.");
      Preconditions.checkArgument(rowField.getSchema().isSimpleOrNullableSimple(),
        "Row field must be a simple (boolean, bytes, int, long, float, double, or string) or nullable simple type.");
      if (rowField.getSchema().isNullableSimple()) {
        this.rowKeyFunction = ROW_KEY_FUNCTIONS.get(rowField.getSchema().getNonNullable().getType());
      } else {
        this.rowKeyFunction = ROW_KEY_FUNCTIONS.get(rowType);
      }
    } else {
      this.rowKeyFunction = null;
    }
  }

  @Override
  public StructuredRecord read(Row row, Schema sourceSchema) throws IOException {
    Preconditions.checkArgument(sourceSchema.getType() == Schema.Type.RECORD, "Source schema must be a record.");
    initializeRead(sourceSchema);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    // if one of the fields should come from the row key, add it.
    if (rowFieldName != null) {
      builder.set(rowFieldName, rowKeyFunction.convert(row.getRow()));
    }

    // go through the Row columns and add their values to the record
    try {
      for (Schema.Field sourceField : sourceSchema.getFields()) {
        String sourceFieldName = sourceField.getName();
        Schema.Field targetField = schema.getField(sourceFieldName);
        // the Row may contain more fields than our target schema. Skip those fields that are not in the target schema,
        // as well as the row key field since it comes from the row key and not the columns.
        if (targetField == null || targetField.getName().equals(rowFieldName)) {
          advanceField();
          continue;
        }
        builder.set(sourceFieldName, read(row, sourceField.getSchema(), targetField.getSchema(), type));
      }
      return builder.build();
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  // converts a row key into some other object.
  private interface RowKeyFunction<T> {
    T convert(byte[] rowKey);
  }
}
