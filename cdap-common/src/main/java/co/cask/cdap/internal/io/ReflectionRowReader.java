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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Decodes an object from a {@link Row} object fetched from a {@link Table}. Assumes that objects
 * fetched are records. All fields are columns in the row, with simple types stored as their byte representation.
 * Complex types (arrays, maps, records) are not supported.
 *
 * @param <T> the type of object to read
 */
// suppress warnings that come from unboxing of objects that we validate are not null
@SuppressWarnings("ConstantConditions")
public class ReflectionRowReader<T> extends ReflectionReader<Row, T> {
  private static final Schema NULL_SCHEMA = Schema.of(Schema.Type.NULL);
  private List<String> fieldNames;
  private int index;

  public ReflectionRowReader(Schema schema, TypeToken<T> type) {
    super(schema, type);
    Preconditions.checkArgument(schema.getType() == Schema.Type.RECORD, "Target schema must be a record.");
  }

  @SuppressWarnings("unchecked")
  public T read(Row row, Schema sourceSchema) throws IOException {
    Preconditions.checkArgument(sourceSchema.getType() == Schema.Type.RECORD, "Source schema must be a record.");
    initializeRead(sourceSchema);
    try {
      Object record = create(type);
      for (Schema.Field sourceField : sourceSchema.getFields()) {
        String sourceFieldName = sourceField.getName();
        Schema.Field targetField = schema.getField(sourceFieldName);
        if (targetField == null) {
          advanceField();
          continue;
        }
        FieldAccessor fieldAccessor = getFieldAccessor(type, sourceFieldName);
        fieldAccessor.set(record, read(row, sourceField.getSchema(),
                                       targetField.getSchema(), fieldAccessor.getType()));
      }
      return (T) record;
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  @Override
  protected Object readNull(Row row) throws IOException {
    advanceField();
    return null;
  }

  @Override
  protected boolean readBool(Row row) throws IOException {
    String name = getCurrentField();
    Boolean val = row.getBoolean(name);
    validateNotNull(val, name);
    advanceField();
    return val;
  }

  @Override
  protected int readInt(Row row) throws IOException {
    String name = getCurrentField();
    Integer val = row.getInt(name);
    validateNotNull(val, name);
    advanceField();
    return val;
  }

  @Override
  protected long readLong(Row row) throws IOException {
    String name = getCurrentField();
    Long val = row.getLong(name);
    validateNotNull(val, name);
    advanceField();
    return val;
  }

  @Override
  protected float readFloat(Row row) throws IOException {
    String name = getCurrentField();
    Float val = row.getFloat(name);
    validateNotNull(val, name);
    advanceField();
    return val;
  }

  @Override
  protected double readDouble(Row row) throws IOException {
    String name = getCurrentField();
    Double val = row.getDouble(name);
    validateNotNull(val, name);
    advanceField();
    return val;
  }

  @Override
  protected String readString(Row row) throws IOException {
    String name = getCurrentField();
    String val = row.getString(name);
    validateNotNull(val, name);
    advanceField();
    return val;
  }

  @Override
  protected ByteBuffer readBytes(Row row) throws IOException {
    String name = getCurrentField();
    byte[] val = row.get(name);
    validateNotNull(val, name);
    advanceField();
    return ByteBuffer.wrap(val);
  }

  @Override
  protected Object readEnum(Row row, Schema sourceSchema, Schema targetSchema,
                            TypeToken<?> targetTypeToken) throws IOException {
    String name = getCurrentField();
    String enumValue = row.getString(name);
    validateNotNull(enumValue, name);
    check(targetSchema.getEnumValues().contains(enumValue), "Enum value '%s' missing in target.", enumValue);
    try {
      Object obj = targetTypeToken.getRawType().getMethod("valueOf", String.class).invoke(null, enumValue);
      advanceField();
      return obj;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected Object readUnion(Row row, Schema sourceSchema, Schema targetSchema,
                             TypeToken<?> targetTypeToken) throws IOException {
    // assumption is that unions are only possible if they represent a nullable.
    if (!sourceSchema.isNullable()) {
      throw new UnsupportedOperationException("Unions that do not represent nullables are not supported.");
    }

    String name = getCurrentField();
    Schema sourceValueSchema = row.get(name) == null ? NULL_SCHEMA : sourceSchema.getNonNullable();
    if (targetSchema.getType() == Schema.Type.UNION) {
      for (Schema targetValueSchema : targetSchema.getUnionSchemas()) {
        try {
          return read(row, sourceValueSchema, targetValueSchema, targetTypeToken);
        } catch (IOException e) {
          // It's ok to have exception here, as we'll keep trying until exhausted the target union.
        }
      }
      throw new IOException(String.format("Fail to resolve %s to %s", sourceSchema, targetSchema));
    } else {
      return read(row, sourceValueSchema, targetSchema, targetTypeToken);
    }
  }

  @Override
  protected Object readArray(Row row, Schema sourceSchema, Schema targetSchema,
                             TypeToken<?> targetTypeToken) throws IOException {
    throw new UnsupportedOperationException("Arrays are not supported.");
  }

  @Override
  protected Object readMap(Row row, Schema sourceSchema, Schema targetSchema,
                           TypeToken<?> targetTypeToken) throws IOException {
    throw new UnsupportedOperationException("Maps are not supported.");
  }

  @Override
  protected Object readRecord(Row row, Schema sourceSchema, Schema targetSchema,
                              TypeToken<?> targetTypeToken) throws IOException {
    throw new UnsupportedOperationException("Records are not supported.");
  }

  protected String getCurrentField() {
    return fieldNames.get(index);
  }

  protected void advanceField() {
    index++;
  }

  // check that the value is not null and throw an exception if it is.
  // Nullable types depend on this behavior to work correctly, as they cycle through
  // possible types and catch IOExceptions if the type doesn't work out.
  protected void validateNotNull(Object val, String column) throws IOException {
    if (val == null) {
      throw new IOException("No value for " + column + " exists.");
    }
  }

  protected void initializeRead(Schema sourceSchema) {
    List<Schema.Field> schemaFields = sourceSchema.getFields();
    int numFields = schemaFields.size();
    Preconditions.checkArgument(numFields > 0, "Record must contain at least one field.");
    this.fieldNames = Lists.newArrayListWithCapacity(numFields);
    for (Schema.Field schemaField : schemaFields) {
      this.fieldNames.add(schemaField.getName());
    }
    this.index = 0;
  }
}
