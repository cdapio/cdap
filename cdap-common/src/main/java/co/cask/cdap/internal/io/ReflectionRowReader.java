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
import co.cask.cdap.common.io.BinaryDecoder;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Decodes an object from a {@link Row} object fetched from a {@link Table}. Assumes that objects
 * fetched are records. All fields are columns in the row, with simple types stored as their byte representation,
 * and complex types as Avro-like binary encoded blobs.
 *
 * @param <T> the type of object to read
 */
// suppress warnings that come from unboxing of objects that we validate are not null
@SuppressWarnings("ConstantConditions")
public class ReflectionRowReader<T> extends ReflectionReader<T> {
  private static final Schema NULL_SCHEMA = Schema.of(Schema.Type.NULL);
  private final Row row;

  public ReflectionRowReader(Row row) {
    this.row = row;
  }

  @SuppressWarnings("unchecked")
  public T read(Schema sourceSchema, Schema targetSchema,
                TypeToken<?> targetTypeToken) throws IOException {
    Preconditions.checkArgument(isRecord(sourceSchema), "Source schema must be a record or nullable record.");
    Preconditions.checkArgument(isRecord(targetSchema), "Target schema must be a record or nullable record.");

    try {
      Object record = create(targetTypeToken);
      for (Schema.Field sourceField : sourceSchema.getFields()) {
        String sourceFieldName = sourceField.getName();
        Schema.Field targetField = targetSchema.getField(sourceFieldName);
        if (targetField == null) {
          continue;
        }
        FieldAccessor fieldAccessor = getFieldAccessor(targetTypeToken, sourceFieldName);
        fieldAccessor.set(record, read(sourceFieldName, sourceField.getSchema(),
                                       targetField.getSchema(), fieldAccessor.getType()));
      }
      return (T) record;
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  @Override
  protected Object readNull(String name) throws IOException {
    return null;
  }

  @Override
  protected boolean readBool(String name) throws IOException {
    Boolean val = row.getBoolean(name);
    validateNotNull(val, name);
    return val;
  }

  @Override
  protected int readInt(String name) throws IOException {
    Integer val = row.getInt(name);
    validateNotNull(val, name);
    return val;
  }

  @Override
  protected long readLong(String name) throws IOException {
    Long val = row.getLong(name);
    validateNotNull(val, name);
    return val;
  }

  @Override
  protected float readFloat(String name) throws IOException {
    Float val = row.getFloat(name);
    validateNotNull(val, name);
    return val;
  }

  @Override
  protected double readDouble(String name) throws IOException {
    Double val = row.getDouble(name);
    validateNotNull(val, name);
    return val;
  }

  @Override
  protected String readString(String name) throws IOException {
    String val = row.getString(name);
    validateNotNull(val, name);
    return val;
  }

  @Override
  protected ByteBuffer readBytes(String name) throws IOException {
    byte[] val = row.get(name);
    validateNotNull(val, name);
    return ByteBuffer.wrap(row.get(name));
  }

  @Override
  protected Object readEnum(String name, Schema sourceSchema, Schema targetSchema,
                            TypeToken<?> targetTypeToken) throws IOException {
    Integer val = row.getInt(name);
    validateNotNull(val, name);
    String enumValue = sourceSchema.getEnumValue(val);
    check(targetSchema.getEnumValues().contains(enumValue), "Enum value '%s' missing in target.", enumValue);
    try {
      return targetTypeToken.getRawType().getMethod("valueOf", String.class).invoke(null, enumValue);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected Object readUnion(String name, Schema sourceSchema, Schema targetSchema,
                             TypeToken<?> targetTypeToken) throws IOException {
    // assumption is that unions are only possible if they represent a nullable.
    if (!sourceSchema.isNullable()) {
      throw new UnsupportedOperationException("Unions that do not represent nullables are not supported.");
    }

    Schema sourceValueSchema = row.get(name) == null ? NULL_SCHEMA : sourceSchema.getNonNullable();
    if (targetSchema.getType() == Schema.Type.UNION) {
      for (Schema targetValueSchema : targetSchema.getUnionSchemas()) {
        try {
          return read(name, sourceValueSchema, targetValueSchema, targetTypeToken);
        } catch (IOException e) {
          // It's ok to have exception here, as we'll keep trying until exhausted the target union.
        }
      }
      throw new IOException(String.format("Fail to resolve %s to %s", sourceSchema, targetSchema));
    } else {
      return read(name, sourceValueSchema, targetSchema, targetTypeToken);
    }
  }

  @Override
  protected Object readArray(String name, Schema sourceSchema, Schema targetSchema,
                             TypeToken<?> targetTypeToken) throws IOException {
    return readAndDecode(name, sourceSchema, targetSchema, targetTypeToken);
  }

  @Override
  protected Object readMap(String name, Schema sourceSchema, Schema targetSchema,
                           TypeToken<?> targetTypeToken) throws IOException {
    return readAndDecode(name, sourceSchema, targetSchema, targetTypeToken);
  }

  @Override
  protected Object readRecord(String name, Schema sourceSchema, Schema targetSchema,
                              TypeToken<?> targetTypeToken) throws IOException {
    return readAndDecode(name, sourceSchema, targetSchema, targetTypeToken);
  }

  private Object readAndDecode(String name, Schema sourceSchema, Schema targetSchema,
                               TypeToken<?> targetTypeToken) throws IOException {
    byte[] val = row.get(name);
    validateNotNull(val, name);
    return decode(val, sourceSchema, targetSchema, targetTypeToken);
  }

  private <O> O decode(byte[] bytes, Schema sourceSchema, Schema targetSchema,
                        TypeToken<O> targetTypeToken) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    BinaryDecoder decoder = new BinaryDecoder(bis);
    ReflectionDatumReader<O> datumReader = new ReflectionDatumReader<O>(targetSchema, targetTypeToken);
    return datumReader.read(decoder, sourceSchema);
  }

  // check that the value is not null and throw an exception if it is.
  // Nullable types depend on this behavior to work correctly, as they cycle through
  // possible types and catch IOExceptions if the type doesn't work out.
  private void validateNotNull(Object val, String column) throws IOException {
    if (val == null) {
      throw new IOException("No value for " + column + " exists.");
    }
  }

  // return true if the schema is a record or a nullable record
  private boolean isRecord(Schema schema) {
    if (schema.isNullable()) {
      return schema.getNonNullable().getType() == Schema.Type.RECORD;
    } else {
      return schema.getType() == Schema.Type.RECORD;
    }
  }
}
