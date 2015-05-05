/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.common.io.Decoder;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

/**
 * Reflection based Datum Reader.
 *
 * @param <T> type T reader
 */
public final class ReflectionDatumReader<T> extends ReflectionReader<Decoder, T> implements DatumReader<T> {

  public ReflectionDatumReader(Schema schema, TypeToken<T> type) {
    super(schema, type);
  }

  @Override
  protected Object readNull(Decoder decoder) throws IOException {
    return decoder.readNull();
  }

  @Override
  protected boolean readBool(Decoder decoder) throws IOException {
    return decoder.readBool();
  }

  @Override
  protected int readInt(Decoder decoder) throws IOException {
    return decoder.readInt();
  }

  @Override
  protected long readLong(Decoder decoder) throws IOException {
    return decoder.readLong();
  }

  @Override
  protected float readFloat(Decoder decoder) throws IOException {
    return decoder.readFloat();
  }

  @Override
  protected double readDouble(Decoder decoder) throws IOException {
    return decoder.readDouble();
  }

  @Override
  protected String readString(Decoder decoder) throws IOException {
    return decoder.readString();
  }

  @Override
  protected ByteBuffer readBytes(Decoder decoder) throws IOException {
    return decoder.readBytes();
  }

  @Override
  protected Object readEnum(Decoder decoder, Schema sourceSchema, Schema targetSchema,
                            TypeToken<?> targetTypeToken) throws IOException {
    String enumValue = sourceSchema.getEnumValue(decoder.readInt());
    check(targetSchema.getEnumValues().contains(enumValue), "Enum value '%s' missing in target.", enumValue);
    try {
      return targetTypeToken.getRawType().getMethod("valueOf", String.class).invoke(null, enumValue);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings({"unchecked", "ConstantConditions"})
  @Override
  protected Object readArray(Decoder decoder, Schema sourceSchema, Schema targetSchema,
                             TypeToken<?> targetTypeToken) throws IOException {
    TypeToken<?> componentType = null;
    if (targetTypeToken.isArray()) {
      componentType = targetTypeToken.getComponentType();
    } else if (Collection.class.isAssignableFrom(targetTypeToken.getRawType())) {
      Type type = targetTypeToken.getType();
      check(type instanceof ParameterizedType, "Only parameterized type is supported for collection.");
      componentType = TypeToken.of(((ParameterizedType) type).getActualTypeArguments()[0]);
    }
    check(componentType != null, "Only array or collection type is support for array value.");

    int len = decoder.readInt();
    Collection<Object> collection = (Collection<Object>) create(targetTypeToken);
    while (len != 0) {
      for (int i = 0; i < len; i++) {
        collection.add(read(decoder, sourceSchema.getComponentSchema(),
                            targetSchema.getComponentSchema(), componentType)
        );
      }
      len = decoder.readInt();
    }

    if (targetTypeToken.isArray()) {
      Object array = Array.newInstance(targetTypeToken.getComponentType().getRawType(), collection.size());
      int idx = 0;
      for (Object obj : collection) {
        Array.set(array, idx++, obj);
      }
      return array;
    }
    return collection;
  }

  @Override
  protected Object readMap(Decoder decoder, Schema sourceSchema, Schema targetSchema,
                           TypeToken<?> targetTypeToken) throws IOException {
    check(Map.class.isAssignableFrom(targetTypeToken.getRawType()), "Only map type is supported for map data.");
    Type type = targetTypeToken.getType();
    Preconditions.checkArgument(type instanceof ParameterizedType, "Only parameterized map is supported.");
    // suppressing warning because we know type is not null
    @SuppressWarnings("ConstantConditions")
    Type[] typeArgs = ((ParameterizedType) type).getActualTypeArguments();

    int len = decoder.readInt();
    // unchecked cast is ok, we're assuming the type token is correct
    @SuppressWarnings("unchecked")
    Map<Object, Object> map = (Map<Object, Object>) create(targetTypeToken);
    while (len != 0) {
      for (int i = 0; i < len; i++) {
        Map.Entry<Schema, Schema> sourceEntry = sourceSchema.getMapSchema();
        Map.Entry<Schema, Schema> targetEntry = targetSchema.getMapSchema();

        map.put(read(decoder, sourceEntry.getKey(), targetEntry.getKey(), TypeToken.of(typeArgs[0])),
                read(decoder, sourceEntry.getValue(), targetEntry.getValue(), TypeToken.of(typeArgs[1])));
      }
      len = decoder.readInt();
    }

    return map;
  }

  @Override
  protected Object readUnion(Decoder decoder, Schema sourceSchema, Schema targetSchema,
                             TypeToken<?> targetTypeToken) throws IOException {
    int idx = decoder.readInt();
    Schema sourceValueSchema = sourceSchema.getUnionSchemas().get(idx);


    if (targetSchema.getType() == Schema.Type.UNION) {
      try {
        // A simple optimization to try resolve before resorting to linearly try the union schema.
        Schema targetValueSchema = targetSchema.getUnionSchema(idx);
        if (targetValueSchema != null && targetValueSchema.getType() == sourceValueSchema.getType()) {
          return read(decoder, sourceValueSchema, targetValueSchema, targetTypeToken);
        }
      } catch (IOException e) {
        // OK to ignore it, as we'll do union schema resolution
      }
      for (Schema targetValueSchema : targetSchema.getUnionSchemas()) {
        try {
          return read(decoder, sourceValueSchema, targetValueSchema, targetTypeToken);
        } catch (IOException e) {
          // It's ok to have exception here, as we'll keep trying until exhausted the target union.
        }
      }
      throw new IOException(String.format("Fail to resolve %s to %s", sourceSchema, targetSchema));
    } else {
      return read(decoder, sourceValueSchema, targetSchema, targetTypeToken);
    }
  }

  @Override
  protected Object readRecord(Decoder decoder, Schema sourceSchema, Schema targetSchema,
                              TypeToken<?> targetTypeToken) throws IOException {
    try {
      Object record = create(targetTypeToken);
      for (Schema.Field sourceField : sourceSchema.getFields()) {
        Schema.Field targetField = targetSchema.getField(sourceField.getName());
        if (targetField == null) {
          skip(decoder, sourceField.getSchema());
          continue;
        }
        FieldAccessor fieldAccessor = getFieldAccessor(targetTypeToken, sourceField.getName());
        fieldAccessor.set(record,
                          read(decoder, sourceField.getSchema(), targetField.getSchema(), fieldAccessor.getType()));
      }
      return record;
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  private void skip(Decoder decoder, Schema schema) throws IOException {
    switch (schema.getType()) {
      case NULL:
        break;
      case BOOLEAN:
        decoder.readBool();
        break;
      case INT:
        decoder.readInt();
        break;
      case LONG:
        decoder.readLong();
        break;
      case FLOAT:
        decoder.skipFloat();
        break;
      case DOUBLE:
        decoder.skipDouble();
        break;
      case BYTES:
        decoder.skipBytes();
        break;
      case STRING:
        decoder.skipString();
        break;
      case ENUM:
        decoder.readInt();
        break;
      case ARRAY:
        skipArray(decoder, schema.getComponentSchema());
        break;
      case MAP:
        skipMap(decoder, schema.getMapSchema());
        break;
      case RECORD:
        skipRecord(decoder, schema);
        break;
      case UNION:
        skip(decoder, schema.getUnionSchema(decoder.readInt()));
        break;
    }
  }

  private void skipArray(Decoder decoder, Schema componentSchema) throws IOException {
    int len = decoder.readInt();
    while (len != 0) {
      skip(decoder, componentSchema);
      len = decoder.readInt();
    }
  }

  private void skipMap(Decoder decoder, Map.Entry<Schema, Schema> mapSchema) throws IOException {
    int len = decoder.readInt();
    while (len != 0) {
      skip(decoder, mapSchema.getKey());
      skip(decoder, mapSchema.getValue());
      len = decoder.readInt();
    }
  }

  private void skipRecord(Decoder decoder, Schema recordSchema) throws IOException {
    for (Schema.Field field : recordSchema.getFields()) {
      skip(decoder, field.getSchema());
    }
  }
}
