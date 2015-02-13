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
import co.cask.cdap.common.lang.Instantiator;
import co.cask.cdap.common.lang.InstantiatorFactory;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

/**
 * Reflection based reader.
 *
 * @param <FROM> type of object to read from
 * @param <TO> type of object to read
 */
public abstract class ReflectionReader<FROM, TO> {

  private final Map<Class<?>, Instantiator<?>> creators;
  private final InstantiatorFactory creatorFactory;
  private final FieldAccessorFactory fieldAccessorFactory;
  protected final Schema schema;
  protected final TypeToken<TO> type;

  protected ReflectionReader(Schema schema, TypeToken<TO> type) {
    this.creatorFactory = new InstantiatorFactory(true);
    this.creators = Maps.newIdentityHashMap();
    this.fieldAccessorFactory = new ReflectionFieldAccessorFactory();
    this.schema = schema;
    this.type = type;
  }

  /**
   * Read from the source to create the target type with target schema.
   *
   * @param source the source from which to read the object
   * @param sourceSchema the write schema of the object
   * @return the object of given type and schema
   * @throws IOException if there was some exception reading the object
   */
  public TO read(FROM source, Schema sourceSchema) throws IOException {
    return read(source, sourceSchema, schema, type);
  }

  @SuppressWarnings("unchecked")
  protected TO read(FROM source, Schema sourceSchema, Schema targetSchema,
                    TypeToken<?> targetTypeToken) throws IOException {
    if (sourceSchema.getType() != Schema.Type.UNION && targetSchema.getType() == Schema.Type.UNION) {
      // Try every target schemas
      for (Schema schema : targetSchema.getUnionSchemas()) {
        try {
          return (TO) doRead(source, sourceSchema, schema, targetTypeToken);
        } catch (IOException e) {
          // Continue;
        }
      }
      throw new IOException(String.format("No matching schema to resolve %s to %s", sourceSchema, targetSchema));
    }
    return (TO) doRead(source, sourceSchema, targetSchema, targetTypeToken);
  }

  protected abstract Object readNull(FROM source) throws IOException;

  protected abstract boolean readBool(FROM source) throws IOException;

  protected abstract int readInt(FROM source) throws IOException;

  protected abstract long readLong(FROM source) throws IOException;

  protected abstract float readFloat(FROM source) throws IOException;

  protected abstract double readDouble(FROM source) throws IOException;

  protected abstract String readString(FROM source) throws IOException;

  protected abstract ByteBuffer readBytes(FROM source) throws IOException;

  protected abstract Object readEnum(FROM source, Schema sourceSchema, Schema targetSchema,
                                     TypeToken<?> targetTypeToken) throws IOException;

  protected abstract Object readArray(FROM source, Schema sourceSchema, Schema targetSchema,
                                      TypeToken<?> targetTypeToken) throws IOException;

  protected abstract Object readMap(FROM source, Schema sourceSchema, Schema targetSchema,
                                    TypeToken<?> targetTypeToken) throws IOException;

  protected abstract Object readUnion(FROM source, Schema sourceSchema, Schema targetSchema,
                                      TypeToken<?> targetTypeToken) throws IOException;

  protected abstract Object readRecord(FROM source, Schema sourceSchema, Schema targetSchema,
                                       TypeToken<?> targetTypeToken) throws IOException;


  protected Object doRead(FROM source, Schema sourceSchema, Schema targetSchema,
                          TypeToken<?> targetTypeToken) throws IOException {

    Schema.Type sourceType = sourceSchema.getType();
    Schema.Type targetType = targetSchema.getType();

    switch(sourceType) {
      case NULL:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return readNull(source);
      case BYTES:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        ByteBuffer buffer = readBytes(source);

        if (targetTypeToken.getRawType().equals(byte[].class)) {
          if (buffer.hasArray()) {
            byte[] array = buffer.array();
            if (buffer.remaining() == array.length) {
              return array;
            }
            byte[] bytes = new byte[buffer.remaining()];
            System.arraycopy(array, buffer.arrayOffset() + buffer.position(), bytes, 0, buffer.remaining());
            return bytes;
          } else {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
          }
        } else if (targetTypeToken.getRawType().equals(UUID.class) && buffer.remaining() == Longs.BYTES * 2) {
          return new UUID(buffer.getLong(), buffer.getLong());
        }
        return buffer;
      case ENUM:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return readEnum(source, sourceSchema, targetSchema, targetTypeToken);
      case ARRAY:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return readArray(source, sourceSchema, targetSchema, targetTypeToken);
      case MAP:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return readMap(source, sourceSchema, targetSchema, targetTypeToken);
      case RECORD:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return readRecord(source, sourceSchema, targetSchema, targetTypeToken);
      case UNION:
        return readUnion(source, sourceSchema, targetSchema, targetTypeToken);
    }
    // For simple type other than NULL and BYTES
    if (sourceType.isSimpleType()) {
      return resolveType(source, sourceType, targetType, targetTypeToken);
    }
    throw new IOException(String.format("Fails to resolve %s to %s", sourceSchema, targetSchema));
  }

  private Object resolveType(FROM source, Schema.Type sourceType, Schema.Type targetType,
                             TypeToken<?> targetTypeToken) throws IOException {
    switch(sourceType) {
      case BOOLEAN:
        switch(targetType) {
          case BOOLEAN:
            return readBool(source);
          case STRING:
            return String.valueOf(readBool(source));
        }
        break;
      case INT:
        switch(targetType) {
          case INT:
            Class<?> targetClass = targetTypeToken.getRawType();
            int value = readInt(source);
            if (targetClass.equals(byte.class) || targetClass.equals(Byte.class)) {
              return (byte) value;
            }
            if (targetClass.equals(char.class) || targetClass.equals(Character.class)) {
              return (char) value;
            }
            if (targetClass.equals(short.class) || targetClass.equals(Short.class)) {
              return (short) value;
            }
            return value;
          case LONG:
            return (long) readInt(source);
          case FLOAT:
            return (float) readInt(source);
          case DOUBLE:
            return (double) readInt(source);
          case STRING:
            return String.valueOf(readInt(source));
        }
        break;
      case LONG:
        switch(targetType) {
          case LONG:
            return readLong(source);
          case FLOAT:
            return (float) readLong(source);
          case DOUBLE:
            return (double) readLong(source);
          case STRING:
            return String.valueOf(readLong(source));
        }
        break;
      case FLOAT:
        switch(targetType) {
          case FLOAT:
            return readFloat(source);
          case DOUBLE:
            return (double) readFloat(source);
          case STRING:
            return String.valueOf(readFloat(source));
        }
        break;
      case DOUBLE:
        switch(targetType) {
          case DOUBLE:
            return readDouble(source);
          case STRING:
            return String.valueOf(readDouble(source));
        }
        break;
      case STRING:
        switch(targetType) {
          case STRING:
            String str = readString(source);
            Class<?> targetClass = targetTypeToken.getRawType();
            if (targetClass.equals(URI.class)) {
              return URI.create(str);
            } else if (targetClass.equals(URL.class)) {
              return new URL(str);
            }
            return str;
        }
        break;
    }

    throw new IOException("Fail to resolve type " + sourceType + " to type " + targetType);
  }

  protected void check(boolean condition, String message, Object... objs) throws IOException {
    if (!condition) {
      throw new IOException(String.format(message, objs));
    }
  }

  protected IOException propagate(Throwable t) throws IOException {
    if (t instanceof IOException) {
      throw (IOException) t;
    }
    throw new IOException(t);
  }

  protected Object create(TypeToken<?> type) {
    Class<?> rawType = type.getRawType();
    Instantiator<?> creator = creators.get(rawType);
    if (creator == null) {
      creator = creatorFactory.get(type);
      creators.put(rawType, creator);
    }
    return creator.create();
  }

  protected FieldAccessor getFieldAccessor(TypeToken<?> targetTypeToken, String fieldName) {
    return fieldAccessorFactory.getFieldAccessor(targetTypeToken, fieldName);
  }
}
