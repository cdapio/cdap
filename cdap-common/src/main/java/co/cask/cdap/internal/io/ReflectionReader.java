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
import javax.annotation.Nullable;

/**
 * Reflection based reader.
 *
 * @param <T> type of object to read
 */
public abstract class ReflectionReader<T> {

  private final Map<Class<?>, Instantiator<?>> creators;
  private final InstantiatorFactory creatorFactory;
  private final FieldAccessorFactory fieldAccessorFactory;

  public ReflectionReader() {
    this.creatorFactory = new InstantiatorFactory(true);
    this.creators = Maps.newIdentityHashMap();
    this.fieldAccessorFactory = new ReflectionFieldAccessorFactory();
  }

  /**
   * Read the object to the target type with target schema, given that it has the given source schema.
   *
   * @param sourceSchema the write schema of the object
   * @param targetSchema the read schema for the object
   * @param targetTypeToken the type token of the object to read
   * @return the object of given type and schema
   * @throws IOException if there was some exception reading the object
   */
  public T read(Schema sourceSchema, Schema targetSchema,
                TypeToken<?> targetTypeToken) throws IOException {
    return read(null, sourceSchema, targetSchema, targetTypeToken);
  }

  @SuppressWarnings("unchecked")
  protected T read(String name, Schema sourceSchema, Schema targetSchema,
                   TypeToken<?> targetTypeToken) throws IOException {

    if (sourceSchema.getType() != Schema.Type.UNION && targetSchema.getType() == Schema.Type.UNION) {
      // Try every target schemas
      for (Schema schema : targetSchema.getUnionSchemas()) {
        try {
          return (T) doRead(name, sourceSchema, schema, targetTypeToken);
        } catch (IOException e) {
          // Continue;
        }
      }
      throw new IOException(String.format("No matching schema to resolve %s to %s", sourceSchema, targetSchema));
    }
    return (T) doRead(name, sourceSchema, targetSchema, targetTypeToken);
  }

  protected abstract Object readNull(String name) throws IOException;

  protected abstract boolean readBool(String name) throws IOException;

  protected abstract int readInt(String name) throws IOException;

  protected abstract long readLong(String name) throws IOException;

  protected abstract float readFloat(String name) throws IOException;

  protected abstract double readDouble(String name) throws IOException;

  protected abstract String readString(String name) throws IOException;

  protected abstract ByteBuffer readBytes(String name) throws IOException;

  protected abstract Object readEnum(String name, Schema sourceSchema, Schema targetSchema,
                                     TypeToken<?> targetTypeToken) throws IOException;

  protected abstract Object readArray(String name, Schema sourceSchema, Schema targetSchema,
                                      TypeToken<?> targetTypeToken) throws IOException;

  protected abstract Object readMap(String name, Schema sourceSchema, Schema targetSchema,
                                    TypeToken<?> targetTypeToken) throws IOException;

  protected abstract Object readUnion(String name, Schema sourceSchema, Schema targetSchema,
                                      TypeToken<?> targetTypeToken) throws IOException;

  protected abstract Object readRecord(String name, Schema sourceSchema, Schema targetSchema,
                                       TypeToken<?> targetTypeToken) throws IOException;


  protected Object doRead(String name, Schema sourceSchema,
                          Schema targetSchema, TypeToken<?> targetTypeToken) throws IOException {

    Schema.Type sourceType = sourceSchema.getType();
    Schema.Type targetType = targetSchema.getType();

    switch(sourceType) {
      case NULL:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return readNull(name);
      case BYTES:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        ByteBuffer buffer = readBytes(name);

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
        return readEnum(name, sourceSchema, targetSchema, targetTypeToken);
      case ARRAY:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return readArray(name, sourceSchema, targetSchema, targetTypeToken);
      case MAP:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return readMap(name, sourceSchema, targetSchema, targetTypeToken);
      case RECORD:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return readRecord(name, sourceSchema, targetSchema, targetTypeToken);
      case UNION:
        return readUnion(name, sourceSchema, targetSchema, targetTypeToken);
    }
    // For simple type other than NULL and BYTES
    if (sourceType.isSimpleType()) {
      return resolveType(name, sourceType, targetType, targetTypeToken);
    }
    throw new IOException(String.format("Fails to resolve %s to %s", sourceSchema, targetSchema));
  }

  private Object resolveType(@Nullable String name, Schema.Type sourceType,
                             Schema.Type targetType, TypeToken<?> targetTypeToken) throws IOException {
    switch(sourceType) {
      case BOOLEAN:
        switch(targetType) {
          case BOOLEAN:
            return readBool(name);
          case STRING:
            return String.valueOf(readBool(name));
        }
        break;
      case INT:
        switch(targetType) {
          case INT:
            Class<?> targetClass = targetTypeToken.getRawType();
            int value = readInt(name);
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
            return (long) readInt(name);
          case FLOAT:
            return (float) readInt(name);
          case DOUBLE:
            return (double) readInt(name);
          case STRING:
            return String.valueOf(readInt(name));
        }
        break;
      case LONG:
        switch(targetType) {
          case LONG:
            return readLong(name);
          case FLOAT:
            return (float) readLong(name);
          case DOUBLE:
            return (double) readLong(name);
          case STRING:
            return String.valueOf(readLong(name));
        }
        break;
      case FLOAT:
        switch(targetType) {
          case FLOAT:
            return readFloat(name);
          case DOUBLE:
            return (double) readFloat(name);
          case STRING:
            return String.valueOf(readFloat(name));
        }
        break;
      case DOUBLE:
        switch(targetType) {
          case DOUBLE:
            return readDouble(name);
          case STRING:
            return String.valueOf(readDouble(name));
        }
        break;
      case STRING:
        switch(targetType) {
          case STRING:
            String str = readString(name);
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
