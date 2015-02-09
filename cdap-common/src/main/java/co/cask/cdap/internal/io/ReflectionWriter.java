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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Base class for writing an object with a {@link Schema}. Examines the schema to cast the object accordingly,
 * and uses reflection to determine field values if the object is a record. Recursive types are not allowed.
 *
 * @param <WRITER> the type of writer used to encode objects
 * @param <TYPE> the type of object to write
 */
public abstract class ReflectionWriter<WRITER, TYPE> {

  protected final Schema schema;
  protected Set<Object> seenRefs;

  protected ReflectionWriter(Schema schema) {
    this.schema = schema;
  }

  public void write(TYPE object, WRITER writer) throws IOException {
    seenRefs = Sets.newIdentityHashSet();
    write(writer, object, schema);
  }

  protected abstract void writeNull(WRITER writer) throws IOException;

  protected abstract void writeBool(WRITER writer, Boolean val) throws IOException;

  protected abstract void writeInt(WRITER writer, int val) throws IOException;

  protected abstract void writeLong(WRITER writer, long val) throws IOException;

  protected abstract void writeFloat(WRITER writer, Float val) throws IOException;

  protected abstract void writeDouble(WRITER writer, Double val) throws IOException;

  protected abstract void writeString(WRITER writer, String val) throws IOException;

  protected abstract void writeBytes(WRITER writer, ByteBuffer val) throws IOException;

  protected abstract void writeBytes(WRITER writer, byte[] val) throws IOException;

  protected abstract void writeEnum(WRITER writer, String val, Schema schema) throws IOException;

  protected abstract void writeArray(WRITER writer, Collection val, Schema componentSchema) throws IOException;

  protected abstract void writeArray(WRITER writer, Object val, Schema componentSchema) throws IOException;

  protected abstract void writeMap(WRITER writer, Map<?, ?> val,
                                   Map.Entry<Schema, Schema> mapSchema) throws IOException;

  protected abstract void writeUnion(WRITER writer, Object val, Schema unionSchema) throws IOException;

  /**
   * Write the given object that has the given schema.
   *
   * @param object the object to write
   * @param objSchema the schema of the object to write
   * @throws IOException if there was an exception writing the object
   */
  @SuppressWarnings("ConstantConditions")
  protected void write(WRITER writer, Object object, Schema objSchema) throws IOException {
    if (object != null) {
      if (seenRefs.contains(object)) {
        throw new IOException("Recursive reference not supported.");
      }
      if (objSchema.getType() == Schema.Type.RECORD) {
        seenRefs.add(object);
      }
    }

    switch(objSchema.getType()) {
      case NULL:
        writeNull(writer);
        break;
      case BOOLEAN:
        writeBool(writer, (Boolean) object);
        break;
      case INT:
        writeInt(writer, ((Number) object).intValue());
        break;
      case LONG:
        writeLong(writer, ((Number) object).longValue());
        break;
      case FLOAT:
        writeFloat(writer, (Float) object);
        break;
      case DOUBLE:
        writeDouble(writer, (Double) object);
        break;
      case STRING:
        writeString(writer, object.toString());
        break;
      case BYTES:
        if (object instanceof ByteBuffer) {
          writeBytes(writer, (ByteBuffer) object);
        } else if (object instanceof UUID) {
          UUID uuid = (UUID)  object;
          ByteBuffer buf = ByteBuffer.allocate(Longs.BYTES * 2);
          buf.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
          writeBytes(writer, (ByteBuffer) buf.flip());
        } else {
          writeBytes(writer, (byte[]) object);
        }
        break;
      case ENUM:
        writeEnum(writer, object.toString(), objSchema);
        break;
      case ARRAY:
        if (object instanceof Collection) {
          writeArray(writer, (Collection) object, objSchema.getComponentSchema());
        } else {
          writeArray(writer, object, objSchema.getComponentSchema());
        }
        break;
      case MAP:
        writeMap(writer, (Map<?, ?>) object, objSchema.getMapSchema());
        break;
      case RECORD:
        writeRecord(writer, object, objSchema);
        break;
      case UNION:
        writeUnion(writer, object, objSchema);
        break;
    }
  }

  protected void writeRecord(WRITER writer, Object record, Schema recordSchema) throws IOException {
    try {
      TypeToken<?> type = TypeToken.of(record.getClass());

      Map<String, Method> methods = collectByMethod(type, Maps.<String, Method>newHashMap());
      Map<String, Field> fields = collectByFields(type, Maps.<String, Field>newHashMap());

      for (Schema.Field field : recordSchema.getFields()) {
        String fieldName = field.getName();
        Object value;
        Field recordField = fields.get(fieldName);
        if (recordField != null) {
          recordField.setAccessible(true);
          value = recordField.get(record);
        } else {
          Method method = methods.get(fieldName);
          if (method == null) {
            throw new IOException("Unable to read field value through getter. Class=" + type + ", field=" + fieldName);
          }
          value = method.invoke(record);
        }

        Schema fieldSchema = field.getSchema();
        write(writer, value, fieldSchema);
      }
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException(e);
    }
  }

  private Map<String, Field> collectByFields(TypeToken<?> typeToken, Map<String, Field> fields) {
    // Collect the field types
    for (TypeToken<?> classType : typeToken.getTypes().classes()) {
      Class<?> rawType = classType.getRawType();
      if (rawType.equals(Object.class)) {
        // Ignore all object fields
        continue;
      }

      for (Field field : rawType.getDeclaredFields()) {
        if (Modifier.isTransient(field.getModifiers()) || field.isSynthetic()) {
          continue;
        }
        fields.put(field.getName(), field);
      }
    }
    return fields;
  }

  private Map<String, Method> collectByMethod(TypeToken<?> typeToken, Map<String, Method> methods) {
    for (Method method : typeToken.getRawType().getMethods()) {
      if (method.getDeclaringClass().equals(Object.class)) {
        // Ignore all object methods
        continue;
      }
      String methodName = method.getName();
      if (!(methodName.startsWith("get") || methodName.startsWith("is"))
           || method.isSynthetic() || method.getParameterTypes().length != 0) {
        // Ignore not getter methods
        continue;
      }
      String fieldName = methodName.startsWith("get") ?
                           methodName.substring("get".length()) : methodName.substring("is".length());
      if (fieldName.isEmpty()) {
        continue;
      }
      fieldName = String.format("%c%s", Character.toLowerCase(fieldName.charAt(0)), fieldName.substring(1));
      if (methods.containsKey(fieldName)) {
        continue;
      }
      methods.put(fieldName, method);
    }
    return methods;
  }
}
