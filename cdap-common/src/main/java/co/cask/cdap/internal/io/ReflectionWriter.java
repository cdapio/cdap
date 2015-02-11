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
 * and uses reflection to determine field values if the object is a record. Recursive types are not allowed,
 * and the writer will keep track of references and throw an exception if it sees a recursive reference. A
 * new writer should be created for each object written.
 */
public abstract class ReflectionWriter {

  protected final Set<Object> seenRefs = Sets.newIdentityHashSet();

  protected abstract void writeNull(String name) throws IOException;

  protected abstract void writeBool(String name, Boolean val) throws IOException;

  protected abstract void writeInt(String name, int val) throws IOException;

  protected abstract void writeLong(String name, long val) throws IOException;

  protected abstract void writeFloat(String name, Float val) throws IOException;

  protected abstract void writeDouble(String name, Double val) throws IOException;

  protected abstract void writeString(String name, String val) throws IOException;

  protected abstract void writeBytes(String name, ByteBuffer val) throws IOException;

  protected abstract void writeBytes(String name, byte[] val) throws IOException;

  protected abstract void writeEnum(String name, String val, Schema schema) throws IOException;

  protected abstract void writeArray(String name, Collection val, Schema componentSchema) throws IOException;

  protected abstract void writeArray(String name, Object val, Schema componentSchema) throws IOException;

  protected abstract void writeMap(String name, Map<?, ?> val,
                                   Map.Entry<Schema, Schema> mapSchema) throws IOException;

  protected abstract void writeUnion(String name, Object val, Schema schema) throws IOException;

  /**
   * Write the given object that has the given schema.
   *
   * @param object the object to write
   * @param objSchema the schema of the object to write
   * @throws IOException if there was an exception writing the object
   */
  public void write(Object object, Schema objSchema) throws IOException {
    write(null, object, objSchema);
  }

  @SuppressWarnings("ConstantConditions")
  protected void write(String name, Object object, Schema objSchema) throws IOException {
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
        writeNull(name);
        break;
      case BOOLEAN:
        writeBool(name, (Boolean) object);
        break;
      case INT:
        writeInt(name, ((Number) object).intValue());
        break;
      case LONG:
        writeLong(name, ((Number) object).longValue());
        break;
      case FLOAT:
        writeFloat(name, (Float) object);
        break;
      case DOUBLE:
        writeDouble(name, (Double) object);
        break;
      case STRING:
        writeString(name, object.toString());
        break;
      case BYTES:
        if (object instanceof ByteBuffer) {
          writeBytes(name, (ByteBuffer) object);
        } else if (object instanceof UUID) {
          UUID uuid = (UUID)  object;
          ByteBuffer buf = ByteBuffer.allocate(Longs.BYTES * 2);
          buf.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
          writeBytes(name, (ByteBuffer) buf.flip());
        } else {
          writeBytes(name, (byte[]) object);
        }
        break;
      case ENUM:
        writeEnum(name, object.toString(), objSchema);
        break;
      case ARRAY:
        if (object instanceof Collection) {
          writeArray(name, (Collection) object, objSchema.getComponentSchema());
        } else {
          writeArray(name, object, objSchema.getComponentSchema());
        }
        break;
      case MAP:
        writeMap(name, (Map<?, ?>) object, objSchema.getMapSchema());
        break;
      case RECORD:
        writeRecord(name, object, objSchema);
        break;
      case UNION:
        writeUnion(name, object, objSchema);
        break;
    }
  }

  protected void writeRecord(String name, Object record, Schema recordSchema) throws IOException {
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
        String nestedFieldName = name == null ? fieldName : scopeFieldName(name, fieldName);
        write(nestedFieldName, value, fieldSchema);
      }
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException(e);
    }
  }

  protected String scopeFieldName(String scope, String fieldName) {
    return scope + "." + fieldName;
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
