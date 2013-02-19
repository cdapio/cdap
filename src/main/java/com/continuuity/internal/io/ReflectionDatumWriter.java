package com.continuuity.internal.io;

import com.continuuity.api.io.Schema;
import com.continuuity.common.io.Encoder;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 *
 */
public final class ReflectionDatumWriter {

  private final Schema schema;

  public ReflectionDatumWriter(Schema schema) {
    this.schema = schema;
  }

  public Schema getSchema() {
    return schema;
  }

  public void write(Object object, Encoder encoder) throws IOException {
    Set<Object> seenRefs = Sets.newIdentityHashSet();
    write(object, encoder, schema, seenRefs);
  }

  private void write(Object object, Encoder encoder, Schema objSchema, Set<Object> seenRefs) throws IOException {
    if(seenRefs.contains(object)) {
      throw new IOException("Circular reference not supported.");
    }
    seenRefs.add(object);

    switch(objSchema.getType()) {
      case NULL:
        encoder.writeNull();
        break;
      case BOOLEAN:
        encoder.writeBool((Boolean) object);
        break;
      case INT:
        encoder.writeInt(( (Number) object ).intValue());
        break;
      case LONG:
        encoder.writeLong(( (Number) object ).longValue());
        break;
      case FLOAT:
        encoder.writeFloat((Float) object);
        break;
      case DOUBLE:
        encoder.writeDouble((Double) object);
        break;
      case STRING:
        encoder.writeString(object.toString());
        break;
      case BYTES:
        writeBytes(object, encoder);
        break;
      case ENUM:
        writeEnum(object.toString(), encoder, objSchema);
        break;
      case ARRAY:
        writeArray(object, encoder, objSchema.getComponentSchema(), seenRefs);
        break;
      case MAP:
        writeMap(object, encoder, objSchema.getMapSchema(), seenRefs);
        break;
      case RECORD:
        writeRecord(object, encoder, objSchema, seenRefs);
    }
  }

  private void writeBytes(Object object, Encoder encoder) throws IOException {
    if(object instanceof ByteBuffer) {
      encoder.writeBytes((ByteBuffer) object);
    } else if (object instanceof UUID) {
      UUID uuid = (UUID)object;
      ByteBuffer buf = ByteBuffer.allocate(Longs.BYTES * 2);
      buf.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
      encoder.writeBytes((ByteBuffer)buf.flip());
    } else {
      encoder.writeBytes((byte[]) object);
    }
  }

  private void writeEnum(String value, Encoder encoder, Schema schema) throws IOException {
    int idx = schema.getEnumIndex(value);
    if(idx < 0) {
      throw new IOException("Invalid enum value " + value);
    }
    encoder.writeInt(idx);
  }

  private void writeArray(Object array, Encoder encoder,
                          Schema componentSchema, Set<Object> seenRefs) throws IOException {
    if(array instanceof Collection) {
      Collection col = (Collection) array;
      encoder.writeInt(col.size());
      for(Object obj : col) {
        write(obj, encoder, componentSchema, seenRefs);
      }
    } else {
      int len = Array.getLength(array);
      encoder.writeInt(len);
      for(int i = 0; i < len; i++) {
        write(Array.get(array, i), encoder, componentSchema, seenRefs);
      }
    }
    encoder.writeInt(0);
  }

  private void writeMap(Object map, Encoder encoder, Map.Entry<Schema,
                                                                Schema> mapSchema,
                        Set<Object> seenRefs) throws IOException {
    Map<?, ?> objMap = (Map<?, ?>) map;
    encoder.writeInt(objMap.size());
    for(Map.Entry<?, ?> entry : objMap.entrySet()) {
      write(entry.getKey(), encoder, mapSchema.getKey(), seenRefs);
      write(entry.getValue(), encoder, mapSchema.getValue(), seenRefs);
    }
  }

  private void writeRecord(Object record, Encoder encoder,
                           Schema recordSchema, Set<Object> seenRefs) throws IOException {
    try {
      TypeToken<?> type = TypeToken.of(record.getClass());

      Map<String, Method> methods = collectByMethod(type, Maps.<String, Method>newHashMap());
      Map<String, Field> fields = collectByFields(type, Maps.<String, Field>newHashMap());

      for(Schema.Field field : recordSchema.getFields()) {
        String fieldName = field.getName();
        Object value;
        Field recordField = fields.get(fieldName);
        if(recordField != null) {
          recordField.setAccessible(true);
          value = recordField.get(record);
        } else {
          Method method = methods.get(fieldName);
          if(method == null) {
            throw new IOException("Unable to read field value through getter. Class=" + type + ", field=" + fieldName);
          }
          value = method.invoke(record);
        }

        Schema fieldSchema = field.getSchema();
        if(fieldSchema.getType() == Schema.Type.UNION) {
          // It's assumed that 0 is for the actual type, 1 is for null value.
          if(value == null) {
            encoder.writeInt(1);
            fieldSchema = fieldSchema.getUnionSchemas().get(1);
          } else {
            encoder.writeInt(0);
            fieldSchema = fieldSchema.getUnionSchemas().get(0);
          }
        }
        write(value, encoder, fieldSchema, seenRefs);
      }
    } catch(Exception e) {
      if(e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException(e);
    }
  }

  private Map<String, Field> collectByFields(TypeToken<?> typeToken, Map<String, Field> fields) {
    // Collect the field types
    for(TypeToken<?> classType : typeToken.getTypes().classes()) {
      Class<?> rawType = classType.getRawType();
      if(rawType.equals(Object.class)) {
        // Ignore all object fields
        continue;
      }

      for(Field field : rawType.getDeclaredFields()) {
        if(Modifier.isTransient(field.getModifiers()) || field.isSynthetic()) {
          continue;
        }
        fields.put(field.getName(), field);
      }
    }
    return fields;
  }

  private Map<String, Method> collectByMethod(TypeToken<?> typeToken, Map<String, Method> methods) {
    for(Method method : typeToken.getRawType().getMethods()) {
      if(method.getDeclaringClass().equals(Object.class)) {
        // Ignore all object methods
        continue;
      }
      String methodName = method.getName();
      if(!( methodName.startsWith("get") || methodName.startsWith("is") )
           || method.isSynthetic() || method.getParameterTypes().length != 0) {
        // Ignore not getter methods
        continue;
      }
      String fieldName = methodName.startsWith("get") ?
                           methodName.substring("get".length()) : methodName.substring("is".length());
      if(fieldName.isEmpty()) {
        continue;
      }
      fieldName = String.format("%c%s", Character.toLowerCase(fieldName.charAt(0)), fieldName.substring(1));
      if(methods.containsKey(fieldName)) {
        continue;
      }
      methods.put(fieldName, method);
    }
    return methods;
  }
}
