package com.continuuity.api.io;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;

/**
 * This class uses Java Reflection to inspect fields in any Java class to generate RECORD schema.
 */
public final class ReflectionSchemaGenerator extends AbstractSchemaGenerator {

  @Override
  protected Schema generateRecord(TypeToken<?> typeToken, Set<String> knowRecords) throws UnsupportedTypeException {
    String recordName = typeToken.getRawType().getName();
    Map<String, TypeToken<?>> recordFieldTypes = Maps.newTreeMap();
    knowRecords.add(recordName);

    collectByFields(typeToken, recordFieldTypes);
    collectByMethod(typeToken, recordFieldTypes);

    // Recursively generate field type schema.
    ImmutableList.Builder<Schema.Field> builder = ImmutableList.builder();
    for (Map.Entry<String, TypeToken<?>> fieldType : recordFieldTypes.entrySet()) {
      Schema fieldSchema = doGenerate(fieldType.getValue(), knowRecords);

      if (!fieldType.getValue().getRawType().isPrimitive()) {
        // For non-primitive, allows "null" value.
        fieldSchema = Schema.unionOf(Schema.of(Schema.Type.NULL), fieldSchema);
      }
      builder.add(Schema.Field.of(fieldType.getKey(), fieldSchema));
    }

    return Schema.recordOf(recordName, builder.build());
  }

  private void collectByFields(TypeToken<?> typeToken, Map<String, TypeToken<?>> fieldTypes) {
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
        TypeToken<?> fieldType = classType.resolveType(field.getGenericType());
        fieldTypes.put(field.getName(), fieldType);
      }
    }
  }

  private void collectByMethod(TypeToken<?> typeToken, Map<String, TypeToken<?>> fieldTypes) {
    for (Method method : typeToken.getRawType().getMethods()) {
      if (method.getDeclaringClass().equals(Object.class)) {
        // Ignore all object methods
        continue;
      }
      String methodName = method.getName();
      if (!methodName.startsWith("get") || method.isSynthetic() || method.getParameterTypes().length != 0) {
        // Ignore not getter methods
        continue;
      }
      String fieldName = methodName.substring("get".length());
      if (fieldName.isEmpty()) {
        continue;
      }
      fieldName = String.format("%c%s", Character.toLowerCase(fieldName.charAt(0)), fieldName.substring(1));
      if (fieldTypes.containsKey(fieldName)) {
        continue;
      }
      TypeToken<?> fieldType = typeToken.resolveType(method.getGenericReturnType());
      fieldTypes.put(fieldName, fieldType);
    }
  }
}
