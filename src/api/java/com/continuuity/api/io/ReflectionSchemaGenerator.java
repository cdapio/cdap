/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

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
 *
 * <p>
 *   If the given type is a class, it will uses the class fields (includes all the fields in parent classes)
 *   to generate the schema. All fields, no matter what access it is would be included, except transient or
 *   synthetic one.
 * </p>
 *
 * <p>
 *   If the given type is an interface, it will uses all getter methods (methods that prefix with "get" or "is",
 *   followed by a name with no arguments) to generate fields for the record schema.
 *   E.g. for the method {@code String getFirstName()}, a field name "firstName" of type String would be generated.
 * </p>
 */
public final class ReflectionSchemaGenerator extends AbstractSchemaGenerator {

  @Override
  protected Schema generateRecord(TypeToken<?> typeToken, Set<String> knowRecords) throws UnsupportedTypeException {
    String recordName = typeToken.getRawType().getName();
    Map<String, TypeToken<?>> recordFieldTypes = Maps.newTreeMap();
    knowRecords.add(recordName);

    if (typeToken.getRawType().isInterface()) {
      collectByMethod(typeToken, recordFieldTypes);
    } else {
      collectByFields(typeToken, recordFieldTypes);
    }

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
      if (fieldTypes.containsKey(fieldName)) {
        continue;
      }
      TypeToken<?> fieldType = typeToken.resolveType(method.getGenericReturnType());
      fieldTypes.put(fieldName, fieldType);
    }
  }
}
