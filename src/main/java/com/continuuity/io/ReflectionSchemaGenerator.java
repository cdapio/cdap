package com.continuuity.io;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
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

    // Collect the field types and sort by field name
    for (TypeToken<?> classType : typeToken.getTypes().classes()) {
      Class<?> rawType = classType.getRawType();
      for (Field field : rawType.getDeclaredFields()) {
        if (Modifier.isTransient(field.getModifiers()) || field.isSynthetic()) {
          continue;
        }
        TypeToken<?> fieldType = classType.resolveType(field.getGenericType());
        recordFieldTypes.put(field.getName(), fieldType);
      }
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
}
