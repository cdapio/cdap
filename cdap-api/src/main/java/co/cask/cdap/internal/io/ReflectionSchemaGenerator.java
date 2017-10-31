/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.internal.guava.reflect.TypeToken;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This class uses Java Reflection to inspect fields in any Java class to generate RECORD schema.
 * <p/>
 * <p>
 * If the given type is a class, it will uses the class fields (includes all the fields in parent classes)
 * to generate the schema. All fields, no matter what access it is would be included, except transient or
 * synthetic one.
 * </p>
 * <p/>
 * <p>
 * If the given type is an interface, it will uses all getter methods (methods that prefix with "get" or "is",
 * followed by a name with no arguments) to generate fields for the record schema.
 * E.g. for the method {@code String getFirstName()}, a field name "firstName" of type String would be generated.
 * </p>
 */
public final class ReflectionSchemaGenerator extends AbstractSchemaGenerator {

  private final boolean isNullableByDefault;

  public ReflectionSchemaGenerator(boolean isNullableByDefault) {
    this.isNullableByDefault = isNullableByDefault;
  }

  public ReflectionSchemaGenerator() {
    this(true);
  }

  @Override
  protected Schema generateRecord(TypeToken<?> typeToken, Set<String> knowRecords, boolean acceptRecursion)
    throws UnsupportedTypeException {
    String recordName = typeToken.getRawType().getName();
    Map<String, TypeToken<?>> recordFieldTypes =
      typeToken.getRawType().isInterface() ?
        collectByMethods(typeToken, new TreeMap<>()) :
        collectByFields(typeToken, new TreeMap<>());

    // Recursively generate field type schema.
    List<Schema.Field> fields = new ArrayList<>();
    for (Map.Entry<String, TypeToken<?>> fieldType : recordFieldTypes.entrySet()) {
      Set<String> records = new HashSet<>(knowRecords);
      records.add(recordName);
      Schema fieldSchema = doGenerate(fieldType.getValue(), records, acceptRecursion);

      if (!fieldType.getValue().getRawType().isPrimitive()) {
        boolean isNotNull = typeToken.getRawType().isAnnotationPresent(Nonnull.class);
        boolean isNull = typeToken.getRawType().isAnnotationPresent(Nullable.class);

        // For non-primitive, allows "null" value
        // i) if it is nullable by default and notnull annotation is not present
        // ii) if it is not nullable by default and nullable annotation is present
        if ((isNullableByDefault && !isNotNull) || (!isNullableByDefault && isNull)) {
          fieldSchema = Schema.unionOf(fieldSchema, Schema.of(Schema.Type.NULL));
        }
      }
      fields.add(Schema.Field.of(fieldType.getKey(), fieldSchema));
    }

    return Schema.recordOf(recordName, Collections.unmodifiableList(fields));
  }

  private Map<String, TypeToken<?>> collectByFields(TypeToken<?> typeToken, Map<String, TypeToken<?>> fieldTypes) {
    // Collect the field types
    for (TypeToken<?> classType : typeToken.getTypes().classes()) {
      Class<?> rawType = classType.getRawType();
      if (rawType.equals(Object.class)) {
        // Ignore all object fields
        continue;
      }

      for (Field field : rawType.getDeclaredFields()) {
        int modifiers = field.getModifiers();
        if (Modifier.isTransient(modifiers) || Modifier.isStatic(modifiers) || field.isSynthetic()) {
          continue;
        }
        TypeToken<?> fieldType = classType.resolveType(field.getGenericType());
        fieldTypes.put(field.getName(), fieldType);
      }
    }
    return fieldTypes;
  }

  private Map<String, TypeToken<?>> collectByMethods(TypeToken<?> typeToken, Map<String, TypeToken<?>> fieldTypes) {
    for (Method method : typeToken.getRawType().getMethods()) {
      if (method.getDeclaringClass().equals(Object.class)) {
        // Ignore all object methods
        continue;
      }
      String methodName = method.getName();
      if (!(methodName.startsWith("get") || methodName.startsWith("is"))
           || method.isSynthetic() || Modifier.isStatic(method.getModifiers())
           || method.getParameterTypes().length != 0) {
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
    return fieldTypes;
  }
}
