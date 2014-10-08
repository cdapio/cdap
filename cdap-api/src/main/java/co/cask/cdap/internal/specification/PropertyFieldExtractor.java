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
package co.cask.cdap.internal.specification;

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.internal.lang.FieldVisitor;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.google.gson.internal.Primitives;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * A {@link FieldVisitor} that extracts {@link Property} fields and save them into a map.
 * For keys that are already exists in the property map, it keep them as is and not overwriting them.
 */
public final class PropertyFieldExtractor extends FieldVisitor {

  private final Map<String, String> properties;

  /**
   * Constructs a {@link PropertyFieldExtractor} that stores results to the given map.
   */
  public PropertyFieldExtractor(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public void visit(Object instance, TypeToken<?> inspectType, TypeToken<?> declareType, Field field) throws Exception {
    if (field.isAnnotationPresent(Property.class)) {

      // Key name is "className.fieldName".
      String key = declareType.getRawType().getName() + '.' + field.getName();
      if (properties.containsKey(key)) {
        return;
      }

      String value = getStringValue(instance, field);
      if (value != null) {
        properties.put(key, value);
      }
    }
  }

  /**
   * Gets the value of the field in the given instance as String.
   * Currently only allows primitive types, boxed types, String and Enum.
   */
  private String getStringValue(Object instance, Field field) throws IllegalAccessException {
    Class<?> fieldType = field.getType();

    // Only support primitive type, boxed type, String and Enum
    Preconditions.checkArgument(
      fieldType.isPrimitive() || Primitives.isWrapperType(fieldType) ||
        String.class.equals(fieldType) || fieldType.isEnum(),
      "Unsupported property type %s of field %s in class %s.",
      fieldType.getName(), field.getName(), field.getDeclaringClass().getName());

    if (!field.isAccessible()) {
      field.setAccessible(true);
    }
    Object value = field.get(instance);
    if (value == null) {
      return null;
    }

    // Key name is "className.fieldName".
    return fieldType.isEnum() ? ((Enum<?>) value).name() : value.toString();
  }
}
