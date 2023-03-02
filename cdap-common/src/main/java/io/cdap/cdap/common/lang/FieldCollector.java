/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.lang;

import com.google.common.base.Defaults;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.internal.lang.FieldVisitor;
import io.cdap.cdap.internal.lang.Reflections;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Helper class to collect all non-static fields with defaults to set while creating an instance for
 * deserialization
 */
class FieldCollector extends FieldVisitor {

  private final Map<Field, Object> fieldsWithDefaults = new LinkedHashMap<>();

  @Override
  public void visit(Object instance, Type inspectType, Type declareType, Field field)
      throws Exception {
    if (Modifier.isStatic(field.getModifiers())) {
      return;
    }
    fieldsWithDefaults.put(field, Defaults.defaultValue(field.getType()));
  }

  /**
   * @return map of field name to default field value as defined by JLS for given type
   * @see Defaults#defaultValue(java.lang.Class)
   */
  static <T> Map<Field, Object> getFieldsWithDefaults(TypeToken<T> type) {
    FieldCollector fieldCollector = new FieldCollector();
    Reflections.visit(null, type.getType(), fieldCollector);
    return fieldCollector.fieldsWithDefaults;
  }
}
