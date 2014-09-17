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

package co.cask.cdap.internal.lang;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;

/**
 *
 */
public final class Fields {

  /**
   * Find a {@link Field} in the class hierarchy of the given type.
   * @param classType The leaf class to start with.
   * @param fieldName Name of the field.
   * @return A {@link Field} if found.
   * @throws NoSuchFieldException If the field is not found.
   */
  public static Field findField(TypeToken<?> classType, String fieldName) throws NoSuchFieldException {
    return findField(classType, fieldName, Predicates.<Field>alwaysTrue());
  }

  /**
   * Find a {@link Field} in the class hierarchy of the given type that passes the predicate.
   * @param classType The leaf class to start with.
   * @param fieldName Name of the field.
   * @param predicate Predicate for accepting a matched field.
   * @return A {@link Field} if found.
   * @throws NoSuchFieldException If the field is not found.
   */
  public static Field findField(TypeToken<?> classType, String fieldName,
                                Predicate<Field> predicate) throws NoSuchFieldException {
    for (Class<?> clz : classType.getTypes().classes().rawTypes()) {
      try {
        Field field = clz.getDeclaredField(fieldName);
        if (predicate.apply(field)) {
          return field;
        }
      } catch (NoSuchFieldException e) {
        // OK to ignore, keep finding.
      }
    }
    throw new NoSuchFieldException("Field " + fieldName + " not exists in the class hierarchy of " + classType);
  }

  private Fields() {}
}
