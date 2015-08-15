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

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.lang.reflect.Type;

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
  public static Field findField(Type classType, String fieldName) throws NoSuchFieldException {
    for (Class<?> clz : TypeToken.of(classType).getTypes().classes().rawTypes()) {
      try {
        return clz.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
        // OK to ignore, keep finding.
      }
    }
    throw new NoSuchFieldException("Field " + fieldName + " not exists in the class hierarchy of " + classType);
  }

  private Fields() {}
}
