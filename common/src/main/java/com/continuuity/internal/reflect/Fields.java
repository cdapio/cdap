package com.continuuity.internal.reflect;

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
   * @param classType
   * @param fieldName
   * @return
   */
  public static Field findField(TypeToken<?> classType, String fieldName) {
    return findField(classType, fieldName, Predicates.<Field>alwaysTrue());
  }

  public static Field findField(TypeToken<?> classType, String fieldName, Predicate<Field> predicate) {
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
    throw new IllegalArgumentException("Field " + fieldName + " not exists in the class hierarchy of " + classType);
  }

  private Fields() {}
}
