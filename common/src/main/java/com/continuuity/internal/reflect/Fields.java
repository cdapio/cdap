package com.continuuity.internal.reflect;

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
    // TODO: This method is not exactly ASM, might find a better package to host it.
    for (TypeToken<?> type : classType.getTypes().classes()) {
      try {
        return type.getRawType().getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
        // OK to ignore, keep finding.
      }
    }
    throw new IllegalArgumentException("Field " + fieldName + " not exists in the class hierarchy of " + classType);
  }

  private Fields() {}
}
