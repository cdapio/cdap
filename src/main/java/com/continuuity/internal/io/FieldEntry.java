package com.continuuity.internal.io;

import com.google.common.base.Objects;
import com.google.common.reflect.TypeToken;

/**
*
*/
final class FieldEntry {
  private final TypeToken<?> type;
  private final String fieldName;

  FieldEntry(TypeToken<?> type, String fieldName) {
    this.type = type;
    this.fieldName = fieldName;
  }

  public TypeToken<?> getType() {
    return type;
  }

  public String getFieldName() {
    return fieldName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FieldEntry other = (FieldEntry) o;
    return type.equals(other.type) && fieldName.equals(other.fieldName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type, fieldName);
  }
}
