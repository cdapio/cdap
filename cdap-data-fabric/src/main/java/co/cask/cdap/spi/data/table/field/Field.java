/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.spi.data.table.field;

import co.cask.cdap.api.common.Bytes;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents a column of a table, and its value.
 * @param <T> the type of the value. Valid types for regular fields are int, long, double, float and string.
 *           Valid types for primary keys are int, long and string.
 */
public final class Field<T> {
  private final FieldType fieldType;
  private final T value;

  Field(FieldType fieldType, @Nullable T value) {
    this.fieldType = fieldType;
    this.value = value;
  }

  /**
   * @return the type of the field
   */
  public FieldType.Type getFieldType() {
    return fieldType.getType();
  }

  /**
   * @return the field name
   */
  public String getName() {
    return fieldType.getName();
  }

  /**
   * @return the field value
   */
  @Nullable
  public T getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Field field = (Field) o;
    if (!Objects.equals(fieldType, field.fieldType)) {
      return false;
    }
    if (fieldType.getType() == FieldType.Type.BYTES) {
      return Bytes.equals((byte[]) value, (byte[]) field.getValue());
    }
    return Objects.equals(value, field.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldType, value);
  }

  @Override
  public String toString() {
    return "Field{" +
      "name='" + fieldType.getName() + '\'' +
      ", type='" + fieldType.getType() + '\'' +
      ", value='" + value + '\'' +
      '}';
  }
}
