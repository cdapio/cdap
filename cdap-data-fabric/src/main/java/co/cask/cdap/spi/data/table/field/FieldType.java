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

import co.cask.cdap.spi.data.InvalidFieldException;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;

/**
 * Contains the name and type information of a {@link Field}.
 */
public final class FieldType {
  /**
   * Supported data types.
   */
  public enum Type {
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    STRING
  }

  static final Set<Type> PRIMARY_KEY_TYPES = ImmutableSet.of(Type.INTEGER, Type.LONG, Type.STRING);

  private final String name;
  private final Type type;

  /**
   * Construct a field type with the given field name and type.
   *
   * @param name field name
   * @param type field type
   */
  public FieldType(String name, Type type) {
    this.name = name;
    this.type = type;
  }

  /**
   * @return the field name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the field type
   */
  public Type getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FieldType fieldType = (FieldType) o;
    return Objects.equals(name, fieldType.name) &&
      type == fieldType.type;
  }

  @Override
  public int hashCode() {

    return Objects.hash(name, type);
  }

  @Override
  public String toString() {
    return "FieldType{" +
      "name='" + name + '\'' +
      ", type=" + type +
      '}';
  }
}
