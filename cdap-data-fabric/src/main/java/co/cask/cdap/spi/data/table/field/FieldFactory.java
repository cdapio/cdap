/*
 * Copyright © 2019 Cask Data, Inc.
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
import co.cask.cdap.spi.data.table.StructuredTableSchema;

import javax.annotation.Nullable;

/**
 * A field factory class which will guarantee the correct type of value of the field is getting created.
 */
public final class FieldFactory {
  private final StructuredTableSchema tableSchema;

  public FieldFactory(StructuredTableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  /**
   * Create a {@link Field} which has value of Integer type.
   *
   * @param fieldName field name
   * @param value the value of the field
   * @return a field with Integer as value
   * @throws InvalidFieldException if the field name is not in schema or the type does not match or the field is a key
   * but provided value is null.
   */
  public Field<Integer> createIntField(String fieldName, @Nullable Integer value) throws InvalidFieldException {
   return validateAndCreateField(fieldName, FieldType.Type.INTEGER, tableSchema.getType(fieldName), value);
  }

  /**
   * Create a {@link Field} which has value of Long type.
   *
   * @param fieldName field name
   * @param value the value of the field
   * @return a field with Long as value
   * @throws InvalidFieldException if the field name is not in schema or the type does not match or the field is a key
   * but provided value is null.
   */
  public Field<Long> createLongField(String fieldName, @Nullable Long value) throws InvalidFieldException {
    return validateAndCreateField(fieldName, FieldType.Type.LONG, tableSchema.getType(fieldName), value);
  }

  /**
   * Create a {@link Field} which has value of String type.
   *
   * @param fieldName field name
   * @param value the value of the field
   * @return a field with String as value
   * @throws InvalidFieldException if the field name is not in schema or the type does not match or the field is a key
   * but provided value is null.
   */
  public Field<String> createStringField(String fieldName, @Nullable String value) throws InvalidFieldException {
    return validateAndCreateField(fieldName, FieldType.Type.STRING, tableSchema.getType(fieldName), value);
  }

  /**
   * Create a {@link Field} which has value of Double type.
   *
   * @param fieldName field name
   * @param value the value of the field
   * @return a field with Double as value
   * @throws InvalidFieldException if the field name is not in schema or the type does not match or the field is a key
   * but provided value is null.
   */
  public Field<Double> createDoubleField(String fieldName, @Nullable Double value) throws InvalidFieldException {
    return validateAndCreateField(fieldName, FieldType.Type.DOUBLE, tableSchema.getType(fieldName), value);
  }

  /**
   * Create a {@link Field} which has value of Float type.
   *
   * @param fieldName field name
   * @param value the value of the field
   * @return a field with Float as value
   * @throws InvalidFieldException if the field name is not in schema or the type does not match or the field is a key
   * but provided value is null.
   */
  public Field<Float> createFloatField(String fieldName, @Nullable Float value) throws InvalidFieldException {
    return validateAndCreateField(fieldName, FieldType.Type.FLOAT, tableSchema.getType(fieldName), value);
  }

  private <T> Field<T> validateAndCreateField(String fieldName, FieldType.Type expected,
                                          @Nullable FieldType.Type actual,
                                          @Nullable T value) throws InvalidFieldException {
    if (actual == null || !actual.equals(expected)) {
      throw new InvalidFieldException(tableSchema.getTableId(), fieldName, expected, actual);
    }

    if (tableSchema.isPrimaryKeyColumn(fieldName) && value == null) {
      throw new InvalidFieldException(tableSchema.getTableId(), fieldName, "is a primary key but the value is null");
    }
    return new Field<>(fieldName, value);
  }
}
