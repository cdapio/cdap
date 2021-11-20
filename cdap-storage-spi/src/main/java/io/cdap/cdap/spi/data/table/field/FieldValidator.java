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

package io.cdap.cdap.spi.data.table.field;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;

import java.util.Collection;
import java.util.List;

/**
 * A field validator class which can be used to validate the given field.
 */
@Beta
public final class FieldValidator {
  private final StructuredTableSchema tableSchema;

  public FieldValidator(StructuredTableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  /**
   * Validate if the given field matches the schema. The given field is invalid if: it is not present in the given
   * schema, its type is different than the given schema, or if it is a primary key but the given value is null.
   *
   * @param field the field to validate
   * @throws InvalidFieldException if the field does not pass the validation
   */
  public void validateField(Field<?> field) throws InvalidFieldException {
    String fieldName = field.getName();
    FieldType.Type expected = tableSchema.getType(fieldName);
    FieldType.Type actual = field.getFieldType();
    if (expected == null) {
      throw new InvalidFieldException(tableSchema.getTableId(), fieldName);
    }
    if (!expected.isCompatible(actual)) {
      throw new InvalidFieldException(tableSchema.getTableId(), fieldName, expected, actual);
    }

    if (tableSchema.isPrimaryKeyColumn(fieldName) && field.getValue() == null) {
      throw new InvalidFieldException(tableSchema.getTableId(), fieldName, "is a primary key but the value is null");
    }
  }


  /**
   * Validate if the given keys are prefix or complete primary keys.
   *
   * @param keys the keys to validate
   * @param allowPrefix boolean to indicate whether the given collection keys can be a prefix of the primary keys
   * @throws InvalidFieldException if the given keys have extra key which is not in primary key, or are not in correct
   * order of the primary keys or are not complete keys if allowPrefix is set to false.
   */
  public void validatePrimaryKeys(Collection<Field<?>> keys, boolean allowPrefix) throws InvalidFieldException {
    List<String> primaryKeys = tableSchema.getPrimaryKeys();
    if (keys.size() > primaryKeys.size()) {
      throw new InvalidFieldException(
        tableSchema.getTableId(), keys,
        String.format("Given keys %s contain more fields than the primary keys %s", keys, primaryKeys));
    }

    if (!allowPrefix && keys.size() < primaryKeys.size()) {
      throw new InvalidFieldException(
        tableSchema.getTableId(), keys,
        String.format("Given keys %s do not contain all the primary keys %s", keys, primaryKeys));
    }

    int i = 0;
    for (Field<?> key : keys) {
      validateField(key);
      if (!key.getName().equals(primaryKeys.get(i))) {
        throw new InvalidFieldException(
          tableSchema.getTableId(), keys,
          String.format("Given keys %s are not the prefix of the primary keys %s", keys, primaryKeys));
      }
      i++;
    }
  }
}
