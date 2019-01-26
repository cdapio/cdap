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

/**
 * A field validator class which can be used to validate the given field.
 */
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
    if (!expected.equals(actual)) {
      throw new InvalidFieldException(tableSchema.getTableId(), fieldName, expected, actual);
    }

    if (tableSchema.isPrimaryKeyColumn(fieldName) && field.getValue() == null) {
      throw new InvalidFieldException(tableSchema.getTableId(), fieldName, "is a primary key but the value is null");
    }
  }
}
