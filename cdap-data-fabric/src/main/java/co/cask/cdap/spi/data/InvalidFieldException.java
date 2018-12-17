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

package co.cask.cdap.spi.data;

import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.table.field.FieldType;

/**
 * Exception thrown when a field is not part of the table schema.
 */
public class InvalidFieldException extends IllegalArgumentException {
  private final String fieldName;
  private final StructuredTableId tableId;

  public InvalidFieldException(StructuredTableId tableId, String fieldName) {
    super(String.format("Field %s is not part of the schema of table %s",
                        fieldName, tableId.getName()));
    this.tableId = tableId;
    this.fieldName = fieldName;
  }

  public InvalidFieldException(StructuredTableId tableId, String fieldName, String definition) {
    super(String.format("Field %s is not defined as %s of table %s",
                        fieldName, definition, tableId.getName()));
    this.tableId = tableId;
    this.fieldName = fieldName;
  }

  public InvalidFieldException(StructuredTableId tableId, String fieldName, FieldType.Type expected,
                               FieldType.Type actual) {
    super(String.format("Wrong type expected for field %s in table %s. Expected %s, actual %s",
                        fieldName, tableId.getName(), expected, actual));
    this.tableId = tableId;
    this.fieldName = fieldName;
  }
  public StructuredTableId getTableId() {
    return tableId;
  }

  public String getFieldName() {
    return fieldName;
  }
}
