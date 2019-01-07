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

  public InvalidFieldException(String message) {
    super(message);
    this.fieldName = null;
    this.tableId = null;
  }

  /**
   * Create an exception when a field is not part of a table schema
   * @param tableId table
   * @param fieldName missing field name
   */
  public InvalidFieldException(StructuredTableId tableId, String fieldName) {
    super(String.format("Field %s is not part of the schema of table %s",
                        fieldName, tableId.getName()));
    this.tableId = tableId;
    this.fieldName = fieldName;
  }

  /**
   * Create an exception when a field is not defined as a primary key or an index, but is used as one
   * @param tableId table
   * @param fieldName wrongly used field name
   * @param message the message which specifies the wrong usage
   */
  public InvalidFieldException(StructuredTableId tableId, String fieldName, String message) {
    super(String.format("Field %s of table %s %s", fieldName, tableId.getName(), message));
    this.tableId = tableId;
    this.fieldName = fieldName;
  }

  /**
   * Create an exception when a field is needs conversion to an incompatible type than what is defined.
   * @param tableId table
   * @param fieldName field name
   * @param expected expected type of the field
   * @param actual actual type of the field
   */
  public InvalidFieldException(StructuredTableId tableId, String fieldName, FieldType.Type expected,
                               FieldType.Type actual) {
    super(String.format("Wrong type for field %s in table %s. Expected %s, actual %s",
                        fieldName, tableId.getName(), expected, actual));
    this.tableId = tableId;
    this.fieldName = fieldName;
  }

  /**
   * @return the table id
   */
  public StructuredTableId getTableId() {
    return tableId;
  }

  /**
   * @return return the field name causing the exception
   */
  public String getFieldName() {
    return fieldName;
  }
}
