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

package io.cdap.cdap.spi.data;

import io.cdap.cdap.spi.data.table.StructuredTableId;

/**
 * Exception thrown when a field value exceeds the allowed storage limit.
 */
public class FieldSizeLimitExceededException extends RuntimeException {

  private final StructuredTableId tableId;
  private final String field;
  private final int size;
  private final int limit;

  /**
   * Constructs a new FieldSizeLimitExceededException.
   *
   * @param tableId   the ID of the table containing the field
   * @param fieldName the name of the field that exceeded the limit
   * @param size      the actual size of the field value
   * @param limit     the maximum allowed size for the field type
   */
  public FieldSizeLimitExceededException(StructuredTableId tableId, String fieldName, int size,
      int limit) {
    super(
        String.format("Field %s.%s exceeds storage limit, size : %s limit : %s", tableId,
            fieldName, size, limit));
    this.field = fieldName;
    this.tableId = tableId;
    this.size = size;
    this.limit = limit;
  }

  /**
   * @return the table ID
   */
  public StructuredTableId getTableId() {
    return tableId;
  }

  /**
   * @return the field name
   */
  public String getField() {
    return field;
  }

  /**
   * @return the actual size of the field value
   */
  public int getSize() {
    return size;
  }

  /**
   * @return the size limit
   */
  public int getLimit() {
    return limit;
  }
}
