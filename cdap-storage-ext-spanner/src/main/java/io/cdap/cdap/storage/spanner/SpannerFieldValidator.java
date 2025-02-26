/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.storage.spanner;

import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldValidator;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.storage.spanner.compression.CompressorType;
import java.util.Collection;

/**
 * Spanner specific implementation of {@link FieldValidator}.
 */
public class SpannerFieldValidator extends FieldValidator {

  private final SpannerStructuredTableSchema tableSchema;

  /**
   * Constructor for SpannerFieldValidator.
   *
   * @param tableSchema the {@link SpannerStructuredTableSchema} to be used.
   */
  public SpannerFieldValidator(SpannerStructuredTableSchema tableSchema) {
    super(tableSchema);
    this.tableSchema = tableSchema;
  }

  /**
   * Validates the given {@link Range}.
   *
   * @param range the {@link Range} to validate.
   * @throws InvalidFieldException if the range field is a compressed columns.
   */
  @Override
  public void validateScanRange(Range range) throws InvalidFieldException {
    super.validateScanRange(range);
    for (Field field : range.getBegin()) {
      CompressorType compressor = tableSchema.getCompressorType(field.getName());
      if (compressor != null) {
        throw new InvalidFieldException(tableSchema.getTableId(), field.getName(),
            "is compression enabled for which range queries are not allowed");
      }
    }
  }

  /**
   * Validates the given filter indexes.
   *
   * @param filterIndexes the filter indexes to validate.
   * @throws InvalidFieldException if any filter index is a compressed column.
   */
  public void validateFilterIndexes(Collection<Field<?>> filterIndexes) {
    for (Field field : filterIndexes) {
      CompressorType compressor = tableSchema.getCompressorType(field.getName());
      if (compressor != null) {
        throw new InvalidFieldException(tableSchema.getTableId(), field.getName(),
            "is compression enabled for which filter is not allowed");
      }
    }
  }
}
