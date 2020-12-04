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

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Range;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Abstraction for a table that contains rows and columns.
 * The schema of the table is fixed, and has to be specified in the
 * {@link StructuredTableSpecification} during the table creation.
 */
@Beta
public interface StructuredTable extends Closeable {
  /**
   * Insert or replace the collection of fields to the table.
   * The fields contain both the primary key and the rest of the columns to write.
   * Note that if the row does not exist in the table, it will get created. Otherwise it will get replaced.
   *
   * @param fields the fields to write
   * @throws InvalidFieldException if any of the fields are not part of the table schema, or the types of the value
   *                               do not match
   * @throws IOException if there is an error writing to the table
   */
  void upsert(Collection<Field<?>> fields) throws InvalidFieldException, IOException;

  /**
   * Read a single row with all the columns from the table.
   *
   * @param keys the primary key of the row to read
   * @return if the optional is not empty, the row addressed by the primary key.
   *         If the optional is empty then the row is missing in the table.
   * @throws InvalidFieldException if any of the keys are not part of the table schema, or the types of the value
   *                               do not match.
   * @throws IOException if there is an error reading from the table
   */
  Optional<StructuredRow> read(Collection<Field<?>> keys) throws InvalidFieldException, IOException;

  /**
   * Read a single row with the specified columns from the table. The primary keys will also be contained in the
   * columns.
   *
   * @param keys the primary key of the row to read
   * @param columns the columns to read. This collection must not be empty, otherwise InvalidFieldException will be
   *                thrown
   * @return the row addressed by the primary key
   * @throws InvalidFieldException if any of the keys are not part of the table schema, or the types of the value
   *                               do not match
   * @throws IOException if there is an error reading from the table
   */
  Optional<StructuredRow> read(Collection<Field<?>> keys,
                               Collection<String> columns) throws InvalidFieldException, IOException;

  /**
   * Reads multiple rows with all the columns from the table. The default implementation is to call
   * {@link #read(Collection)} one by one. Implementations of this interface can provide an optimized version.
   *
   * @param multiKeys a collection of keys to read
   * @return a {@link Collection} of {@link StructuredRow} that are present in the table
   * @throws InvalidFieldException if any of the keys are not part of the table schema, or the types of the value
   *                               do not match
   * @throws IOException if there is an error reading from the table
   */
  default Collection<StructuredRow> multiRead(Collection<? extends Collection<Field<?>>> multiKeys)
    throws InvalidFieldException, IOException {
    List<StructuredRow> result = new ArrayList<>();
    for (Collection<Field<?>> keys : multiKeys) {
      read(keys).ifPresent(result::add);
    }
    return result;
  }

  /**
   * Read a set of rows from the table matching the key range.
   * The rows returned will be sorted on the primary key order.
   *
   * @param keyRange key range for the scan
   * @param limit maximum number of rows to return
   * @return a {@link CloseableIterator} of rows
   * @throws InvalidFieldException if any of the keys are not part of the table schema, or the types of the value
   *                               do not match
   * @throws IOException if there is an error scanning the table
   */
  CloseableIterator<StructuredRow> scan(Range keyRange, int limit) throws InvalidFieldException, IOException;

  /**
   * Read a set of rows from the table matching the index.
   * The rows returned will be sorted on the primary key order.
   *
   * @param index the index value
   * @return a {@link CloseableIterator} of rows
   * @throws InvalidFieldException if the field is not part of the table schema, or is not an indexed column,
   *                               or the type does not match the schema
   * @throws IOException if there is an error scanning the table
   */
  CloseableIterator<StructuredRow> scan(Field<?> index) throws InvalidFieldException, IOException;

  /**
   * Read a set of rows from the table matching the set of key ranges.
   * The rows returned will be sorted on the primary key order.
   *
   * @param keyRanges collection of key ranges for the scan
   * @param limit maximum number of rows to return
   * @return a {@link CloseableIterator} of rows
   * @throws InvalidFieldException if any of the keys are not part of the table schema, or the types of the value
   *                               do not match
   * @throws IOException if there is an error scanning the table
   */
  CloseableIterator<StructuredRow> multiScan(Collection<Range> keyRanges,
                                             int limit) throws InvalidFieldException, IOException;

  /**
   * Atomically compare and swap the value of a column in a row if the expected value matches.
   * To match a non-existent value, the value of the expected field should be null.
   *
   * @param keys the primary key of row
   * @param oldValue the expected value in the table. The field cannot be part of the primary key
   * @param newValue the new value to write if the expected value matches
   * @return true if the compare and swap was successful, false otherwise
   * @throws InvalidFieldException if any of the keys/fields are not part of table schema,
   *                               or their types do not match the schema
   * @throws IOException if there is an error reading or writing to the table
   * @throws IllegalArgumentException if the field name or the type of the oldValue and newValue do not match
   */
  boolean compareAndSwap(Collection<Field<?>> keys, Field<?> oldValue, Field<?> newValue)
    throws InvalidFieldException, IOException, IllegalArgumentException;

  /**
   * Atomically increment a column of type LONG in a row.
   *
   * @param keys the primary key of row
   * @param column the column name to increment, cannot be part of the primary key
   * @param amount the amount of increment
   * @throws InvalidFieldException if any of the keys/column are not part of table schema,
   *    *                          or their types do not match the schema
   * @throws IOException if there is an error reading or writing to the table
   * @throws IllegalArgumentException if the column type is not LONG
   */
  void increment(Collection<Field<?>> keys, String column, long amount)
    throws InvalidFieldException, IOException, IllegalArgumentException;

  /**
   * Delete a single row from the table.
   *
   * @param keys the primary key of the row to delete
   * @throws InvalidFieldException if any of the keys are not part of the table schema, or the types of the value
   *                               do not match
   * @throws IOException if there is an error deleting from the table
   */
  void delete(Collection<Field<?>> keys) throws InvalidFieldException, IOException;

  /**
   * Delete a range of rows from the table.
   *
   * @param keyRange key range of the rows to delete
   * @throws InvalidFieldException if any of the keys are not part of table schema,
   *                               or their types do not match the schema
   * @throws IOException if there is an error reading or deleting from the table
   */
  void deleteAll(Range keyRange) throws InvalidFieldException, IOException;

  /**
   * Get the number of records in the table.
   * @param keyRanges key ranges of the rows to count
   * @return number of records in the table
   * @throws IOException if there is an error reading from the table
   */
  long count(Collection<Range> keyRanges) throws IOException;
}
