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

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.spi.data.table.StructuredTableSpecification;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.FieldFactory;
import co.cask.cdap.spi.data.table.field.Range;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

/**
 * Abstraction for a table that contains rows and columns.
 * The schema of the table is fixed, and has to be specified in the
 * {@link StructuredTableSpecification} during the table creation.
 */
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
   * Read a single row with the specified columns from the table.
   *
   * @param keys the primary key of the row to read
   * @param columns the columns to read. Empty collection returns all the columns
   * @return the row addressed by the primary key
   * @throws InvalidFieldException if any of the keys are not part of the table schema, or the types of the value
   *                               do not match
   * @throws IOException if there is an error reading from the table
   */
  Optional<StructuredRow> read(Collection<Field<?>> keys,
                               Collection<String> columns) throws InvalidFieldException, IOException;

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
   * Delete a single row from the table.
   *
   * @param keys the primary key of the row to delete
   * @throws InvalidFieldException if any of the keys are not part of the table schema, or the types of the value
   *                               do not match
   * @throws IOException if there is an error deleting from the table
   */
  void delete(Collection<Field<?>> keys) throws InvalidFieldException, IOException;

  /**
   * Get a field factory to generate field for this table, the factory will gaurantee the type of the value of the
   * field is correct.
   *
   * @return the field factory
   */
  FieldFactory getFieldFactory();
}
