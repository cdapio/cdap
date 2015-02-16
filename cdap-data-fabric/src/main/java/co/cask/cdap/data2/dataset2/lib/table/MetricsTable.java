/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.table.Scanner;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * A table interface dedicated to our metrics system. It is non-transactional, hence does not implement read or write
 * isolation. It provides all functions that the metrics system uses for its data.
 */
public interface MetricsTable extends Dataset {

  /**
   * Read single column of a row
   */
  @Nullable
  byte[] get(byte[] row, byte[] column) throws Exception;

  /**
   * Write multiple rows, each with multiple individual columns to write.
   */
  void put(NavigableMap<byte[], NavigableMap<byte[], Long>> updates) throws Exception;

  /**
   * Atomically compare a single column of a row with a expected value, and if it matches, replace it with a new value.
   * @param oldValue the expected value of the column. If null, this means that the column must not exist.
   * @param newValue the new value of the column. If null, the effect to delete the column if the comparison succeeds.
   * @return whether the write happened, that is, whether the existing value of the column matched the expected value.
   */
  boolean swap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) throws Exception;

  /**
   * Atomic increment of multiple columns of a row. If a column does not exist prior to the increment,
   * its value is assumed zero. Increments are only guaranteed atomic with respect to other increments.
   * @param increments Map from each column key to the value it should be incremented by.
   */
  void increment(byte[] row, Map<byte[], Long> increments) throws Exception;


  /**
   * Batch increment multiple rows each with multiple columns and increments
   */
  void increment(NavigableMap<byte[], NavigableMap<byte[], Long>> updates) throws Exception;

  /**
   * Increment a single column of a row and return the new value.
   * @return the new value after the increment.
   */
  long incrementAndGet(byte[] row, byte[] column, long delta) throws Exception;

  /**
   * Delete (all columns of) all rows that start with a given prefix.
   */
  void deleteAll(byte[] prefix) throws Exception;

  /**
   * Deletes specified columns of the specified row.
   *
   * @param row row to delete from
   * @param columns names of columns to delete
   */
  void delete(byte[] row, byte[][] columns) throws Exception;

  /**
   * Delete (all columns of) a set of rows.
   */
  void delete(Collection<byte[]> rows) throws Exception;

  /**
   * Scan the table from the start row to the stop row and delete all matching cells,
   * where the row and column match the given filter and column list.
   * @param start the row key of the first row to scan. If null, the scan begins at the first row of the table.
   * @param stop the row key of the last row to scan. If null, the scan goes to the last row of the table.
   * @param columns if non-null, delete only the given columns.
   * @param filter if non-null, a fuzzy row filter used to efficiently skip over entire rows.
   */
  void deleteRange(@Nullable byte[] start, @Nullable byte[] stop, @Nullable byte[][] columns,
                   @Nullable FuzzyRowFilter filter) throws Exception;


  /**
   * Get a scanner for a table.
   * @param start the row key of the first row to scan. If null, the scan begins at the first row of the table.
   * @param stop the row key of the last row to scan. If null, the scan goes to the last row of the table.
   * @param columns if non-null, return only the given columns.
   * @param filter if non-null, a fuzzy row filter used to efficiently skip over entire rows.
   */
  Scanner scan(@Nullable byte[] start, @Nullable byte[] stop, @Nullable byte[][] columns,
               @Nullable FuzzyRowFilter filter) throws Exception;

}

