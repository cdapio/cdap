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

package io.cdap.cdap.data2.dataset2.lib.table;

import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.table.Scanner;

import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import javax.annotation.Nullable;

/**
 * A table interface dedicated to our metrics system and is currently used by Cube dataset.
 */
public interface MetricsTable extends Dataset {

  /**
   * Read single column of a row
   */
  @Nullable
  byte[] get(byte[] row, byte[] column);

  /**
   * Write multiple rows, each with multiple individual columns to write.
   */
  void put(SortedMap<byte[], ? extends SortedMap<byte[], Long>> updates);

  /**
   * Write multiple rows, each with multiple individual columns of byte array type to write.
   * TODO: (CDAP-8216) This method is only for storing messageId for
   * {@link cdap-watchdog.io.cdap.cdap.metrics.process.MetricsConsumerMetaTable}. Once (CDAP-8216) is resolved, this
   * method can be removed.
   */
  void putBytes(SortedMap<byte[], ? extends SortedMap<byte[], byte[]>> updates);

  /**
   * Atomically compare a single column of a row with an expected value, and if it matches, replace it with a new value.
   * @param oldValue the expected value of the column. If null, this means that the column must not exist.
   * @param newValue the new value of the column. If null, the effect to delete the column if the comparison succeeds.
   * @return whether the write happened, that is, whether the existing value of the column matched the expected value.
   */
  boolean swap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue);

  /**
   * Atomic increment of multiple columns of a row. If a column does not exist prior to the increment,
   * its value is assumed zero. Increments are only guaranteed atomic with respect to other increments.
   * @param increments Map from each column key to the value it should be incremented by.
   */
  void increment(byte[] row, Map<byte[], Long> increments);


  /**
   * Batch increment multiple rows each with multiple columns and increments
   */
  void increment(NavigableMap<byte[], NavigableMap<byte[], Long>> updates);

  /**
   * Increment a single column of a row and return the new value.
   * @return the new value after the increment.
   */
  long incrementAndGet(byte[] row, byte[] column, long delta);

  /**
   * Deletes specified columns of the specified row.
   *
   * @param row row to delete from
   * @param columns names of columns to delete
   */
  void delete(byte[] row, byte[][] columns);

  /**
   * Deletes specified columns of the specified row.
   *
   * @param row row to delete from
   * @param columns names of columns to delete
   * @param fullRow if the whole row is deleted. Note that this parameter is just an optimization and list
   *                of columns must still be passed in case when this optimization is not implemented
   * @see #delete(byte[], byte[][]) 
   */
  default void delete(byte[] row, byte[][] columns, boolean fullRow) {
    delete(row, columns);
  }

  /**
   * Get a scanner for a table.
   * @param start the row key of the first row to scan. If null, the scan begins at the first row of the table.
   * @param stop the row key of the last row to scan. If null, the scan goes to the last row of the table.
   * @param filter if non-null, a fuzzy row filter used to efficiently skip over entire rows.
   */
  Scanner scan(@Nullable byte[] start, @Nullable byte[] stop, @Nullable FuzzyRowFilter filter);

}

