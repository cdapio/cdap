/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Result;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import com.google.common.annotations.VisibleForTesting;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Implements Scanner on merge data from v2 and v3 metrics table.
 */
public class CombinedMetricsScanner implements Scanner {

  private final Scanner v2TableScanner;
  private final Scanner v3TableScanner;
  private Row v2TableNextRow;
  private Row v3TableNextRow;

  @VisibleForTesting
  public CombinedMetricsScanner(Scanner v2TableScanner, Scanner v3TableScanner) {
    this.v3TableScanner = v3TableScanner;
    this.v2TableScanner = v2TableScanner;
    this.v3TableNextRow = v3TableScanner.next();
    this.v2TableNextRow = v2TableScanner.next();
  }

  @Nullable
  @Override
  public Row next() {
    Row resultRow;
    // both the scanners are exhausted, so return null
    if (v2TableNextRow == null && v3TableNextRow == null) {
      return null;
    }

    // v2 scanner is exhausted, so return row from v3 table
    if (v2TableNextRow == null) {
      return getV3Row();
    }

    // v3 scanner is exhausted, so return row from v2 table
    if (v3TableNextRow == null) {
      return getV2Row();
    }

    // if both the scanners have rows, return the minimum row.
    int rowComparison = Bytes.compareTo(v2TableNextRow.getRow(), v3TableNextRow.getRow());

    if (rowComparison > 0) {
      resultRow = getV3Row();
    } else if (rowComparison < 0) {
      resultRow = getV2Row();
    } else {
      // if the rows are same from v2 and v3 tables, we will merge the columns from both the tables and return the
      // results. For example,

      // v2 table has row1: t1 t2 t3 t4 t5   and v3 table has row1: t4 t5 t6 t7 t8
      //                    v1 v2 v3 v4 v5                          v6 v7 v8 v9 v10

      // then we will return row1: t1   t2   t3   t4     t5    t6   t7   t8
      //                           v1   v2   v3  v4+v6  v5+v7  v8   v9   v10

      Map<byte[], byte[]> mergedColumns = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      byte[] rowKey = v3TableNextRow.getRow();

      mergeColumns(v2TableNextRow, v3TableNextRow, mergedColumns);

      v2TableNextRow = v2TableScanner.next();
      v3TableNextRow = v3TableScanner.next();
      resultRow = new Result(rowKey, mergedColumns);
    }

    return resultRow;
  }

  @Override
  public void close() {
    v3TableScanner.close();
    v2TableScanner.close();
  }

  private void mergeColumns(Row v2TableNextRow, Row v3TableNextRow, Map<byte[], byte[]> mergedColumns) {
    Iterator<Map.Entry<byte[], byte[]>> v2ColumnIterator = v2TableNextRow.getColumns().entrySet().iterator();
    Iterator<Map.Entry<byte[], byte[]>> v3ColumnIterator = v3TableNextRow.getColumns().entrySet().iterator();

    // if v2 row does not have any columns then get all the columns of v3 row
    if (!v2ColumnIterator.hasNext()) {
      mergeRemainingColumns(v3ColumnIterator, mergedColumns);
      return;
    }

    // if v3 row does not have any columns then get all the columns of v2 row
    if (!v3ColumnIterator.hasNext()) {
      mergeRemainingColumns(v2ColumnIterator, mergedColumns);
      return;
    }

    Map.Entry<byte[], byte[]> v2Column = v2ColumnIterator.next();
    Map.Entry<byte[], byte[]> v3Column = v3ColumnIterator.next();

    do {
      long v2ColumnTs = v2TableNextRow.getLong(v2Column.getKey());
      long v3ColumnTs = v3TableNextRow.getLong(v3Column.getKey());

      try {
        if (v2ColumnTs < v3ColumnTs) {
          mergedColumns.put(v2Column.getKey(), v2Column.getValue());
          v2Column = v2ColumnIterator.next();
        } else if (v2ColumnTs > v3ColumnTs) {
          mergedColumns.put(v3Column.getKey(), v3Column.getValue());
          v3Column = v3ColumnIterator.next();
        } else {
          // column qualifiers are equal so add the values
          long mergedValue = Bytes.toLong(v3Column.getValue()) + Bytes.toLong(v2Column.getValue());
          mergedColumns.put(v3Column.getKey(), Bytes.toBytes(mergedValue));
          v2Column = v2ColumnIterator.next();
          v3Column = v3ColumnIterator.next();
        }
      } catch (NoSuchElementException e) {
        // one of the iterator is exhausted
        v2Column = null;
        v3Column = null;
      }
    } while (v2Column != null && v3Column != null);

    // Merge all the remaining elements from v2 iterator
    if (v2ColumnIterator.hasNext()) {
      mergeRemainingColumns(v2ColumnIterator, mergedColumns);
    }

    // Merge all the remaining elements from v3 iterator
    if (v3ColumnIterator.hasNext()) {
      mergeRemainingColumns(v3ColumnIterator, mergedColumns);
    }
  }

  private void mergeRemainingColumns(Iterator<Map.Entry<byte[], byte[]>> columnIterator,
                                     Map<byte[], byte[]> mergedColumns) {
    while (columnIterator.hasNext()) {
      Map.Entry<byte[], byte[]> column = columnIterator.next();
      mergedColumns.put(column.getKey(), column.getValue());
    }
  }

  private Row getV2Row() {
    Row resultRow = v2TableNextRow;
    v2TableNextRow = v2TableScanner.next();
    return resultRow;
  }

  private Row getV3Row() {
    Row resultRow = v3TableNextRow;
    v3TableNextRow = v3TableScanner.next();
    return resultRow;
  }
}
