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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implements Scanner on merge data from v2 and v3 metrics table.
 */
public class CombinedMetricsScanner implements Scanner {

  private final Scanner v2TableScanner;
  private final Scanner v3TableScanner;
  private Row v2TableNextRow;
  private Row v3TableNextRow;

  public CombinedMetricsScanner(Scanner v2TableScanner, Scanner v3TableScanner) {
    this.v3TableScanner = v3TableScanner;
    this.v2TableScanner = v2TableScanner;
    this.v3TableNextRow = v3TableScanner.next();
    this.v2TableNextRow = v2TableScanner.next();
  }

  @Nullable
  @Override
  public Row next() {
    if (v2TableNextRow == null && v3TableScanner == null) {
      return null;
    }

    if (v2TableNextRow == null) {
      return v3TableNextRow;
    }

    if (v3TableNextRow == null) {
      return v2TableNextRow;
    }

    // Compare rows
    int rowComparison = Bytes.compareTo(v2TableNextRow.getRow(), v3TableNextRow.getRow());

    if (rowComparison > 0) {
      return v3TableNextRow;
    } else if (rowComparison > 0) {
      return v2TableNextRow;
    } else {
      // rows are equal, so merge the row columns and return the merged row
      Iterator<Map.Entry<byte[], byte[]>> v2ColumnIterator = v2TableNextRow.getColumns().entrySet().iterator();
      Iterator<Map.Entry<byte[], byte[]>> v3ColumnIterator = v3TableNextRow.getColumns().entrySet().iterator();
      Map<byte[], byte[]> mergedColumns = new HashMap<>();

      while (v2ColumnIterator.hasNext() && v3ColumnIterator.hasNext()) {

      }

      while (v3ColumnIterator.hasNext()) {
        Map.Entry<byte[], byte[]> column = v3ColumnIterator.next();
        mergedColumns.put(column.getKey(), column.getValue());
      }
      return new Result(v3TableNextRow.getRow(), mergedColumns);
    }

    // TODO: update the row to get next row from the scanner
  }

  @Override
  public void close() {
    v3TableScanner.close();
    v2TableScanner.close();
  }
}
