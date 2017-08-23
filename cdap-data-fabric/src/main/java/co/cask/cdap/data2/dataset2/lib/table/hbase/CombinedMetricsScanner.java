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
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.table.Result;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.id.DatasetId;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Implements Scanner to merge data from v2 and v3 metrics tables.
 */
public class CombinedMetricsScanner implements Scanner {
  private static final Logger LOG = LoggerFactory.getLogger(CombinedMetricsScanner.class);
  private Scanner v2TableScanner;
  private final Scanner v3TableScanner;
  private final DatasetId v2MetricsTableDatasetId;
  private final DatasetFramework datasetFramework;

  private Row v2TableNextRow;
  private Row v3TableNextRow;

  @VisibleForTesting
  public CombinedMetricsScanner(@Nullable Scanner v2TableScanner, Scanner v3TableScanner,
                                DatasetId v2MetricsTableDatasetId, DatasetFramework datasetFramework) {
    this.v2TableScanner = v2TableScanner;
    this.v3TableScanner = v3TableScanner;
    this.v2TableNextRow = getV2TableScannerNextRow();
    this.v3TableNextRow = v3TableScanner.next();
    this.v2MetricsTableDatasetId = v2MetricsTableDatasetId;
    this.datasetFramework = datasetFramework;
  }

  @Nullable
  @Override
  public Row next() {
    // both the scanners are exhausted, so return null
    if (v2TableNextRow == null && v3TableNextRow == null) {
      return null;
    }

    // v2 scanner is exhausted, so return row from v3 table
    if (v2TableNextRow == null) {
      return advanceV3Scanner();
    }

    // v3 scanner is exhausted, so return row from v2 table
    if (v3TableNextRow == null) {
      return advanceV2Scanner();
    }

    // if both the scanners have rows, return the minimum row.
    int rowComparison = Bytes.compareTo(v2TableNextRow.getRow(), v3TableNextRow.getRow());

    if (rowComparison > 0) {
      return advanceV3Scanner();
    } else if (rowComparison < 0) {
      return advanceV2Scanner();
    }

    // if the rows are same from v2 and v3 tables, we will merge the columns from both the tables and return the
    // results. For example:

    // v2 table has row1: t1 t2 t3 t4 t5   and v3 table has row1: t4 t5 t6 t7 t8
    //                    v1 v2 v3 v4 v5                          v6 v7 v8 v9 v10

    // then we will return row1: t1   t2   t3   t4     t5    t6   t7   t8
    //                           v1   v2   v3  v4+v6  v5+v7  v8   v9   v10

    byte[] rowKey = v3TableNextRow.getRow();
    Map<byte[], byte[]> mergedColumns = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    mergedColumns.putAll(v2TableNextRow.getColumns());

    for (Map.Entry<byte[], byte[]> v3Column : v3TableNextRow.getColumns().entrySet()) {
      byte[] columnValue = v3Column.getValue();

      // If columns are overlapping, add the column values
      if (mergedColumns.containsKey(v3Column.getKey())) {
        long mergedValue = Bytes.toLong(v3Column.getValue()) + Bytes.toLong(mergedColumns.get(v3Column.getKey()));
        columnValue = Bytes.toBytes(mergedValue);
      }

      mergedColumns.put(v3Column.getKey(), columnValue);
    }

    advanceV2Scanner();
    advanceV3Scanner();

    return new Result(rowKey, mergedColumns);
  }

  @Override
  public void close() {
    v3TableScanner.close();
    if (v2TableScanner != null) {
      v2TableScanner.close();
    }
  }

  private Row advanceV2Scanner() {
    Row resultRow = v2TableNextRow;
    v2TableNextRow = getV2TableScannerNextRow();
    return resultRow;
  }

  /**
   * get next row from v2 table; if v2 table scanner is null; this returns null
   * however if v2 table scanner is non-null, but return by calling scanner next
   * if scanner next throws exception and if the cause is table doesn't exist, we close the scanner,
   * set scanner to null and return null
   * @return
   */
  @Nullable
  private Row getV2TableScannerNextRow() {
    if (v2TableScanner == null) {
      return null;
    }
    try {
      return v2TableScanner.next();
    } catch (Exception e) {
      if (e instanceof IOException) {
        try {
          if (!datasetFramework.hasInstance(v2MetricsTableDatasetId)) {
            v2TableScanner.close();
            v2TableScanner = null;
            return null;
          }
        } catch (DatasetManagementException dme) {
          // TODO : how to handle this exception ? should we throw ?
          LOG.error("Exception while checking dataset exists", dme);
        }
      }
    }
    return null;
  }

  private Row advanceV3Scanner() {
    Row resultRow = v3TableNextRow;
    v3TableNextRow = v3TableScanner.next();
    return resultRow;
  }
}
