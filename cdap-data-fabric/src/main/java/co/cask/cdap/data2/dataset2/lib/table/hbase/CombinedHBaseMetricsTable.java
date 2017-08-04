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

import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import javax.annotation.Nullable;

/**
 * Combined Metrics table to read from both v2 and v3 Tables
 */
public class CombinedHBaseMetricsTable implements MetricsTable {
  private final MetricsTable v2HBaseTable;
  private final MetricsTable v3HBaseTable;

  public CombinedHBaseMetricsTable(MetricsTable v2HBaseTable, MetricsTable v3HBaseTable) {
    this.v2HBaseTable = v2HBaseTable;
    this.v3HBaseTable = v3HBaseTable;
  }

  @Nullable
  @Override
  public byte[] get(byte[] row, byte[] column) {
    return v3HBaseTable.get(row, column);
  }

  @Override
  public void put(SortedMap<byte[], ? extends SortedMap<byte[], Long>> updates) {
    deleteColumns(updates);
    v3HBaseTable.put(updates);
  }

  private void deleteColumns(SortedMap<byte[], ? extends SortedMap<byte[], Long>> updates) {
    for (Map.Entry<byte[], ? extends SortedMap<byte[], Long>> entry : updates.entrySet()) {
      byte[] row = entry.getKey();
      List<byte[]> columns = Lists.newArrayList();
      for (byte[] column : entry.getValue().keySet()) {
        columns.add(column);
      }
      // only delete from old table
      v2HBaseTable.delete(row, columns.toArray(new byte[columns.size()][]));
    }
  }

  @Override
  public void putBytes(SortedMap<byte[], ? extends SortedMap<byte[], byte[]>> updates) {
    v3HBaseTable.putBytes(updates);
  }

  @Override
  public boolean swap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) {
    return v3HBaseTable.swap(row, column, oldValue, newValue);
  }

  @Override
  public void increment(byte[] row, Map<byte[], Long> increments) {
    v3HBaseTable.increment(row, increments);
  }

  @Override
  public void increment(NavigableMap<byte[], NavigableMap<byte[], Long>> updates) {
    v3HBaseTable.increment(updates);
  }

  @Override
  public long incrementAndGet(byte[] row, byte[] column, long delta) {
    // This method will not be called from FactTable
    return v3HBaseTable.incrementAndGet(row, column, delta);
  }

  @Override
  public void delete(byte[] row, byte[][] columns) {
    v3HBaseTable.delete(row, columns);
    v2HBaseTable.delete(row, columns);
  }

  @Override
  public Scanner scan(@Nullable byte[] start, @Nullable byte[] stop, @Nullable FuzzyRowFilter filter) {
    return new CombinedMetricsScanner(v2HBaseTable.scan(start, stop, filter),
                                      v3HBaseTable.scan(start, stop, filter));
  }

  @Override
  public void close() throws IOException {
    v3HBaseTable.close();
    v2HBaseTable.close();
  }
}
