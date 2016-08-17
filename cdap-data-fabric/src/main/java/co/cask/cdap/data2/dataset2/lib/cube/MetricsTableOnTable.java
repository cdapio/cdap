/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.cube;

import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import javax.annotation.Nullable;

/**
 * Implementation of {@link MetricsTable} based on {@link Table}.
 */
class MetricsTableOnTable implements MetricsTable {
  private final Table table;

  MetricsTableOnTable(Table table) {
    this.table = table;
  }

  @Nullable
  @Override
  public byte[] get(byte[] row, byte[] column) {
    return table.get(row, column);
  }

  @Override
  public void put(SortedMap<byte[], ? extends SortedMap<byte[], Long>> updates) {
    for (Map.Entry<byte[], ? extends SortedMap<byte[], Long>> rowUpdate : updates.entrySet()) {
      Put put = new Put(rowUpdate.getKey());
      for (Map.Entry<byte[], Long> columnUpdate : rowUpdate.getValue().entrySet()) {
        put.add(columnUpdate.getKey(), columnUpdate.getValue());
      }
      table.put(put);
    }
  }

  @Override
  public boolean swap(byte[] row, byte[] column, byte[] oldValue, byte[] newValue) {
    return table.compareAndSwap(row, column, oldValue, newValue);
  }

  @Override
  public void increment(byte[] row, Map<byte[], Long> increments) {
    Increment increment = new Increment(row);
    for (Map.Entry<byte[], Long> columnUpdate : increments.entrySet()) {
      increment.add(columnUpdate.getKey(), columnUpdate.getValue());
    }
    table.increment(increment);
  }

  @Override
  public void increment(NavigableMap<byte[], NavigableMap<byte[], Long>> updates) {
    for (Map.Entry<byte[], NavigableMap<byte[], Long>> rowUpdate : updates.entrySet()) {
      Increment increment = new Increment(rowUpdate.getKey());
      for (Map.Entry<byte[], Long> columnUpdate : rowUpdate.getValue().entrySet()) {
        increment.add(columnUpdate.getKey(), columnUpdate.getValue());
      }
      table.increment(increment);
    }
  }

  @Override
  public long incrementAndGet(byte[] row, byte[] column, long delta) {
    return table.incrementAndGet(row, column, delta);
  }

  @Override
  public void delete(byte[] row, byte[][] columns) {
    table.delete(row, columns);
  }

  @Override
  public Scanner scan(@Nullable byte[] start, @Nullable byte[] stop,
                      @Nullable FuzzyRowFilter filter) {
    return table.scan(new Scan(start, stop, filter));
  }

  @Override
  public void close() throws IOException {
    table.close();
  }
}
