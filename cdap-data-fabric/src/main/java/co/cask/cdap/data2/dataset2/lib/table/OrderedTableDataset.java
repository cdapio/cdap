/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Result;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableSplit;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * A dataset exposing the {@link OrderedTable} interface.
 *
 * @deprecated since 2.8.0, use {@link Table} datasets instead.
 */
@Deprecated
class OrderedTableDataset extends AbstractDataset implements OrderedTable {
  private static final Logger LOG = LoggerFactory.getLogger(OrderedTableDataset.class);

  private final Table table;

  OrderedTableDataset(String instanceName, Table table) {
    super(instanceName, table);
    this.table = table;
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, byte[][] columns) {
    return table.get(row, columns).getColumns();
  }

  @Override
  public byte[] get(byte[] row, byte[] column) {
    return table.get(row, column);
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row) {
    return table.get(row).getColumns();
  }

  @Override
  public Map<byte[], byte[]> get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit) {
    return table.get(row, startColumn, stopColumn, limit).getColumns();
  }

  @Override
  public void put(byte[] row, byte[][] columns, byte[][] values) {
    table.put(row, columns, values);
  }

  @Override
  public void put(byte[] row, byte[] column, byte[] value) {
    table.put(row, column, value);
  }

  @Override
  public void delete(byte[] row) {
    table.delete(row);
  }

  @Override
  public void delete(byte[] row, byte[] column) {
    table.delete(row, column);
  }

  @Override
  public void delete(byte[] row, byte[][] columns) {
    table.delete(row, columns);
  }

  @Override
  public long incrementAndGet(byte[] row, byte[] column, long amount) {
    return table.incrementAndGet(row, column, amount);
  }

  @Override
  public Map<byte[], Long> incrementAndGet(byte[] row, byte[][] columns, long[] amounts) {
    Map<byte[], byte[]> incResult = table.incrementAndGet(row, columns, amounts).getColumns();
    return Maps.transformValues(incResult, new Function<byte[], Long>() {
      @Nullable
      @Override
      public Long apply(@Nullable byte[] bytes) {
        return Bytes.toLong(bytes);
      }
    });
  }

  @Override
  public void increment(byte[] row, byte[] column, long amount) {
    table.increment(row, column, amount);
  }

  @Override
  public void increment(byte[] row, byte[][] columns, long[] amounts) {
    table.increment(row, columns, amounts);
  }

  @Override
  public boolean compareAndSwap(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue) throws Exception {
    return table.compareAndSwap(row, column, expectedValue, newValue);
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow) {
    return table.scan(startRow, stopRow);
  }

  /**
   * Returns splits for a range of keys in the table.
   *
   * @param numSplits Desired number of splits. If greater than zero, at most this many splits will be returned.
   *                  If less or equal to zero, any number of splits can be returned.
   * @param start If non-null, the returned splits will only cover keys that are greater or equal.
   * @param stop If non-null, the returned splits will only cover keys that are less.
   * @return list of {@link Split}
   */
  @Beta
  @Override
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    return table.getSplits(numSplits, start, stop);
  }
}
