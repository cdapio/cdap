/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Result;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableSplit;
import co.cask.tephra.TransactionAware;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Implements some of the methods in a generic way (not necessarily in most efficient way).
 */
public abstract class AbstractTable implements Table, TransactionAware {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTable.class);

  // empty immutable row's column->value map constant
  // Using ImmutableSortedMap instead of Maps.unmodifiableNavigableMap to avoid conflicts with
  // Hadoop, which uses an older version of guava without that method.
  protected static final NavigableMap<byte[], byte[]> EMPTY_ROW_MAP =
    ImmutableSortedMap.<byte[], byte[]>orderedBy(Bytes.BYTES_COMPARATOR).build();

  @Override
  public byte[] get(byte[] row, byte[] column) {
    Row result = get(row, new byte[][]{column});
    return result.isEmpty() ? null : result.get(column);
  }

  @Override
  public Row get(Get get) {
    return get.getColumns().isEmpty() ?
        get(get.getRow()) :
        get(get.getRow(), get.getColumns().toArray(new byte[get.getColumns().size()][]));
  }

  @Override
  public List<Row> get(List<Get> gets) {
    List<Row> results = Lists.newArrayListWithCapacity(gets.size());
    for (Get get : gets) {
      results.add(get(get));
    }
    return results;
  }

  @Override
  public void put(byte [] row, byte [] column, byte[] value) {
    put(row, new byte[][]{column}, new byte[][]{value});
  }

  @Override
  public void put(Put put) {
    Preconditions.checkArgument(!put.getValues().isEmpty(), "Put must have at least one value");
    byte[][] columns = new byte[put.getValues().size()][];
    byte[][] values = new byte[put.getValues().size()][];
    int i = 0;
    for (Map.Entry<byte[], byte[]> columnValue : put.getValues().entrySet()) {
      columns[i] = columnValue.getKey();
      values[i] = columnValue.getValue();
      i++;
    }
    put(put.getRow(), columns, values);
  }

  @Override
  public long incrementAndGet(byte[] row, byte[] column, long amount) {
    byte[] result = incrementAndGet(row, new byte[][]{column}, new long[]{amount}).get(column);
    return Bytes.toLong(result);
  }

  @Override
  public Row incrementAndGet(Increment increment) {
    Preconditions.checkArgument(!increment.getValues().isEmpty(), "Increment must have at least one value");
    byte[][] columns = new byte[increment.getValues().size()][];
    long[] values = new long[increment.getValues().size()];
    int i = 0;
    for (Map.Entry<byte[], Long> columnValue : increment.getValues().entrySet()) {
      columns[i] = columnValue.getKey();
      values[i] = columnValue.getValue();
      i++;
    }
    return incrementAndGet(increment.getRow(), columns, values);
  }

  @Override
  public void increment(byte[] row, byte[] column, long amount) {
    increment(row, new byte[][]{column}, new long[]{amount});
  }

  @Override
  public void increment(Increment increment) {
    Preconditions.checkArgument(!increment.getValues().isEmpty(), "Increment must have at least one value");
    byte[][] columns = new byte[increment.getValues().size()][];
    long[] values = new long[increment.getValues().size()];
    int i = 0;
    for (Map.Entry<byte[], Long> columnValue : increment.getValues().entrySet()) {
      columns[i] = columnValue.getKey();
      values[i] = columnValue.getValue();
      i++;
    }
    increment(increment.getRow(), columns, values);
  }

  @Override
  public void delete(byte[] row, byte[] column) {
    delete(row, new byte[][]{column});
  }

  @Override
  public void delete(Delete delete) {
    if (delete.getColumns().isEmpty()) {
      delete(delete.getRow());
    } else {
      delete(delete.getRow(), delete.getColumns().toArray(new byte[delete.getColumns().size()][]));
    }
  }

  // from TableDataset

  @Override
  public void write(byte[] key, Put put) {
    put(put);
  }

  @Override
  public List<Split> getSplits() {
    return getSplits(-1, null, null);
  }

  @Override
  public SplitReader<byte[], Row> createSplitReader(Split split) {
    return new TableScanner();
  }

  /**
   * Implements a split reader for a key range of a table, based on the Scanner implementation of the underlying
   * table implementation.
   */
  public class TableScanner extends SplitReader<byte[], Row> {

    // the underlying scanner
    private Scanner scanner;
    // the current key
    private byte[] key = null;
    // the current row, that is, a map from column key to value
    private Map<byte[], byte[]> row = null;

    @Override
    public void initialize(Split split) throws InterruptedException {
      TableSplit tableSplit = (TableSplit) split;
      try {
        this.scanner = scan(tableSplit.getStart(), tableSplit.getStop());
      } catch (Exception e) {
        LOG.debug("scan failed for table: " + getTransactionAwareName(), e);
        throw new DataSetException("scan failed", e);
      }
    }

    @Override
    public boolean nextKeyValue() throws InterruptedException {
      // call the underlying scanner, and depending on whether there it returns something, set current key and row.
      Row next = this.scanner.next();
      if (next == null) {
        this.key = null;
        this.row = null;
        return false;
      } else {
        this.key = next.getRow();
        this.row = next.getColumns();
        return true;
      }
    }

    @Override
    public byte[] getCurrentKey() throws InterruptedException {
      return this.key;
    }

    @Override
    public Row getCurrentValue() throws InterruptedException {
      return new Result(this.key, this.row);
    }

    @Override
    public void close() {
      this.scanner.close();
    }
  }

}
