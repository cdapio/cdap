package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.DataSetException;
import com.continuuity.api.dataset.lib.AbstractDataset;
import com.continuuity.api.dataset.table.Delete;
import com.continuuity.api.dataset.table.Get;
import com.continuuity.api.dataset.table.Increment;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.api.dataset.table.Put;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Scanner;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.api.dataset.table.TableSplit;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
class TableDataset extends AbstractDataset implements Table {
  public static final Logger LOG = LoggerFactory.getLogger(TableDataset.class);

  private final OrderedTable table;

  TableDataset(String instanceName, OrderedTable table) {
    super(instanceName, table);
    this.table = table;
  }

  @Override
  public Row get(byte[] row, byte[][] columns) {
    try {
      return new Result(row, table.get(row, columns));
    } catch (Exception e) {
      LOG.debug("get failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("get failed", e);
    }
  }

  @Override
  public Row get(byte[] row) {
    try {
      return new Result(row, table.get(row));
    } catch (Exception e) {
      LOG.debug("get failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("get failed", e);
    }
  }

  @Override
  public byte[] get(byte[] row, byte[] column) {
    try {
      return table.get(row, column);
    } catch (Exception e) {
      LOG.debug("get failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("get failed", e);
    }
  }

  @Override
  public Row get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit) {
    try {
      return new Result(row, table.get(row, startColumn, stopColumn, limit));
    } catch (Exception e) {
      LOG.debug("get failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("get failed", e);
    }
  }


  @Override
  public Row get(Get get) {
    return get.getColumns().isEmpty() ?
      get(get.getRow()) :
      get(get.getRow(), get.getColumns().toArray(new byte[get.getColumns().size()][]));
  }

  @Override
  public void put(byte[] row, byte[][] columns, byte[][] values) {
    try {
      table.put(row, columns, values);
    } catch (Exception e) {
      LOG.debug("put failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("put failed", e);
    }
  }

  @Override
  public void put(byte[] row, byte[] column, byte[] value) {
    try {
      table.put(row, column, value);
    } catch (Exception e) {
      LOG.debug("put failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("put failed", e);
    }
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
  public void delete(byte[] row) {
    try {
      table.delete(row);
    } catch (Exception e) {
      LOG.debug("delete failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("delete failed", e);
    }
  }

  @Override
  public void delete(byte[] row, byte[] column) {
    try {
      table.delete(row, column);
    } catch (Exception e) {
      LOG.debug("delete failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("delete failed", e);
    }
  }

  @Override
  public void delete(byte[] row, byte[][] columns) {
    try {
      table.delete(row, columns);
    } catch (Exception e) {
      LOG.debug("delete failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("delete failed", e);
    }
  }

  @Override
  public void delete(Delete delete) {
    if (delete.getColumns().isEmpty()) {
      delete(delete.getRow());
    } else {
      delete(delete.getRow(), delete.getColumns().toArray(new byte[delete.getColumns().size()][]));
    }
  }

  @Override
  public long increment(byte[] row, byte[] column, long amount) {
    try {
      return table.increment(row, column, amount);
    } catch (NumberFormatException e) {
      LOG.debug("increment failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw e;
    } catch (Exception e) {
      LOG.debug("increment failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("increment failed", e);
    }
  }

  @Override
  public Row increment(byte[] row, byte[][] columns, long[] amounts) {
    Map<byte[], Long> incResult;
    try {
      incResult = table.increment(row, columns, amounts);
    } catch (NumberFormatException e) {
      LOG.debug("increment failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw e;
    } catch (Exception e) {
      LOG.debug("increment failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("increment failed", e);
    }
    // todo: define IncrementResult to make it more efficient
    return new Result(row, Maps.transformValues(incResult, new Function<Long, byte[]>() {
      @Nullable
      @Override
      public byte[] apply(@Nullable Long input) {
        return input == null ? null : Bytes.toBytes(input);
      }
    }));
  }

  @Override
  public Row increment(Increment increment) {
    Preconditions.checkArgument(!increment.getValues().isEmpty(), "Increment must have at least one value");
    byte[][] columns = new byte[increment.getValues().size()][];
    long[] values = new long[increment.getValues().size()];
    int i = 0;
    for (Map.Entry<byte[], Long> columnValue : increment.getValues().entrySet()) {
      columns[i] = columnValue.getKey();
      values[i] = columnValue.getValue();
      i++;
    }
    return increment(increment.getRow(), columns, values);
  }

  @Override
  public boolean compareAndSwap(byte[] row, byte[] column, byte[] expectedValue, byte[] newValue) {
    try {
      return table.compareAndSwap(row, column, expectedValue, newValue);
    } catch (Exception e) {
      String msg = "compareAndSwap failed for table: " + getName();
      LOG.debug(msg, e);
      throw new DataSetException(msg, e);
    }
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow) {
    try {
      return table.scan(startRow, stopRow);
    } catch (Exception e) {
      LOG.debug("scan failed for table: " + getName(), e);
      throw new DataSetException("scan failed", e);
    }
  }

  @Override
  public void write(byte[] key, Put put) {
    put(put);
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
    try {
      return table.getSplits(numSplits, start, stop);
    } catch (Exception e) {
      LOG.error("getSplits failed for table: " + getName(), e);
      throw new DataSetException("getSplits failed", e);
    }
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
        this.scanner = table.scan(tableSplit.getStart(), tableSplit.getStop());
      } catch (Exception e) {
        LOG.debug("scan failed for table: " + getName(), e);
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
