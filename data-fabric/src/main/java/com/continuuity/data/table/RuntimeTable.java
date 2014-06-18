package com.continuuity.data.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.DataSetException;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Get;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Put;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.data.dataset.table.Scanner;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.DataFabric;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TxConstants;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Base class for runtime implementations of Table.
 */
public class RuntimeTable extends Table {
  public static final Logger LOG = LoggerFactory.getLogger(Table.class);

  private final DataFabric dataFabric;

  // the name to use for metrics collection, typically the name of the enclosing dataset
  private final String metricName;
  private OrderedColumnarTable ocTable;


  /**
   * Creates instance of Runtime Table. This constructor is called by DataSetInstantiator at runtime only,
   * hence the table name doesn't matter, as it'll get initialized
   * in the {@link #initialize(com.continuuity.api.data.DataSetSpecification, DataSetContext)} method.
   */
  public RuntimeTable(DataFabric dataFabric, String metricName) {
    super(null);
    this.dataFabric = dataFabric;
    this.metricName = metricName;
  }

  @Override
  public void initialize(DataSetSpecification spec, DataSetContext context) {
    super.initialize(spec, context);
    ocTable = getOcTable(dataFabric);
  }

  // todo: hack
  public TransactionAware getTxAware() {
    return ocTable instanceof TransactionAware ? ((TransactionAware) ocTable) : null;
  }

  /**
   * @return the name to use for metrics
   */
  protected String getMetricName() {
    return metricName;
  }

  protected OrderedColumnarTable getOcTable(DataFabric dataFabric) {
    DataSetManager dataSetManager = dataFabric.getDataSetManager(OrderedColumnarTable.class);

    try {
      // We want to ensure table is there before creating table client
      // todo: races? add createIfNotExists() or simply open()?
      // todo: better exception handling?
      // TODO: Will be moving out into DataSet management system.
      if (!dataSetManager.exists(getName())) {
        Properties props = new Properties();
        props.put(TxConstants.PROPERTY_TTL, String.valueOf(getTTL()));
        dataSetManager.create(getName(), props);
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    Properties props = new Properties();
    props.put("conflict.level", getConflictLevel().name());
    props.put(TxConstants.PROPERTY_TTL, String.valueOf(getTTL()));
    return dataFabric.getDataSetClient(getName(), OrderedColumnarTable.class, props);
  }

  // Basic data operations

  @Override
  public Row get(byte[] row, byte[][] columns) {
    try {
      return new Result(row, ocTable.get(row, columns));
    } catch (Exception e) {
      LOG.debug("get failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("get failed", e);
    }
  }

  @Override
  public Row get(byte[] row) {
    try {
      return new Result(row, ocTable.get(row));
    } catch (Exception e) {
      LOG.debug("get failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("get failed", e);
    }
  }

  @Override
  public byte[] get(byte[] row, byte[] column) {
    try {
      return ocTable.get(row, column);
    } catch (Exception e) {
      LOG.debug("get failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("get failed", e);
    }
  }

  @Override
  public Row get(byte[] row, byte[] startColumn, byte[] stopColumn, int limit) {
    try {
      return new Result(row, ocTable.get(row, startColumn, stopColumn, limit));
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
      ocTable.put(row, columns, values);
    } catch (Exception e) {
      LOG.debug("put failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("put failed", e);
    }
  }

  @Override
  public void put(byte[] row, byte[] column, byte[] value) {
    try {
      ocTable.put(row, column, value);
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
      ocTable.delete(row);
    } catch (Exception e) {
      LOG.debug("delete failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("delete failed", e);
    }
  }

  @Override
  public void delete(byte[] row, byte[] column) {
    try {
      ocTable.delete(row, column);
    } catch (Exception e) {
      LOG.debug("delete failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("delete failed", e);
    }
  }

  @Override
  public void delete(byte[] row, byte[][] columns) {
    try {
      ocTable.delete(row, columns);
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
      return ocTable.increment(row, column, amount);
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
      incResult = ocTable.increment(row, columns, amounts);
    } catch (NumberFormatException e) {
      LOG.debug("increment failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw e;
    } catch (Exception e) {
      LOG.debug("increment failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("increment failed", e);
    }
    // todo: try to avoid copying maps
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
      return ocTable.compareAndSwap(row, column, expectedValue, newValue);
    } catch (Exception e) {
      LOG.debug("compareAndSwap failed for table: " + getName() + ", row: " + Bytes.toStringBinary(row), e);
      throw new DataSetException("compareAndSwap failed", e);
    }
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow) {
    try {
      return new ScannerAdapter(ocTable.scan(startRow, stopRow));
    } catch (Exception e) {
      LOG.debug("scan failed for table: " + getName(), e);
      throw new DataSetException("scan failed", e);
    }
  }

  @Override
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    try {
      return ocTable.getSplits(numSplits, start, stop);
    } catch (Exception e) {
      LOG.debug("getSplits failed for table: " + getName(), e);
      throw new DataSetException("getSplits failed", e);
    }
  }

  @Override
  public SplitReader<byte[], Row> createSplitReader(Split split) {
    return new TableScanner();
  }

  @Override
  public void write(byte[] key, Put put) {
    Preconditions.checkArgument(Bytes.equals(key, put.getRow()), "The key should be the same as row in Put");
    put(put);
  }

  /**
   * Table splits are simply a start and stop key.
   */
  public static class TableSplit extends Split {
    private final byte[] start, stop;

    public TableSplit(byte[] start, byte[] stop) {
      this.start = start;
      this.stop = stop;
    }

    byte[] getStart() {
      return start;
    }

    byte[] getStop() {
      return stop;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
                    .add("start", Bytes.toString(start))
                    .add("stop", Bytes.toString(stop))
                    .toString();
    }
  }

  /**
   * Implements a split reader for a key range of a table, based on the Scanner implementation of the underlying
   * table implementation.
   */
  public class TableScanner extends SplitReader<byte[], Row> {

    // the underlying scanner
    private com.continuuity.data.table.Scanner scanner;
    // the current key
    private byte[] key = null;
    // the current row, that is, a map from column key to value
    private Map<byte[], byte[]> row = null;

    @Override
    public void initialize(Split split) throws InterruptedException {
      TableSplit tableSplit = (TableSplit) split;
      try {
        this.scanner = ocTable.scan(tableSplit.getStart(), tableSplit.getStop());
      } catch (Exception e) {
        LOG.debug("scan failed for table: " + getName(), e);
        throw new DataSetException("scan failed", e);
      }
    }

    @Override
    public boolean nextKeyValue() throws InterruptedException {
      // call the underlying scanner, and depending on whether there it returns something, set current key and row.
      ImmutablePair<byte[], Map<byte[], byte[]>> next = this.scanner.next();
      if (next == null) {
        this.key = null;
        this.row = null;
        return false;
      } else {
        this.key = next.getFirst();
        this.row = next.getSecond();
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

  // NOTE: we want this because we don't want to expose internal Scanner. This will change with Table API refactoring
  private static class ScannerAdapter implements Scanner {
    private final com.continuuity.data.table.Scanner delegate;

    private ScannerAdapter(com.continuuity.data.table.Scanner delegate) {
      this.delegate = delegate;
    }

    @Override
    public Row next() {
      ImmutablePair<byte[], Map<byte[], byte[]>> next = delegate.next();
      return next == null ? null : new Result(next.getFirst(), next.getSecond());
    }

    @Override
    public void close() {
      delegate.close();
    }
  }
}
