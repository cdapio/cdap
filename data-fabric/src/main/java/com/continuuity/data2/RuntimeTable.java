package com.continuuity.data2;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.data.dataset.table.Scanner;
import com.continuuity.api.data.dataset.table.Swap;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.api.data.dataset.table.WriteOperation;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.DataFabric;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.TransactionAware;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Base class for runtime implementations of Table.
 */
public class RuntimeTable extends Table {
  private final DataFabric dataFabric;

  // the name to use for metrics collection, typically the name of the enclosing dataset
  private final String metricName;
  private OrderedColumnarTable ocTable;


  /**
   * Creates instance of Runtime Table. This constructor is called by DataSetInstantiator at runtime only,
   * hence the table name doesn't matter, as it'll get initialized
   * in the {@link #initialize(com.continuuity.api.data.DataSetSpecification)} method.
   */
  public RuntimeTable(DataFabric dataFabric, String metricName) {
    super(null);
    this.dataFabric = dataFabric;
    this.metricName = metricName;
  }

  @Override
  public void initialize(DataSetSpecification spec) {
    super.initialize(spec);

    DataSetManager dataSetManager = dataFabric.getDataSetManager(OrderedColumnarTable.class);

    try {
      // We want to ensure table is there before creating table client
      // todo: races? add createIfNotExists() or simply open()?
      // todo: better exception handling?
      // TODO: Will be moving out into DataSet management system.
      if (!dataSetManager.exists(getName())) {
        dataSetManager.create(getName());
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    Properties props = new Properties();
    props.put("conflict.level", getConflictLevel().name());
    ocTable = dataFabric.getDataSetClient(getName(), OrderedColumnarTable.class, props);
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

  @Override
  public OperationResult<Map<byte[], byte[]>> read(Read read)
    throws OperationException {

    if (read.getColumns() != null) {
      // this is a multi-column read
      try {
        return ocTable.get(read.getRow(), read.getColumns());
      } catch (OperationException oe) {
        throw oe;
      } catch (Exception e) {
        // todo: add more details in error message
        throw new OperationException(StatusCode.INTERNAL_ERROR,
                                     "Read failed for table " + tableName(), e);
      }
    } else {
      // this is a column-range read
      try {
        return ocTable.get(read.getRow(), read.getStartCol(), read.getStopCol(), read.getLimit());
      } catch (OperationException oe) {
        throw oe;
      } catch (Exception e) {
        // todo: add more details in error message
        throw new OperationException(StatusCode.INTERNAL_ERROR,
                                     "Read failed for table " + tableName(), e);
      }
    }
  }

  @Override
  public void write(WriteOperation op) throws OperationException {
    try {
      if (op instanceof Write) {
        Write write = (Write) op;
        ocTable.put(write.getRow(), write.getColumns(), write.getValues());
      } else if (op instanceof Delete) {
        Delete delete = (Delete) op;
        ocTable.delete(delete.getRow(), delete.getColumns());
      } else if (op instanceof Increment) {
        Increment increment = (Increment) op;
        ocTable.increment(increment.getRow(), increment.getColumns(), increment.getValues());
      } else if (op instanceof Swap) {
        Swap swap = (Swap) op;
        if (!ocTable.compareAndSwap(swap.getRow(), swap.getColumn(), swap.getExpected(), swap.getValue())) {
          // throwing exception is not good, but we do it to support current code (we improve incrementally
          throw new OperationException(StatusCode.WRITE_CONFLICT, "compare and swap failed");
        }
      } else { // can't happen but...
        throw new IllegalArgumentException("Received an operation of unknown type " + op.getClass().getName());
      }
    } catch (OperationException oe) {
      throw oe;
    } catch (Exception e) {
      // todo: add more details in error message
      throw new OperationException(StatusCode.INTERNAL_ERROR,
                                   "WriteOperation failed for table " + tableName(), e);
    }
  }

  @Override
  public Map<byte[], Long> incrementAndGet(Increment increment) throws OperationException {
    try {
      return ocTable.increment(increment.getRow(), increment.getColumns(), increment.getValues());
    } catch (OperationException oe) {
      throw oe;
    } catch (Exception e) {
      // todo: add more details in error message
      throw new OperationException(StatusCode.INTERNAL_ERROR,
                                   "Increment failed for table " + tableName(), e);
    }
  }

  @Override
  public Scanner scan(byte[] startRow, byte[] stopRow) throws OperationException {
    try {
      return new ScannerAdapter(ocTable.scan(startRow, stopRow));
    } catch (OperationException oe) {
      throw oe;
    } catch (Exception e) {
      // todo: add more details in error message
      throw new OperationException(StatusCode.INTERNAL_ERROR,
                                   "scan failed for table " + tableName(), e);
    }
  }

  @Override
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) throws OperationException {
    try {
      return ocTable.getSplits(numSplits, start, stop);
    } catch (OperationException oe) {
      throw oe;
    } catch (Exception e) {
      // todo: add more details in error message
      throw new OperationException(StatusCode.INTERNAL_ERROR,
                                   "getSplits failed for table " + tableName(), e);
    }
  }

  @Override
  public SplitReader<byte[], Map<byte[], byte[]>> createSplitReader(Split split) {
    return new TableScanner();
  }

  @Override
  public void write(byte[] key, Map<byte[], byte[]> row) throws OperationException {
    if (key == null || row == null) {
      return;
    }
    final byte[][] columns = new byte[row.size()][];
    final byte[][] values = new byte[row.size()][];
    int i = 0;
    for (Map.Entry<byte[], byte[]> entry : row.entrySet()) {
      columns[i] = entry.getKey();
      values[i] = entry.getValue();
      i++;
    }
    this.write(new Write(key, columns, values));
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
  public class TableScanner extends SplitReader<byte[], Map<byte[], byte[]>> {

    // the underlying scanner
    private com.continuuity.data.table.Scanner scanner;
    // the current key
    private byte[] key = null;
    // the current row, that is, a map from column key to value
    private Map<byte[], byte[]> row = null;

    @Override
    public void initialize(Split split) throws InterruptedException, OperationException {
      TableSplit tableSplit = (TableSplit) split;
      try {
        this.scanner = ocTable.scan(tableSplit.getStart(), tableSplit.getStop());
      } catch (OperationException oe) {
        throw oe;
      } catch (Exception e) {
        throw new OperationException(StatusCode.INTERNAL_ERROR,
                                     "Could not initialize scanner for table " + tableName(), e);
      }
    }

    @Override
    public boolean nextKeyValue() throws InterruptedException, OperationException {
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
    public Map<byte[], byte[]> getCurrentValue() throws InterruptedException {
      return this.row;
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
      return next == null ? null : new TableRow(next);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  private static class TableRow implements Row {
    private final ImmutablePair<byte[], Map<byte[], byte[]>> row;

    private TableRow(ImmutablePair<byte[], Map<byte[], byte[]>> row) {
      this.row = row;
    }

    @Override
    public byte[] getRow() {
      return row.getFirst();
    }

    @Override
    public Map<byte[], byte[]> getColumns() {
      return row.getSecond();
    }
  }
}
