package com.continuuity.data.dataset;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Swap;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.api.data.dataset.table.WriteOperation;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.DataFabric;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.GetSplits;
import com.continuuity.data.operation.KeyRange;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.Scan;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.continuuity.data.table.Scanner;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base class for runtime implementations of Table.
 */
public abstract class RuntimeTable extends Table {

  /**
   * package-protected constructor, only to be called from @see #setReadOnlyTable()
   * and @see ReadWriteTable constructor.
   * @param table the original table
   * @param fabric the data fabric
   */
  RuntimeTable(Table table, DataFabric fabric, TransactionProxy proxy) {
    super(table.getName());
    this.dataFabric = fabric;
    this.proxy = proxy;
  }

  @Override
  public void setDelegate(Table delegate) {
    // this should never be called - it should only be called on the base class
    throw new UnsupportedOperationException("setDelegate() must not be called on the delegate itself.");
  }

  // the data fabric to use for executing synchronous operations.
  private final DataFabric dataFabric;

  // the transaction proxy for all operations.
  private final TransactionProxy proxy;

  // the name to use for metrics collection, typically the name of the enclosing dataset
  private String metricName;

  /**
   * @return the current transaction agent
   */
  protected TransactionAgent getTransactionAgent() {
    return this.proxy.getTransactionAgent();
  }

  /**
   * @return the name to use for metrics
   */
  protected String getMetricName() {
    return metricName;
  }

  /**
   * Set the name to use for metrics.
   * @param metricName the name to use for emitting metrics
   */
  protected void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  /**
   * Open the table in the data fabric, to ensure it exists and is accessible.
   * @throws OperationException if something goes wrong
   */
  public void open() throws OperationException {
    this.dataFabric.openTable(this.getName());
  }

  @Override
  public OperationResult<Map<byte[], byte[]>> read(Read read)
    throws OperationException {

    if (read.getColumns() != null) {
      // this is a multi-column read
      com.continuuity.data.operation.Read op =
        new com.continuuity.data.operation.Read(this.tableName(), read.getRow(), read.getColumns());
      op.setMetricName(getMetricName());
      return this.getTransactionAgent().execute(op);
    } else {
      // this is a column-range red
      ReadColumnRange op = new ReadColumnRange(
        this.tableName(), read.getRow(), read.getStartCol(), read.getStopCol(), read.getLimit());
      op.setMetricName(getMetricName());
      return this.getTransactionAgent().execute(op);
    }
  }

  /**
   * Turn a table operation into a data fabric operation.
   *
   * @param op a table write operation
   * @return the corresponding data fabric operation
   */
  private com.continuuity.data.operation.WriteOperation toOperation(WriteOperation op) {
    com.continuuity.data.operation.WriteOperation operation;
    if (op instanceof Write) {
      Write write = (Write) op;
      operation = new com.continuuity.data.operation.Write(
        this.tableName(), write.getRow(), write.getColumns(), write.getValues());
    } else if (op instanceof Delete) {
      Delete delete = (Delete) op;
      operation = new com.continuuity.data.operation.Delete(
        this.tableName(), delete.getRow(), delete.getColumns());
    } else if (op instanceof Increment) {
      operation = toOperation((Increment) op);
    } else if (op instanceof Swap) {
      Swap swap = (Swap) op;
      operation = new CompareAndSwap(
        this.tableName(), swap.getRow(), swap.getColumn(), swap.getExpected(), swap.getValue());
    } else { // can't happen but...
      throw new IllegalArgumentException("Received an operation of unknown type " + op.getClass().getName());
    }
    operation.setMetricName(getMetricName());
    return operation;
  }

  /**
   * Helper to convert an increment operation.
   * @param increment the table increment
   * @return a corresponding data fabric increment operation
   */
  private com.continuuity.data.operation.Increment toOperation(Increment increment) {
    com.continuuity.data.operation.Increment operation = new com.continuuity.data.operation.Increment(
      this.tableName(), increment.getRow(), increment.getColumns(), increment.getValues());
    operation.setMetricName(getMetricName());
    return operation;
  }

  @Override
  public void write(WriteOperation op) throws OperationException {
    this.getTransactionAgent().submit(toOperation(op));
  }

  @Override
  public Map<byte[], Long> incrementAndGet(Increment increment) throws OperationException {
    return this.getTransactionAgent().execute(toOperation(increment));
  }

  @Override
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) throws OperationException {
    OperationResult<List<KeyRange>> ranges = getTransactionAgent().
      execute(new GetSplits(this.tableName(), numSplits, start, stop));
    if (ranges.isEmpty()) {
      return Collections.emptyList();
    }
    List<Split> splits = Lists.newArrayListWithExpectedSize(ranges.getValue().size());
    for (KeyRange range : ranges.getValue()) {
      splits.add(new TableSplit(range.getStart(), range.getStop()));
    }
    return splits;
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

    TableSplit(byte[] start, byte[] stop) {
      this.start = start;
      this.stop = stop;
    }

    byte[] getStart() {
      return start;
    }

    byte[] getStop() {
      return stop;
    }
  }

  /**
   * Implements a split reader for a key range of a table, based on the Scanner implementation of the underlying
   * table implementation.
   */
  public class TableScanner extends SplitReader<byte[], Map<byte[], byte[]>> {

    // the underlying scanner
    private Scanner scanner;
    // the current key
    private byte[] key = null;
    // the current row, that is, a map from column key to value
    private Map<byte[], byte[]> row = null;

    @Override
    public void initialize(Split split) throws InterruptedException, OperationException {
      TableSplit tableSplit = (TableSplit) split;
      this.scanner = getTransactionAgent().scan(new Scan(tableName(), tableSplit.getStart(), tableSplit.getStop()));
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
}
