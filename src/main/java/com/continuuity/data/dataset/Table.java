package com.continuuity.data.dataset;

import com.continuuity.api.data.*;

import java.util.Collections;
import java.util.Map;

/**
 * This is the DataSet implementation of named tables. Other DataSets can be
 * defined by embedding instances Table (and other DataSets).
 *
 * A Table can execute operations on its data, including read, write,
 * delete etc. These operations can be performed in one of two ways:
 * <li>Synchronously: The operation is executed immediately against the
 *   data fabric, in its own transaction. This is supported for all types
 *   of operations. </li>
 * <li>Asynchronously: The operation is staged for execution as part of
 *   the transaction of the context in which this data set was
 *   instantiated (a flowlet, or a procedure). In this case,
 *   the actual execution is delegated to the context. This is useful
 *   when multiple operations, possibly over multiple table,
 *   must be performed atomically. This is only supported for write
 *   operations.</li>
 *
 * The Table relies on injection of the data fabric by the execution context.
 * (@see DataSet).
 */
public class Table extends DataSet {

  /** construct by name */
  public Table(String name) {
    super(name);
  }

  // these two must be injected through the execution context
  private DataFabric dataFabric = null;
  private SimpleBatchCollectionClient collectionClient = null;

  private BatchCollector getCollector() {
    return this.collectionClient.getCollector();
  }

  public Table(DataSetMeta meta) {
    super(meta);
  }

  public void open() throws OperationException {
    this.dataFabric.openTable(this.getName());
  }

  @Override
  DataSetMeta.Builder configure() {
    return new DataSetMeta.Builder(this);
  }

  private String tableName() {
    return this.getName();
  }

  public OperationResult<Map<byte[], byte[]>> read(Read read)
      throws OperationException {
    if (read.columns != null) {
      return this.dataFabric.read(new com.continuuity.api.data.Read(
          this.tableName(), read.row, read.columns));
    } else {
      return this.dataFabric.read(new ReadColumnRange(
          this.tableName(), read.row, read.startCol, read.stopCol));
    }
  }

  enum Mode { Sync, Async }

  private void execute(WriteOp op, Mode mode) throws OperationException {
    WriteOperation operation;
    if (op instanceof Write) {
      Write write = (Write)op;
      operation = new com.continuuity.api.data.Write(
          this.tableName(), write.row, write.columns, write.values);
    }
    else if (op instanceof Delete) {
      Delete delete = (Delete)op;
      operation = new com.continuuity.api.data.Delete(
          this.tableName(), delete.row, delete.columns);
    }
    else if (op instanceof Increment) {
      Increment increment = (Increment)op;
      operation = new com.continuuity.api.data.Increment(
          this.tableName(), increment.row, increment.columns, increment.values);
    }
    else if (op instanceof Swap) {
      Swap swap = (Swap)op;
      operation = new CompareAndSwap(
          this.tableName(), swap.row, swap.column, swap.expected, swap.value);
    }
    else { // can't happen but...
      return;
    }

    if (mode.equals(Mode.Async)) {
      this.getCollector().add(operation);
    } else {
      this.dataFabric.execute(Collections.singletonList(operation));
    }
  }

  public void stage(WriteOp op) throws OperationException {
    execute(op, Mode.Async);
  }

  public void exec(WriteOp op) throws OperationException {
    execute(op, Mode.Sync);
  }

  public static interface WriteOp {
  }

  public static class Read {
    byte[] row;
    byte[][] columns;
    byte[] startCol;
    byte[] stopCol;

    public Read(byte[] row, byte[][] columns) {
      this.row = row;
      this.columns = columns;
      this.startCol = this.stopCol = null;
    }

    public Read(byte[] row, byte[] column) {
      this(row, new byte[][] { column });
    }

    public Read(byte[] row, byte[] start, byte[] stop) {
      this.row = row;
      this.columns = null;
      this.startCol = start;
      this.stopCol = stop;
    }
  }

  public static class Write implements WriteOp {
    byte[] row;
    byte[][] columns;
    byte[][] values;

    public Write(byte[] row, byte[][] columns, byte[][] values) {
      this.row = row;
      this.columns = columns;
      this.values = values;
    }

    public Write(byte[] row, byte[] column, byte[] value) {
      this(row, new byte[][] { column }, new byte[][] { value });
    }
  }

  public static class Increment implements WriteOp {
    byte[] row;
    byte[][] columns;
    long[] values;

    public Increment(byte[] row, byte[][] columns, long[] values) {
      this.row = row;
      this.columns = columns;
      this.values = values;
    }

    public Increment(byte[] row, byte[] column, long value) {
      this(row, new byte[][] { column }, new long[] { value });
    }
  }

  public static class Delete implements WriteOp {
    byte[] row;
    byte[][] columns;

    public Delete(byte[] row, byte[][] columns) {
      this.row = row;
      this.columns = columns;
    }

    public Delete(byte[] row, byte[] column) {
      this(row, new byte[][] { column });
    }
  }

  public static class Swap implements WriteOp {
    byte[] row;
    byte[] column;
    byte[] expected;
    byte[] value;

    public Swap(byte[] row, byte[] column, byte[] expected, byte[] value) {
      this.row = row;
      this.column = column;
      this.expected = expected;
      this.value = value;
    }
  }

}
