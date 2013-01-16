package com.continuuity.data.dataset;

import com.continuuity.api.data.*;

import java.util.Collections;
import java.util.Map;

public class Table extends DataSet {

  public Table(String name) {
    super(name);
  }

  private DataFabric fabric;
  private BatchCollector collector;

  @Override
  void initialize(DataSetMeta meta) throws OperationException {
    this.fabric.openTable(this.getName());
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
      return this.fabric.read(new com.continuuity.api.data.Read(
          this.tableName(), read.row, read.columns));
    } else {
      return this.fabric.read(new ReadColumnRange(
          this.tableName(), read.row, read.startCol, read.stopCol));
    }
  }

  enum Mode { Sync, Async }

  private void execute(WriteOp op, Mode mode) throws OperationException {
    WriteOperation operation = null;
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
      this.collector.add(operation);
    } else {
      this.fabric.execute(Collections.singletonList(operation));
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
