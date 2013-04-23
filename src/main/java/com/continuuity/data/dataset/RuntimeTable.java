package com.continuuity.data.dataset;

import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.AsyncIncrement;
import com.continuuity.api.data.dataset.table.AsyncWrite;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Swap;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.api.data.dataset.table.WriteOperation;
import com.continuuity.data.DataFabric;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;

import java.util.Map;

/**
 * Base class for runtime implementations of Table.
 */
public abstract class RuntimeTable extends Table {

  /**
   * package-protected constructor, only to be called from @see #setReadOnlyTable()
   * and @see ReadWriteTable constructor
   * @param table the original table
   * @param fabric the data fabric
   */
  RuntimeTable(Table table, DataFabric fabric, TransactionProxy proxy) {
    super(table.getName());
    this.dataFabric = fabric;
    this.proxy = proxy;
  }

  /** the data fabric to use for executing synchronous operations */
  private final DataFabric dataFabric;

  /** the transaction proxy for all operations */
  private final TransactionProxy proxy;

  /** the name to use for metrics collection, typically the name of the enclosing dataset */
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
   * set the name to use for metrics
   * @param metricName the name to use for emitting metrics
   */
  protected void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  /**
   * open the table in the data fabric, to ensure it exists and is accessible.
   * @throws com.continuuity.api.data.OperationException if something goes wrong
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
    if (op instanceof AsyncWrite) {
      AsyncWrite write = (AsyncWrite)op;
      operation = new com.continuuity.data.operation.AsyncWrite(
        this.tableName(), write.getRow(), write.getColumns(), write.getValues());
    }
    else if (op instanceof Write) {
      Write write = (Write)op;
      operation = new com.continuuity.data.operation.Write(
        this.tableName(), write.getRow(), write.getColumns(), write.getValues());
    }
    else if (op instanceof Delete) {
      Delete delete = (Delete)op;
      operation = new com.continuuity.data.operation.Delete(
        this.tableName(), delete.getRow(), delete.getColumns());
    }
    else if (op instanceof AsyncIncrement) {
      operation = toOperation((AsyncIncrement)op);
    }
    else if (op instanceof Increment) {
      operation = toOperation((Increment)op);
    }
    else if (op instanceof Swap) {
      Swap swap = (Swap)op;
      operation = new CompareAndSwap(
        this.tableName(), swap.getRow(), swap.getColumn(), swap.getExpected(), swap.getValue());
    }
    else { // can't happen but...
      throw new IllegalArgumentException("Received an operation of unknown type " + op.getClass().getName());
    }
    operation.setMetricName(getMetricName());
    return operation;
  }

  /**
   * Helper to convert an increment operation
   * @param increment the table increment
   * @return a corresponding data fabric increment operation
   */
  private com.continuuity.data.operation.Increment toOperation(Increment increment) {
    com.continuuity.data.operation.Increment operation = new com.continuuity.data.operation.Increment(
      this.tableName(), increment.getRow(), increment.getColumns(), increment.getValues());
    operation.setMetricName(getMetricName());
    return operation;
  }

  private com.continuuity.data.operation.Increment toOperation(AsyncIncrement increment) {
    com.continuuity.data.operation.AsyncIncrement operation = new com.continuuity.data.operation.AsyncIncrement(
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
}
