package com.continuuity.data.dataset;

import com.continuuity.api.data.Closure;
import com.continuuity.data.DataFabric;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.WriteOperation;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;

import java.util.Map;

/**
 * The read-only runtime implementation of the Table data set.
 */
public class ReadOnlyTable extends RuntimeTable {

  /**
   * Given a Table, create a new ReadOnlyTable and make it the delegate for that
   * table.
   *
   * @param table the original table
   * @param fabric the data fabric
   * @param proxy transaction proxy for all operations
   * @return the new ReadOnlyTable
   */
  public static ReadOnlyTable setReadOnlyTable(Table table, DataFabric fabric, TransactionProxy proxy) {
    ReadOnlyTable readOnlyTable = new ReadOnlyTable(table, fabric, proxy);
    table.setDelegate(readOnlyTable);
    return readOnlyTable;
  }

  /**
   * package-protected constructor, only to be called from @see #setReadOnlyTable()
   * and @see ReadWriteTable constructor
   * @param table the original table
   * @param proxy transaction proxy for all operations
   * @param fabric the data fabric
   */
  ReadOnlyTable(Table table, DataFabric fabric, TransactionProxy proxy) {
    super(table, fabric);
    this.proxy = proxy;
  }

  /** the transaction proxy for all operations */
  protected TransactionProxy proxy;

  protected TransactionAgent getTransactionAgent() {
    return this.proxy.getTransactionAgent();
  }

  // synchronous read
  @Override
  public OperationResult<Map<byte[], byte[]>> read(Read read)
    throws OperationException {

    if (read.getColumns() != null) {
      // this is a multi-column read
      com.continuuity.data.operation.Read op =
        new com.continuuity.data.operation.Read(this.tableName(), read.getRow(), read.getColumns());
      if (this.proxy != null) {
        // new-style: use transaction agent
        return this.getTransactionAgent().execute(op);
      } else {
        // old-style: use data fabric
        // TODO this will go away with the new flow system
        return this.getDataFabric().read(op);
      }
    } else {
      // this is a column-range red
      ReadColumnRange op = new ReadColumnRange(
        this.tableName(), read.getRow(), read.getStartCol(), read.getStopCol());
      if (this.proxy != null) {
        // new-style: use transaction agent
        return this.getTransactionAgent().execute(op);
      } else {
        // old-style: use data fabric
        // TODO this will go away with the new flow system
        return this.getDataFabric().read(op);
      }
    }
  }

  // no support for write operations
  @Override
  public void stage(WriteOperation op) throws OperationException {
    throw new UnsupportedOperationException("Write operations are not supported by read only table.");
  }

  // no support for write operations
  @Override
  public void exec(WriteOperation op) throws OperationException {
    throw new UnsupportedOperationException("Write operations are not supported by read only table.");
  }

  // no support for write operations, including the increment closure
  @Override
  public Closure closure(Increment increment) {
    throw new UnsupportedOperationException("Write operations are not supported by read only table.");
  }

}

