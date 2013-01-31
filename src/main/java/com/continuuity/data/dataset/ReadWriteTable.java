package com.continuuity.data.dataset;


import com.continuuity.api.data.BatchCollectionClient;
import com.continuuity.api.data.BatchCollector;
import com.continuuity.api.data.Closure;
import com.continuuity.api.data.CompareAndSwap;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Swap;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.api.data.dataset.table.WriteOperation;
import com.continuuity.data.operation.IncrementClosure;

import java.util.Collections;

/**
 * The read/write runtime implementation of the Table data set.
 */
public class ReadWriteTable extends ReadOnlyTable {

  /**
   * Given a Table, create a new ReadWriteTable and make it the delegate for that
   * table.
   *
   * @param table the original table
   * @param fabric the data fabric
   * @param client the batch collection client
   * @return the new ReadWriteTable
   */
  public static ReadWriteTable setReadWriteTable(Table table, DataFabric fabric, BatchCollectionClient client) {
    ReadWriteTable readWriteTable = new ReadWriteTable(table, fabric, client);
    table.setDelegate(readWriteTable);
    return readWriteTable;
  }

  /**
   * private constructor, only to be called from @see #setReadWriteTable().
   * @param table the original table
   * @param fabric the data fabric
   * @param client the batch collection client
   */
  private ReadWriteTable(Table table, DataFabric fabric, BatchCollectionClient client) {
    super(table, fabric);
    this.collectionClient = client;
  }

  // the batch collection client for executing asynchronous operations
  private BatchCollectionClient collectionClient = null;

  /**
   * helper method to get the batch collector from the collection client
   * @return the current batch collector
   */
  private BatchCollector getCollector() {
    return this.collectionClient.getCollector();
  }

  /**
   * Perform a write operation. If the mode is synchronous, then the write is
   * executed immediately in its own transaction, if it is asynchronous, the
   * write is appended to the current transaction, which will be committed
   * by the executing agent.
   *
   * @param op a table write operation
   * @return the corresponding data fabric operation
   */
  private com.continuuity.api.data.WriteOperation toOperation(WriteOperation op) {
    com.continuuity.api.data.WriteOperation operation;
    if (op instanceof Write) {
      Write write = (Write)op;
      operation = new com.continuuity.api.data.Write(
          this.tableName(), write.getRow(), write.getColumns(), write.getValues());
    }
    else if (op instanceof Delete) {
      Delete delete = (Delete)op;
      operation = new com.continuuity.api.data.Delete(
          this.tableName(), delete.getRow(), delete.getColumns());
    }
    else if (op instanceof Increment) {
      Increment increment = (Increment)op;
      operation = new com.continuuity.api.data.Increment(
          this.tableName(), increment.getRow(), increment.getColumns(), increment.getValues());
    }
    else if (op instanceof Swap) {
      Swap swap = (Swap)op;
      operation = new CompareAndSwap(
          this.tableName(), swap.getRow(), swap.getColumn(), swap.getExpected(), swap.getValue());
    }
    else { // can't happen but...
      throw new IllegalArgumentException("Received an operation of unknown type " + op.getClass().getName());
    }
    return operation;
  }

  // perform an asynchronous write operation (see toOperation())
  @Override
  public void stage(WriteOperation op) {
    this.getCollector().add(toOperation(op));
  }

  // perform a synchronous write operation (see toOperation())
  @Override
  public void exec(WriteOperation op) throws OperationException {
    this.getDataFabric().execute(Collections.singletonList(toOperation(op)));
  }

  // get a closure for an increment
  @Override
  public Closure closure(Increment increment) {
    return new IncrementClosure(new com.continuuity.api.data.Increment
        (this.tableName(), increment.getRow(), increment.getColumns(), increment.getValues()));
  }


}
