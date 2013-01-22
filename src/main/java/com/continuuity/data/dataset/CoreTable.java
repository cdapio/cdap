package com.continuuity.data.dataset;

import com.continuuity.api.data.*;
import com.continuuity.api.data.set.Table;
import com.continuuity.data.operation.IncrementClosure;

import java.util.Collections;
import java.util.Map;

public class CoreTable extends Table {

  public static CoreTable setCoreTable(Table table, DataFabric fabric,
                                       BatchCollectionClient client) {
    CoreTable coreTable = new CoreTable(table, fabric, client);
    table.setDelegate(coreTable);
    return coreTable;
  }

  private CoreTable(Table table,
                   DataFabric fabric,
                   BatchCollectionClient client) {
    super(table.getName());
    this.dataFabric = fabric;
    this.collectionClient = client;
  }

  private DataFabric dataFabric = null;
  private BatchCollectionClient collectionClient = null;

  /** helper method to get the batch collector from the collection client */
  private BatchCollector getCollector() {
    return this.collectionClient.getCollector();
  }

  /**
   * open the table in the data fabric, to ensure it exists
   * @throws OperationException if something goes wrong
   */
  public void open() throws OperationException {
    this.dataFabric.openTable(this.getName());
  }

  /**
   * Perform a read as a synchronous operation.
   * @param read a Read operation
   * @return the result of the read
   * @throws OperationException if the operation fails
   */
  @Override
  public OperationResult<Map<byte[], byte[]>> read(Read read)
      throws OperationException {
    if (read.getColumns() != null) {
      return this.dataFabric.read(new com.continuuity.api.data.Read(
          this.tableName(), read.getRow(), read.getColumns()));
    } else {
      return this.dataFabric.read(new ReadColumnRange(
          this.tableName(), read.getRow(), read.getStartCol(), read.getStopCol()));
    }
  }
  /** helper enum */
  enum Mode { Sync, Async }

  /**
   * Perform a write operation. If the mode is synchronous, then the write is
   * executed immediately in its own transaction, if it is asynchronous, the
   * write is appended to the current transaction, which will be committed
   * by the executing agent.
   * @param op The write operation
   * @param mode The execution mode
   * @throws com.continuuity.api.data.OperationException if something goes wrong
   */
  private void execute(WriteOperation op, Mode mode) throws OperationException {
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
      return;
    }

    if (mode.equals(Mode.Async)) {
      this.getCollector().add(operation);
    } else {
      this.dataFabric.execute(Collections.singletonList(operation));
    }
  }

  /** perform an asynchronous write operation (@see execute()) */
  @Override
  public void stage(WriteOperation op) throws OperationException {
    execute(op, Mode.Async);
  }

  /** perform a synchronous write operation (@see execute()) */
  @Override
  public void exec(WriteOperation op) throws OperationException {
    execute(op, Mode.Sync);
  }

  @Override
  public Closure closure(Increment increment) {
    return new IncrementClosure(new com.continuuity.api.data.Increment
        (this.tableName(), increment.getRow(), increment.getColumns(), increment.getValues()));
  }


}
