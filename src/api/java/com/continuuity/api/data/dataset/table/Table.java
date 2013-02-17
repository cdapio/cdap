package com.continuuity.api.data.dataset.table;

import com.continuuity.api.data.*;

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

  // this is the Table that executed the actual operations. using a delegate
  // allows us to inject a different implementation.
  private Table delegate = null;

  /**
   * Constructor by name
   * @param name the name of the table
   */
  public Table(String name) {
    super(name);
  }

  /**
   * Runtime initialization, only calls the super class
   * @param spec the data set spec for this data set
   */
  public Table(DataSetSpecification spec) {
    super(spec);
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this).create();
  }

  /**
   * helper to return the name of the physical table. currently the same as
   * the name of the (Table) data set.
   * @return the name of the underlying table in the data fabric
   */
  protected String tableName() {
    return this.getName();
  }

  /**
   * sets the Table to which all operations are delegated. This can be used
   * to inject different implementations.
   * @param table the implementation to delegate to
   */
  public void setDelegate(Table table) {
    this.delegate = table;
  }

  /**
   * Perform a read as a synchronous operation.
   * @param read a Read operation
   * @return the result of the read
   * @throws OperationException if the operation fails
   */
  public OperationResult<Map<byte[], byte[]>> read(@SuppressWarnings("unused") Read read) throws
      OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.read(read);
  }

  /**
   * Add a write operation to the current transaction. The execution is
   * deferred until the time when the current transaction is executed and
   * (asynchronously) committed as a batch, by the executing context (a
   * flowlet or a query etc.)
   * @param op The write operation
   * @throws OperationException if something goes wrong
   *
   * TODO this method will be renamed to write() in the new flow system
   */
  // @Deprecated
  public void stage(WriteOperation op) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    this.delegate.stage(op);
  }

  /**
   * Perform a write operation synchronously. It is executed immediately in
   * its own transaction, outside of the current transaction of the
   * execution context (flowlet, query, etc.).
   * @param op The write operation
   * @throws OperationException if something goes wrong
   *
   * TODO this method will go away with the new flow system
   */
  // @Deprecated
  public void exec(WriteOperation op) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    this.delegate.exec(op);
  }

  /**
   * Returns a "closure", that is an encapsulated operation that,
   * when executed, returns a value that can be used by another operation.
   * The only supported type of closure at this time is an Incrememt: it
   * returns a long value, which can be used as a field value in a tuple.
   * @param op the increment operation, must be on a single column.
   * @return a closure encapsulating the increment operation
   * @throws OperationException if something goes wrong
   *
   * TODO this method will go away with the new flow system
   */
  // @Deprecated
  public Closure closure(Increment op) {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.closure(op);
  }

}
