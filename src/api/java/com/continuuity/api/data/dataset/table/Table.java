package com.continuuity.api.data.dataset.table;

import com.continuuity.api.data.*;

import java.util.Map;

/**
 * This is the DataSet implementation of named tables. Other DataSets can be
 * defined by embedding instances Table (and other DataSets).
 *
 * A Table can execute operations on its data, including read, write,
 * delete etc. Internally, the table delegates all operations to the
 * current transaction of the context in which this data set was
 * instantiated (a flowlet, or a procedure). Depending on that context,
 * operations may be executed immediately or deferred until the transaction
 * is committed.
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
   * Perform a read in the context of the current transaction.
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
   * Perform a write operation in the context of the current transaction.
   * @param op The write operation
   * @throws OperationException if something goes wrong
   *
   * TODO this method will be renamed to write() in the new flow system
   */
  // @Deprecated
  public void write(WriteOperation op) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    this.delegate.write(op);
  }

  /**
   * Perform an increment and return the incremented value(s).
   * @param increment the increment operation
   * @return a map with the incremented values as Long
   * @throws OperationException if something goes wrong
   *
   * TODO this method will go away with the new flow system
   */
  public Map<byte[], Long> incrementAndGet(Increment increment) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.incrementAndGet(increment);
  }

}
