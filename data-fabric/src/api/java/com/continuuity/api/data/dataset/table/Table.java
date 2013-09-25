package com.continuuity.api.data.dataset.table;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;

import javax.annotation.Nullable;
import java.util.List;
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
public class Table extends DataSet implements
  BatchReadable<byte[], Map<byte[], byte[]>>, BatchWritable<byte[], Map<byte[], byte[]>> {

  // this is the Table that executed the actual operations. using a delegate
  // allows us to inject a different implementation.
  private Table delegate = null;

  private ConflictDetection conflictLevel;

  /**
   * Constructor by name.
   * @param name the name of the table
   */
  public Table(String name) {
    this(name, ConflictDetection.ROW);
  }

  /**
   * Constructor by name.
   * @param name the name of the table
   * @param level level on which to detect conflicts in changes made by different transactions
   */
  public Table(String name, ConflictDetection level) {
    super(name);
    this.conflictLevel = level;
  }

  /**
   * Defines level on which to resolve conflicts of the changes made in different transactions.
   */
  public static enum ConflictDetection {
    ROW,
    COLUMN
  }

  /**
   * Runtime initialization, only calls the super class.
   * @param spec the data set spec for this data set
   */
  public Table(DataSetSpecification spec) {
    super(spec);
    this.conflictLevel = ConflictDetection.valueOf(spec.getProperty(getName() + ".conflict.level"));
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
      .property(getName() + ".conflict.level", conflictLevel.name())
      .create();
  }

  /**
   * Helper to return the name of the physical table. Currently the same as
   * the name of the (Table) data set.
   * @return the name of the underlying table in the data fabric
   */
  protected String tableName() {
    return this.getName();
  }

  /**
   * @return conflict detection level
   */
  public ConflictDetection getConflictLevel() {
    return conflictLevel;
  }

  /**
   * Sets the Table to which all operations are delegated. This can be used
   * to inject different implementations.
   * @param table the implementation to delegate to
   */
  public void setDelegate(Table table) {
    this.delegate = table;
  }

  /**
   * @return delegate which was set by {@link #setDelegate(Table)}
   */
  public Table getDelegate() {
    return delegate;
  }

  /**
   * Perform a read in the context of the current transaction.
   * @param read a Read operation
   * @return the result of the read
   * @throws OperationException if the operation fails
   */
  public OperationResult<Map<byte[], byte[]>> read(Read read) throws
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
   */
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
   */
  public Map<byte[], Long> incrementAndGet(Increment increment) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.incrementAndGet(increment);
  }

  /**
   * Scan rows of this table.
   * @param startRow start row inclusive. {@code null} means start from first row of the table
   * @param stopRow stop row exclusive. {@code null} means scan all rows to the end of the table
   * @return instance of {@link Scanner}
   * @throws OperationException
   */
  public Scanner scan(@Nullable byte[] startRow, @Nullable byte[] stopRow) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.scan(startRow, stopRow);
  }

  @Override
  public List<Split> getSplits() throws OperationException {
    return getSplits(-1, null, null);
  }

  /**
   * Returns splits for a range of keys in the table.
   * @param numSplits Desired number of splits. If greater than zero, at most this many splits will be returned.
   *                  If less or equal to zero, any number of splits can be returned.
   * @param start If non-null, the returned splits will only cover keys that are greater or equal.
   * @param stop If non-null, the returned splits will only cover keys that are less.
   * @return list of {@link Split}
   */
  @Beta
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.getSplits(numSplits, start, stop);
  }

  @Override
  public SplitReader<byte[], Map<byte[], byte[]>> createSplitReader(Split split) {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.createSplitReader(split);
  }

  @Override
  public void write(byte[] key, Map<byte[], byte[]> row) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    this.delegate.write(key, row);
  }
}
