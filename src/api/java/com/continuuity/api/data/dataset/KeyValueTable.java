package com.continuuity.api.data.dataset;

import com.continuuity.api.data.Closure;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Swap;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.api.data.dataset.table.WriteOperation;

import java.util.Map;

/**
 * This class implements a key/value map on top of Table. Supported
 * operations are read, write, delete, and swap.
 */
public class KeyValueTable extends DataSet {

  // the fixed single column to use for the key
  static final byte[] KEY_COLUMN = { 'c' };

  // the underlying table
  private Table table;

  /**
   * constructor for configuration
   * @param name the name of the table
   */
  public KeyValueTable(String name) {
    super(name);
    this.table = new Table("kv_" + name);
  }

  /**
   * Constructor for runtime (@see DataSet#DataSet(DataSetSpecification))
   * @param spec the data set spec for this data set
   * @throws OperationException if the underlying table cannot be opened in
   *         the data fabric
   */
  @SuppressWarnings("unused")
  public KeyValueTable(DataSetSpecification spec) {
    super(spec);
    this.table = new Table(spec.getSpecificationFor("kv_" + this.getName()));
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this).
        dataset(this.table.configure()).create();
  }

  /**
   * Read the value for a given key.
   * @param key the key to read for
   * @return the value for that key, or null if no value was found
   */
  public byte[] read(byte[] key) throws OperationException {
    OperationResult<Map<byte[], byte[]>> result =
        this.table.read(new Read(key, KEY_COLUMN));
    if (result.isEmpty()) {
      return null;
    } else {
      return result.getValue().get(KEY_COLUMN);
    }
  }

  /**
   * Synchronous execution of a write operation
   * @param op the write operation
   * @throws OperationException if the operation fails
   */
  public void exec(KeyOperation op) throws OperationException {
    this.table.exec(op);
  }

  /**
   * Asynchronous execution of a write operation
   * @param op the write operation
   * @throws OperationException if the operation fails
   */
  public void stage(KeyOperation op) {
    this.table.stage(op);
  }

  /**
   * Get a closure for an increment operation
   * @param increment the increment operation.
   * @return a closure encapsulating the increment operation
   */
  public Closure closure(IncrementKey increment) {
    return this.table.closure(increment);
  }

  /**
   * Base interface for all key/value operations
   */
  static interface KeyOperation extends WriteOperation {  }

  /**
   * A read operation for a given key
   */
  public static class ReadKey extends Read implements KeyOperation {
    public ReadKey(byte[] key) {
      super(key, KEY_COLUMN);
    }
  }

  /**
   * An operation to write a new value for a given key
   */
  public static class WriteKey extends Write implements KeyOperation {
    public WriteKey(byte[] key, byte[] value) {
      super(key, KEY_COLUMN, value);
    }
  }

  /**
   * An operation to increment the value for a given key
   */
  public static class IncrementKey extends Increment implements KeyOperation {
    public IncrementKey(byte[] key, long value) {
      super(key, KEY_COLUMN, value);
    }
    public IncrementKey(byte[] key) {
      this(key, 1L);
    }
  }

  /**
   * An operation to delete a given key
   */
  public static class DeleteKey extends Delete implements KeyOperation {
    public DeleteKey(byte[] key) {
      super(key, KEY_COLUMN);
    }
  }

  /**
   * An operation to compare the value for key with an expected value, and,
   * if they match, to replace the value with a new value. If they don't
   * match, the operation fails with status code WRITE_CONFLICT.
   *
   * An expected value of null means that the key must not exist. A new value
   * of null means that the key shall be deleted instead of replaced.
   */
  public static class SwapKey extends Swap implements KeyOperation {
    public SwapKey(byte[] row, byte[] expected, byte[] value) {
      super(row, KEY_COLUMN, expected, value);
    }
  }
}