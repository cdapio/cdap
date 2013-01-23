package com.continuuity.api.data.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.*;

import java.util.Map;

/**
 * This class implements a key/value map on top of Table. Supported
 * operations are read, write, delete, and swap.
 */
public class KeyValueTable extends DataSet {

  // the fixed single column to use for the key
  static final byte[] KEY_COLUMN = { 'k', 'e', 'y' };

  // the underlying table
  private Table table;

  /** constructor for configuration */
  public KeyValueTable(String name) {
    super(name);
    this.table = new Table("kv_" + name);
  }

  /** Constructor for runtime */
  @SuppressWarnings("unused")
  public KeyValueTable(DataSetSpecification spec) throws OperationException {
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
  public void stage(KeyOperation op) throws OperationException {
    this.table.stage(op);
  }

  static interface KeyOperation extends WriteOperation {  }

  public static class ReadKey extends Read implements KeyOperation {
    public ReadKey(byte[] key) {
      super(key, KEY_COLUMN);
    }
  }
  public static class WriteKey extends Write implements KeyOperation {
    public WriteKey(byte[] key, byte[] value) {
      super(key, KEY_COLUMN, value);
    }
  }
  public static class DeleteKey extends Delete implements KeyOperation {
    public DeleteKey(byte[] key) {
      super(key, KEY_COLUMN);
    }
  }
  public static class SwapKey extends Swap implements KeyOperation {
    public SwapKey(byte[] row, byte[] expected, byte[] value) {
      super(row, KEY_COLUMN, expected, value);
    }
  }
}