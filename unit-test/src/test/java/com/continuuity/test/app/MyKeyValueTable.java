package com.continuuity.test.app;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Swap;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.data.operation.StatusCode;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * This class implements a key/value map on top of Table. Supported
 * operations are read, write, delete, and swap.
 */
public class MyKeyValueTable extends DataSet implements BatchReadable<byte[], byte[]>, BatchWritable<byte[], byte[]> {

  // the fixed single column to use for the key
  static final byte[] KEY_COLUMN = { 'c' };

  // the underlying table
  private Table table;

  /**
   * Constructor for configuration of key-value table.
   * @param name the name of the table
   */
  public MyKeyValueTable(String name) {
    super(name);
    this.table = new Table("kv." + name);
  }

  /**
   * Constructor for runtime (@see DataSet#DataSet(DataSetSpecification)).
   * @param spec the data set spec for this data set
   */
  @SuppressWarnings("unused")
  public MyKeyValueTable(DataSetSpecification spec) {
    super(spec);
    this.table = new Table(spec.getSpecificationFor("kv." + this.getName()));
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
   * @throws OperationException if the read fails
   */
  @Nullable
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
   * Increment the value for a given key and return the resulting value.
   * @param key the key to incrememt
   * @return the incremented value of that key
   * @throws OperationException if the increment fails
   */
  public long incrementAndGet(byte[] key, long value) throws OperationException {
    Map<byte[], Long> result =
      this.table.incrementAndGet(new Increment(key, KEY_COLUMN, value));
    Long newValue = result.get(KEY_COLUMN);
    if (newValue == null) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, "Incremented value not part of operation result.");
    } else {
      return newValue;
    }
  }

  /**
   * Write a value to a key.
   * @param key the key
   * @param value the new value
   * @throws OperationException if the write fails
   */
  public void write(byte[] key, byte[] value) throws OperationException {
    this.table.write(new Write(key, KEY_COLUMN, value));
  }

  /**
   * Increment the value tof a key. The key must either not exist yet, or its
   * current value must be 8 bytes long to be interpretable as a long.
   * @param key the key
   * @param value the new value
   * @throws OperationException if the increment fails
   */
  public void increment(byte[] key, long value) throws OperationException {
    this.table.write(new Increment(key, KEY_COLUMN, value));
  }

  /**
   * Delete a key.
   * @param key the key to delete
   * @throws OperationException if the delete fails
   */
  public void delete(byte[] key) throws OperationException {
    this.table.write(new Delete(key, KEY_COLUMN));
  }

  /**
   * Compare the value for key with an expected value, and,
   * if they match, to replace the value with a new value. If they don't
   * match, this operation fails with status code WRITE_CONFLICT.
   *
   * An expected value of null means that the key must not exist. A new value
   * of null means that the key shall be deleted instead of replaced.
   *
   * @param key the key to delete
   * @throws OperationException if the swap fails
   */
  public void swap(byte[] key, byte[] oldValue, byte[] newValue) throws OperationException {
    this.table.write(new Swap(key, KEY_COLUMN, oldValue, newValue));
  }

  // Batch integration functionality

  @Override
  public List<Split> getSplits() throws OperationException {
    return this.table.getSplits();
  }

  @Override
  public SplitReader<byte[], byte[]> createSplitReader(Split split) {
    return new KeyValueScanner(split);
  }

  /**
   * The split reader for key/value is reading table split using the underlying Table's split reader.
   */
  public class KeyValueScanner extends SplitReader<byte[], byte[]> {

    // the underlying table's split reader
    private SplitReader<byte[], Map<byte[], byte[]>> reader;

    public KeyValueScanner(Split split) {
      this.reader = table.createSplitReader(split);
    }

    @Override
    public void initialize(Split split) throws InterruptedException, OperationException {
      this.reader.initialize(split);
    }

    @Override
    public boolean nextKeyValue() throws InterruptedException, OperationException {
      return this.reader.nextKeyValue();
    }

    @Override
    public byte[] getCurrentKey() throws InterruptedException {
      return this.reader.getCurrentKey();
    }

    @Override
    public byte[] getCurrentValue() throws InterruptedException, OperationException {
      return this.reader.getCurrentValue().get(KEY_COLUMN);
    }

    @Override
    public void close() {
      this.reader.close();
    }
  }
}
