package com.continuuity.test.app;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.data.dataset.table.Table;

import java.util.List;
import javax.annotation.Nullable;

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
    this.table = new Table("one");
  }

  /**
   * Read the value for a given key.
   * @param key the key to read for
   * @return the value for that key, or null if no value was found
   */
  @Nullable
  public byte[] read(byte[] key) {
    return this.table.get(key, KEY_COLUMN);
  }

  /**
   * Increment the value for a given key and return the resulting value.
   * @param key the key to incrememt
   * @return the incremented value of that key
   */
  public long incrementAndGet(byte[] key, long value) {
    return this.table.increment(key, KEY_COLUMN, value);
  }

  /**
   * Write a value to a key.
   *
   * @param key the key
   * @param value the new value
   */
  public void write(byte[] key, byte[] value) {
    this.table.put(key, KEY_COLUMN, value);
  }

  /**
   * Increment the value tof a key. The key must either not exist yet, or its
   * current value must be 8 bytes long to be interpretable as a long.
   * @param key the key
   * @param value the new value
   */
  public void increment(byte[] key, long value) {
    this.table.increment(key, KEY_COLUMN, value);
  }

  /**
   * Delete a key.
   * @param key the key to delete
   */
  public void delete(byte[] key) {
    this.table.delete(key, KEY_COLUMN);
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
   */
  public void swap(byte[] key, byte[] oldValue, byte[] newValue) {
    this.table.compareAndSwap(key, KEY_COLUMN, oldValue, newValue);
  }

  // Batch integration functionality

  @Override
  public List<Split> getSplits() {
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
    private SplitReader<byte[], Row> reader;

    public KeyValueScanner(Split split) {
      this.reader = table.createSplitReader(split);
    }

    @Override
    public void initialize(Split split) throws InterruptedException {
      this.reader.initialize(split);
    }

    @Override
    public boolean nextKeyValue() throws InterruptedException {
      return this.reader.nextKeyValue();
    }

    @Override
    public byte[] getCurrentKey() throws InterruptedException {
      return this.reader.getCurrentKey();
    }

    @Override
    public byte[] getCurrentValue() throws InterruptedException {
      return this.reader.getCurrentValue().get(KEY_COLUMN);
    }

    @Override
    public void close() {
      this.reader.close();
    }
  }
}
