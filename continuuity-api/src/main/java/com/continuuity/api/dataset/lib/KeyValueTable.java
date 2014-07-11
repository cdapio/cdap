package com.continuuity.api.dataset.lib;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.RecordScannable;
import com.continuuity.api.data.batch.RecordScanner;
import com.continuuity.api.data.batch.Scannables;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Table;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A key/value map implementation on top of {@link Table} supporting read and write operations.
 */
public class KeyValueTable extends AbstractDataset implements
  BatchReadable<byte[], byte[]>,
    RecordScannable<KeyValue<byte[], byte[]>> {

  // the fixed single column to use for the key
  static final byte[] KEY_COLUMN = { 'c' };

  private final Table table;

  public KeyValueTable(String instanceName, Table table) {
    super(instanceName, table);
    this.table = table;
  }

  /**
   * Read the value for a given key.
   * @param key the key to read for
   * @return the value for that key, or null if no value was found
   */
  @Nullable
  public byte[] read(String key) {
    return read(Bytes.toBytes(key));
  }

  /**
   * Read the value for a given key.
   * @param key the key to read for
   * @return the value for that key, or null if no value was found
   */
  @Nullable
  public byte[] read(byte[] key) {
    return table.get(key, KEY_COLUMN);
  }

  /**
   * Increment the value for a given key and return the resulting value.
   * @param key the key to increment
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
   * Write a value to a key.
   *
   * @param key the key
   * @param value the new value
   */
  public void write(String key, String value) {
    this.table.put(Bytes.toBytes(key), KEY_COLUMN, Bytes.toBytes(value));
  }

  /**
   * Write a value to a key.
   *
   * @param key the key
   * @param value the new value
   */
  public void write(String key, byte[] value) {
    this.table.put(Bytes.toBytes(key), KEY_COLUMN, value);
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
   * Compares-and-swaps (atomically) the value of the specified row and column
   * by looking for the specified expected value and if found, replacing with
   * the specified new value.
   *
   * @param key key to modify
   * @param oldValue expected value before change
   * @param newValue value to set
   * @return true if compare and swap succeeded, false otherwise (stored value is different from expected)
   */
  public boolean swap(byte[] key, byte[] oldValue, byte[] newValue) throws Exception {
    return this.table.compareAndSwap(key, KEY_COLUMN, oldValue, newValue);
  }

  @Override
  public Type getRecordType() {
    return new TypeToken<KeyValue<byte[], byte[]>>() { }.getType();
  }

  @Override
  public List<Split> getSplits() {
    return table.getSplits();
  }

  @Override
  public RecordScanner<KeyValue<byte[], byte[]>> createSplitRecordScanner(Split split) {
    return Scannables.splitRecordScanner(createSplitReader(split), new KeyValueRecordMaker());
  }

  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    return table.getSplits(numSplits, start, stop);
  }

  @Override
  public SplitReader<byte[], byte[]> createSplitReader(Split split) {
    return new KeyValueScanner(table.createSplitReader(split));
  }

  /**
   * {@link com.continuuity.api.data.batch.Scannables.RecordMaker} for {@link #createSplitReader(Split)}.
   */
  public class KeyValueRecordMaker implements Scannables.RecordMaker<byte[], byte[], KeyValue<byte[], byte[]>> {
    @Override
    public KeyValue<byte[], byte[]> makeRecord(byte[] key, byte[] value) {
      return new KeyValue<byte[], byte[]>(key, value);
    }
  }

  /**
   * The split reader for KeyValueTable.
   */
  public class KeyValueScanner extends SplitReader<byte[], byte[]> {

    // the underlying KeyValueTable's split reader
    private SplitReader<byte[], Row> reader;

    public KeyValueScanner(SplitReader<byte[], Row> reader) {
      this.reader = reader;
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
