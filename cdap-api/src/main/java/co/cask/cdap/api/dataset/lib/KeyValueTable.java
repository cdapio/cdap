/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.data.batch.Scannables;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A key/value map implementation on top of {@link Table} supporting read, write and delete operations.
 */
public class KeyValueTable extends AbstractDataset implements
  BatchReadable<byte[], byte[]>, BatchWritable<byte[], byte[]>,
  RecordScannable<KeyValue<byte[], byte[]>>, RecordWritable<KeyValue<byte[], byte[]>> {

  // the fixed single column to use for the key
  static final byte[] KEY_COLUMN = { 'c' };

  private final Table table;

  public KeyValueTable(String instanceName, Table table) {
    super(instanceName, table);
    this.table = table;
  }

  /**
   * Read the value for a given key.
   *
   * @param key the key to read for
   * @return the value for that key, or null if no value was found
   */
  @Nullable
  public byte[] read(String key) {
    return read(Bytes.toBytes(key));
  }

  /**
   * Read the value for a given key.
   *
   * @param key the key to read for
   * @return the value for that key, or null if no value was found
   */
  @Nullable
  public byte[] read(byte[] key) {
    return table.get(key, KEY_COLUMN);
  }

  /**
   * Reads the values for an array of given keys.
   *
   * @param keys the keys to be read
   * @return a map of the stored values, keyed by key
   */
  public Map<byte[], byte[]> readAll(byte[][] keys) {
    List<Get> gets = Lists.newArrayListWithCapacity(keys.length);
    for (byte[] key : keys) {
      gets.add(new Get(key).add(KEY_COLUMN));
    }
    List<Row> results = table.get(gets);
    Map<byte[], byte[]> values = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Row row : results) {
      if (row.get(KEY_COLUMN) != null) {
        values.put(row.getRow(), row.get(KEY_COLUMN));
      }
    }
    return values;
  }

  /**
   * Increment the value for a given key and return the resulting value.
   *
   * @param key the key to increment
   * @return the incremented value of that key
   */
  public long incrementAndGet(byte[] key, long value) {
    return this.table.incrementAndGet(key, KEY_COLUMN, value);
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
   * Increment the value of a key by amount; the key must either not exist yet, or the
   * current value at the key must be 8 bytes long to be interpretable as a long.
   *
   * @param key the key
   * @param amount the amount to increment by
   */
  public void increment(byte[] key, long amount) {
    this.table.increment(key, KEY_COLUMN, amount);
  }

  /**
   * Delete a key.
   *
   * @param key the key to delete
   */
  public void delete(byte[] key) {
    this.table.delete(key, KEY_COLUMN);
  }

  /**
   * Compares-and-swaps (atomically) the value of the specified row and column
   * by looking for the specified expected value and, if found, replacing with
   * the specified new value.
   *
   * @param key key to modify
   * @param oldValue expected value before change
   * @param newValue value to set
   * @return true if compare and swap succeeded, false otherwise (stored value is different from expected)
   */
  public boolean compareAndSwap(byte[] key, byte[] oldValue, byte[] newValue) throws Exception {
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

  /**
  * Returns splits for a range of keys in the table.
  * 
  * @param numSplits Desired number of splits. If greater than zero, at most this many splits will be returned.
  *                  If less than or equal to zero, any number of splits can be returned.
  * @param start if non-null, the returned splits will only cover keys that are greater or equal
  * @param stop if non-null, the returned splits will only cover keys that are less
  * @return list of {@link Split}
  */
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    return table.getSplits(numSplits, start, stop);
  }

  @Override
  public SplitReader<byte[], byte[]> createSplitReader(Split split) {
    return new KeyValueScanner(table.createSplitReader(split));
  }

  @Override
  public void write(KeyValue<byte[], byte[]> keyValue) throws IOException {
    write(keyValue.getKey(), keyValue.getValue());
  }

  /**
   * Scans table.
   * @param startRow start row inclusive. {@code null} means start from first row of the table
   * @param stopRow stop row exclusive. {@code null} means scan all rows to the end of the table
   * @return {@link co.cask.cdap.api.dataset.lib.CloseableIterator} of
   * {@link KeyValue KeyValue&lt;byte[], byte[]&gt;}
   */
  public CloseableIterator<KeyValue<byte[], byte[]>> scan(byte[] startRow, byte[] stopRow) {
    final Scanner scanner = table.scan(startRow, stopRow);

    return new AbstractCloseableIterator<KeyValue<byte[], byte[]>>() {
      private boolean closed = false;
      @Override
      protected KeyValue<byte[], byte[]> computeNext() {
        Preconditions.checkState(!closed);
        Row next = scanner.next();
        if (next != null) {
          return new KeyValue<byte[], byte[]>(next.getRow(), next.get(KEY_COLUMN));
        }
        close();
        return null;
      }

      @Override
      public void close() {
        scanner.close();
        endOfData();
        closed = true;
      }
    };
  }

  /**
   * {@link co.cask.cdap.api.data.batch.Scannables.RecordMaker} for {@link #createSplitReader(Split)}.
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
