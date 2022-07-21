/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package io.cdap.cdap.api.dataset.lib;

import io.cdap.cdap.api.annotation.ReadOnly;
import io.cdap.cdap.api.annotation.ReadWrite;
import io.cdap.cdap.api.annotation.WriteOnly;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.batch.BatchReadable;
import io.cdap.cdap.api.data.batch.BatchWritable;
import io.cdap.cdap.api.data.batch.RecordScannable;
import io.cdap.cdap.api.data.batch.RecordScanner;
import io.cdap.cdap.api.data.batch.RecordWritable;
import io.cdap.cdap.api.data.batch.Scannables;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.data.batch.SplitReader;
import io.cdap.cdap.api.dataset.table.Get;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.internal.guava.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A key/value map implementation on top of {@link Table} supporting read, write and delete operations.
 */
public class KeyValueTable extends AbstractDataset implements
  BatchReadable<byte[], byte[]>, BatchWritable<byte[], byte[]>,
  RecordScannable<KeyValue<byte[], byte[]>>, RecordWritable<KeyValue<byte[], byte[]>> {

  /**
   * Type name
   */
  public static final String TYPE = "keyValueTable";

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
  @ReadOnly
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
  @ReadOnly
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
  @ReadOnly
  public Map<byte[], byte[]> readAll(byte[][] keys) {
    List<Get> gets = Arrays.stream(keys).map(key -> new Get(key).add(KEY_COLUMN)).collect(Collectors.toList());
    return table.get(gets).stream()
      .filter(row -> row.get(KEY_COLUMN) != null)
      .collect(Collectors.toMap(Row::getRow, row -> row.get(KEY_COLUMN), (v1, v2) -> v2,
                                () -> new TreeMap<>(Bytes.BYTES_COMPARATOR)));
  }

  /**
   * Increment the value for a given key and return the resulting value.
   *
   * @param key the key to increment
   * @return the incremented value of that key
   */
  @ReadWrite
  public long incrementAndGet(byte[] key, long value) {
    return this.table.incrementAndGet(key, KEY_COLUMN, value);
  }

  /**
   * Write a value to a key.
   *
   * @param key the key
   * @param value the new value
   */
  @Override
  @WriteOnly
  public void write(byte[] key, byte[] value) {
    this.table.put(key, KEY_COLUMN, value);
  }

  /**
   * Write a value to a key.
   *
   * @param key the key
   * @param value the new value
   */
  @WriteOnly
  public void write(String key, String value) {
    this.table.put(Bytes.toBytes(key), KEY_COLUMN, Bytes.toBytes(value));
  }

  /**
   * Write a value to a key.
   *
   * @param key the key
   * @param value the new value
   */
  @WriteOnly
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
  @WriteOnly
  public void increment(byte[] key, long amount) {
    this.table.increment(key, KEY_COLUMN, amount);
  }

  /**
   * Delete a key.
   *
   * @param key the key to delete
   */
  @WriteOnly
  public void delete(String key) {
    delete(Bytes.toBytes(key));
  }

  /**
   * Delete a key.
   *
   * @param key the key to delete
   */
  @WriteOnly
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
  @ReadWrite
  public boolean compareAndSwap(byte[] key, byte[] oldValue, byte[] newValue) {
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

  @ReadOnly
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
  public List<Split> getSplits(int numSplits, @Nullable byte[] start, @Nullable byte[] stop) {
    return table.getSplits(numSplits, start, stop);
  }

  @ReadOnly
  @Override
  public SplitReader<byte[], byte[]> createSplitReader(Split split) {
    return new KeyValueScanner(table.createSplitReader(split));
  }

  @WriteOnly
  @Override
  public void write(KeyValue<byte[], byte[]> keyValue) throws IOException {
    write(keyValue.getKey(), keyValue.getValue());
  }

  /**
   * Scans table.
   * @param startRow start row inclusive. {@code null} means start from first row of the table
   * @param stopRow stop row exclusive. {@code null} means scan all rows to the end of the table
   * @return {@link io.cdap.cdap.api.dataset.lib.CloseableIterator} of
   * {@link KeyValue KeyValue&lt;byte[], byte[]&gt;}
   */
  public CloseableIterator<KeyValue<byte[], byte[]>> scan(byte[] startRow, byte[] stopRow) {
    final Scanner scanner = table.scan(startRow, stopRow);

    return new AbstractCloseableIterator<KeyValue<byte[], byte[]>>() {
      private boolean closed;
      @Override
      protected KeyValue<byte[], byte[]> computeNext() {
        if (closed) {
          return endOfData();
        }
        Row next = scanner.next();
        if (next != null) {
          return new KeyValue<>(next.getRow(), next.get(KEY_COLUMN));
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
   * {@link io.cdap.cdap.api.data.batch.Scannables.RecordMaker} for {@link #createSplitReader(Split)}.
   */
  public class KeyValueRecordMaker implements Scannables.RecordMaker<byte[], byte[], KeyValue<byte[], byte[]>> {
    @Override
    public KeyValue<byte[], byte[]> makeRecord(byte[] key, byte[] value) {
      return new KeyValue<>(key, value);
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
