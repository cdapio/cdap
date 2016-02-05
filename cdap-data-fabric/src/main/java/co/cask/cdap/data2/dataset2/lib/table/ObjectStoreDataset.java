/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Scannables;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.common.io.BinaryDecoder;
import co.cask.cdap.common.io.BinaryEncoder;
import co.cask.cdap.internal.io.ReflectionDatumReader;
import co.cask.cdap.internal.io.ReflectionDatumWriter;
import co.cask.cdap.internal.io.TypeRepresentation;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Default implementation for {@link ObjectStore}
 * @param <T> the type of objects in the store
 */
@Beta
public class ObjectStoreDataset<T> extends AbstractDataset implements ObjectStore<T> {

  private final KeyValueTable kvTable;
  private final TypeRepresentation typeRep;
  private final Schema schema;

  private final ReflectionDatumWriter<T> datumWriter;
  // we get this lazily, since we may not have the actual Type when simply instantiating this class (for instance, when
  // datasets are instantiated in DatasetSystemMetadataWriter for checking RecordScannable/BatchWritable/etc.),
  // but we do expect to have it when using it in a program context
  private ReflectionDatumReader<T> datumReader;

  public ObjectStoreDataset(String name, KeyValueTable kvTable, TypeRepresentation typeRep,
                            Schema schema, @Nullable ClassLoader classLoader) {
    super(name, kvTable);
    this.kvTable = kvTable;
    this.typeRep = typeRep;
    this.typeRep.setClassLoader(classLoader);
    this.schema = schema;
    this.datumWriter = new ReflectionDatumWriter<>(this.schema);
  }

  public ObjectStoreDataset(String name, KeyValueTable kvTable,
                            TypeRepresentation typeRep, Schema schema) {
    this(name, kvTable, typeRep, schema, null);
  }

  @Override
  public void write(String key, T object) {
    kvTable.write(Bytes.toBytes(key), encode(object));
  }

  @Override
  public void write(byte[] key, T object) {
    kvTable.write(key, encode(object));
  }

  @Override
  public T read(String key) {
    return decode(kvTable.read(Bytes.toBytes(key)));
  }

  @Override
  public CloseableIterator<KeyValue<byte[], T>> scan(byte[] startRow, byte[] stopRow) {
    final CloseableIterator<KeyValue<byte[], byte[]>> keyValueIterator = kvTable.scan(startRow, stopRow);
    return new AbstractCloseableIterator<KeyValue<byte[], T>>() {
      boolean closed = false;
      @Override
      protected KeyValue<byte[], T> computeNext() {
        Preconditions.checkState(!closed);
        if (keyValueIterator.hasNext()) {
          KeyValue<byte[], byte[]> row = keyValueIterator.next();
          return new KeyValue<>(row.getKey(), decode(row.getValue()));
        }
        close();
        return null;
      }

      @Override
      public void close() {
        keyValueIterator.close();
        endOfData();
        closed = true;
      }
    };
  }

  @Override
  public T read(byte[] key) {
    byte[] read = kvTable.read(key);
    return decode(read);
  }

  @Override
  public void delete(byte[] key) {
    kvTable.delete(key);
  }

  private byte[] encode(T object) {
    // encode T using schema
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(bos);
    try {
      this.datumWriter.encode(object, encoder);
    } catch (IOException e) {
      // SHOULD NEVER happen
      throw new DataSetException("Failed to encode object to be written: " + e.getMessage(), e);
    }
    return bos.toByteArray();
  }

  @SuppressWarnings("unchecked")
  private ReflectionDatumReader<T> getReflectionDatumReader() {
    if (datumReader == null) {
      datumReader = new ReflectionDatumReader<>(schema, (TypeToken<T>) TypeToken.of(this.typeRep.toType()));
    }
    return datumReader;
  }

  private T decode(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    // decode T using schema
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    BinaryDecoder decoder = new BinaryDecoder(bis);
    try {
      return getReflectionDatumReader().read(decoder, this.schema);
    } catch (IOException e) {
      // SHOULD NEVER happen
      throw new DataSetException("Failed to decode read object: " + e.getMessage(), e);
    }
  }

  // TODO: it should implement RecordScannable, but due to classloading issues it doesn't
//  @Override
  public RecordScanner<KeyValue<byte[], T>> createSplitRecordScanner(Split split) {
    return Scannables.splitRecordScanner(createSplitReader(split), new ObjectRecordMaker());
  }

  // TODO: it should implement RecordScannable, but due to classloading issues it doesn't
//  @Override
  public Type getRecordType() {
    return typeRep.toType();
  }

  @Override
  public List<Split> getSplits() {
    return kvTable.getSplits();
  }

  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    return kvTable.getSplits(numSplits, start, stop);
  }

  @Override
  public SplitReader<byte[], T> createSplitReader(Split split) {
    return new ObjectScanner(kvTable.createSplitReader(split));
  }

  /**
   * {@link co.cask.cdap.api.data.batch.Scannables.RecordMaker} for {@link ObjectStoreDataset}.
   */
  public class ObjectRecordMaker implements Scannables.RecordMaker<byte[], T, KeyValue<byte[], T>> {
    @Override
    public KeyValue<byte[], T> makeRecord(byte[] key, T value) {
      return new KeyValue<>(key, value);
    }
  }

  /**
   * The split reader for objects is reading a table split using the underlying KeyValueTable's split reader.
   */
  public class ObjectScanner extends SplitReader<byte[], T> {

    // the underlying KeyValueTable's split reader
    private SplitReader<byte[], byte[]> reader;

    public ObjectScanner(SplitReader<byte[], byte[]> reader) {
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
    public T getCurrentValue() throws InterruptedException {
      return decode(this.reader.getCurrentValue());
    }

    @Override
    public void close() {
      this.reader.close();
    }
  }
}
