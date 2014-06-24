package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.batch.RecordScanner;
import com.continuuity.api.data.batch.Scannables;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.DataSetException;
import com.continuuity.api.dataset.lib.AbstractDataset;
import com.continuuity.api.dataset.lib.KeyValue;
import com.continuuity.api.dataset.lib.MultiObjectStore;
import com.continuuity.api.dataset.table.Put;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.ReflectionDatumWriter;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.TypeRepresentation;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link MultiObjectStore}
 * @param <T> the type of objects in the store
 */
@Beta
public class MultiObjectStoreDataset<T> extends AbstractDataset implements MultiObjectStore<T> {

  // the default column to use for the key
  protected static final byte[] DEFAULT_COLUMN = { 'c' };

  // the (write) schema of the objects in the store
  private final Schema schema;
  // representation of the type of the objects in the store. needed for decoding (we need to tell the decoder what
  // type is should return - otherwise it would have to return an avro generic).
  private final TypeRepresentation typeRep;
  // the underlying table that we use to store the objects
  private final Table table;

  private final ReflectionDatumWriter<T> datumWriter;
  private final ReflectionDatumReader<T> datumReader;

  /**
   * Constructor for an object store from its name and the type of the objects it stores.
   * @param name the name of the data set/object store
   * @param typeRep the type of the objects in the store
   */
  public MultiObjectStoreDataset(String name, Table table, TypeRepresentation typeRep,
                                 Schema schema, @Nullable ClassLoader classLoader) {
    super(name, table);
    this.table = table;
    this.typeRep = typeRep;
    this.typeRep.setClassLoader(classLoader);
    this.schema = schema;
    this.datumWriter = new ReflectionDatumWriter<T>(this.schema);
    this.datumReader = new ReflectionDatumReader<T>(this.schema, getTypeToken());
  }

  public MultiObjectStoreDataset(String name, Table table, TypeRepresentation typeRep, Schema schema) {
    this(name, table, typeRep, schema, null);
  }

  // this function only exists to reduce the scope of the SuppressWarnings annotation to a single cast.
  @SuppressWarnings("unchecked")
  private TypeToken<T> getTypeToken() {
    return (TypeToken<T>) TypeToken.of(this.typeRep.toType());
  }

  @Override
  public void write(byte[] key, T object) {
    table.put(key, DEFAULT_COLUMN, encode(object));
  }

  @Override
  public void write(String key, T object) {
    table.put(Bytes.toBytes(key), DEFAULT_COLUMN, encode(object));
  }

  @Override
  public void write(byte[] key, byte[] col, T object) {
    table.put(key, col, encode(object));
  }

  @Override
  public void write(byte[] key, Map<byte[], T> columnValues) {
    Put put = new Put(key);
    for (Map.Entry<byte[], T> entry : columnValues.entrySet()) {
      put.add(entry.getKey(), encode(entry.getValue()));
    }
    table.put(put);
  }

  @Override
  public T read(byte[] key) {
    byte[] bytes = table.get(key, DEFAULT_COLUMN);
    return decode(bytes);
  }

  @Override
  public T read(byte[] key, byte[] col) {
    byte[] bytes = table.get(key, col);
    return decode(bytes);
  }

  @Override
  public void delete(byte[] key, byte[] col) {
    table.delete(key, col);
  }

  @Override
  public void delete(byte[] key) {
    table.delete(key, DEFAULT_COLUMN);
  }

  @Override
  public void deleteAll(byte[] key) {
    table.delete(key);
  }

  @Override
  public Map<byte[], T> readAll(byte[] key) {
    Row row = table.get(key);
    return Maps.transformValues(row.getColumns(), new Function<byte[], T>() {
      @Nullable
      @Override
      public T apply(@Nullable byte[] input) {
        return decode(input);
      }
    });
  }

  // TODO: duplicate code in RuntimeObjectStore
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

  // TODO: duplicate code in RuntimeObjectStore
  private T decode(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    // decode T using schema
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    BinaryDecoder decoder = new BinaryDecoder(bis);
    try {
      return this.datumReader.read(decoder, this.schema);
    } catch (IOException e) {
      // SHOULD NEVER happen
      throw new DataSetException("Failed to decode read object: " + e.getMessage(), e);
    }
  }

  @Override
  @Beta
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    return table.getSplits(numSplits, start, stop);
  }

  // TODO: it should implement RecordScannable, but due to classloading issues it doesn't
//  @Override
  public Type getRecordType() {
    return new TypeToken<KeyValue<byte[], Map<byte[], T>>>() { }.getType();
  }

  @Override
  public List<Split> getSplits() {
    return table.getSplits();
  }

  // TODO: it should implement RecordScannable, but due to classloading issues it doesn't
//  @Override
  public RecordScanner<KeyValue<byte[], Map<byte[], T>>> createSplitRecordScanner(Split split) {
    return Scannables.splitRecordScanner(createSplitReader(split), new MultiObjectRecordMaker());
  }

  @Override
  public SplitReader<byte[], Map<byte[], T>> createSplitReader(Split split) {
    return new MultiObjectScanner(table.createSplitReader(split));
  }

  /**
   * {@link com.continuuity.api.data.batch.Scannables.RecordMaker} for {@link MultiObjectStoreDataset}.
   */
  public class MultiObjectRecordMaker
    implements Scannables.RecordMaker<byte[], Map<byte[], T>, KeyValue<byte[], Map<byte[], T>>> {

    @Override
    public KeyValue<byte[], Map<byte[], T>> makeRecord(byte[] key, Map<byte[], T> value) {
      return new KeyValue<byte[], Map<byte[], T>>(key, value);
    }
  }

  /**
   * The split reader for objects is reading a table split using the underlying Table's split reader.
   */
  public class MultiObjectScanner extends SplitReader<byte[], Map<byte[], T>> {

    // the underlying KeyValueTable's split reader
    private SplitReader<byte[], Row> reader;

    public MultiObjectScanner(SplitReader<byte[], Row> reader) {
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
    public Map<byte[], T> getCurrentValue() throws InterruptedException {
      // get the current value as a byte array and decode it into an object of type T
      Map<byte[], T> columnValues = Maps.newTreeMap(new Bytes.ByteArrayComparator());
      for (Map.Entry<byte[], byte[]> entry : this.reader.getCurrentValue().getColumns().entrySet()) {
        columnValues.put(entry.getKey(), decode(entry.getValue()));
      }
      return columnValues;
    }

    @Override
    public void close() {
      this.reader.close();
    }
  }
}
