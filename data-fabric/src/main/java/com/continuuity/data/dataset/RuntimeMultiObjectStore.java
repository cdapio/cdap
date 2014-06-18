package com.continuuity.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.DataSetException;
import com.continuuity.api.data.dataset.MultiObjectStore;
import com.continuuity.api.data.dataset.table.Put;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.ReflectionDatumWriter;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This is the implementation of object store that is injected as the delegate at runtime. It has actual
 * implementations of the data APIs.
 * @param <T> the type of the objects in the store
 */
public final class RuntimeMultiObjectStore<T> extends MultiObjectStore<T> {

  private final ClassLoader classLoader;
  private DatumWriter<T> datumWriter; // to serialize an object
  private ReflectionDatumReader<T> datumReader; // to deserialize an object

  public static <T> RuntimeMultiObjectStore<T> create(@Nullable ClassLoader loader) {
    try {
      return new RuntimeMultiObjectStore<T>(loader);
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a Runtime MultiObjectStore.
   * @param loader the class loader for the object type (it may be a user-defined type requiring its own clas loader).
   *               If null, then the default class loader is used.
   */
  private RuntimeMultiObjectStore(@Nullable ClassLoader loader) throws UnsupportedTypeException {
    // Doesn't really matter what get passed as initialize would overwrite them.
    super("", int.class);
    this.classLoader = loader;
  }

  @Override
  public void initialize(DataSetSpecification spec, DataSetContext context) {
    super.initialize(spec, context);
    this.typeRep.setClassLoader(classLoader);
    this.datumWriter = new ReflectionDatumWriter<T>(this.schema);
    this.datumReader = new ReflectionDatumReader<T>(this.schema, getTypeToken());
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

  // todo: duplicate code in RuntimeObjectStore
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

  // todo: duplicate code in RuntimeObjectStore
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

  /**
   * Returns splits for a range of keys in the table.
   * @param numSplits Desired number of splits. If greater than zero, at most this many splits will be returned.
   *                  If less or equal to zero, any number of splits can be returned.
   * @param start If non-null, the returned splits will only cover keys that are greater or equal.
   * @param stop If non-null, the returned splits will only cover keys that are less.
   * @return list of {@link Split}
   */
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    return table.getSplits(numSplits, start, stop);
  }

  @Override
  public List<Split> getSplits() {
    return table.getSplits();
  }

  @Override
  public SplitReader<byte[], Map<byte[], T>> createSplitReader(Split split) {
    return new ObjectScanner(split);
  }

  /**
   * The split reader for objects is reading a table split using the underlying Table's split reader.
   */
  public class ObjectScanner extends SplitReader<byte[], Map<byte[], T>> {

    // the underlying KeyValueTable's split reader
    private SplitReader<byte[], Row> reader;

    public ObjectScanner(Split split) {
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
