package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.DataSetException;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.ReflectionDatumWriter;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * This is the implementation of object store that is injected as the delegate at runtime. It has actual
 * implementations of the data APIs.
 * @param <T> the type of the objects in the store
 */
public final class RuntimeObjectStore<T> extends ObjectStore<T> {

  private final ClassLoader classLoader;
  private DatumWriter<T> datumWriter; // to serialize an object
  private ReflectionDatumReader<T> datumReader; // to deserialize an object

  public static <T> RuntimeObjectStore<T> create(@Nullable ClassLoader loader) {
    try {
      return new RuntimeObjectStore<T>(loader);
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates runtime instance of ObjectStore.
   * @param loader the class loader for the object type (it may be a user-defined type requiring its own class loader).
   *               If null, then the default class loader is used.
   */
  private RuntimeObjectStore(@Nullable ClassLoader loader) throws UnsupportedTypeException {
    // Doesn't really matter what get passed, as the initialize would override.
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
    // write to key value table
    kvTable.write(key, encode(object));
  }

  @Override
  public T read(byte[] key) {
    byte[] bytes = kvTable.read(key);
    return decode(bytes);
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

  // Batch support
  
  /**
   * Returns splits for a range of keys in the table.
   * @param numSplits Desired number of splits. If greater than zero, at most this many splits will be returned.
   *                  If less or equal to zero, any number of splits can be returned.
   * @param start If non-null, the returned splits will only cover keys that are greater or equal.
   * @param stop If non-null, the returned splits will only cover keys that are less.
   * @return list of {@link Split}
   */
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    return kvTable.getSplits(numSplits, start, stop);
  }

  @Override
  public List<Split> getSplits() {
    return kvTable.getSplits();
  }

  @Override
  public SplitReader<byte[], T> createSplitReader(Split split) {
    return new ObjectScanner(split);
  }

  /**
   * The split reader for objects is reading a table split using the underlying KeyValuyeTable's split reader.
   */
  public class ObjectScanner extends SplitReader<byte[], T> {

    // the underlying KeyValueTable's split reader
    private SplitReader<byte[], byte[]> reader;

    public ObjectScanner(Split split) {
      this.reader = kvTable.createSplitReader(split);
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
      // get the current value as a byte array and decode it into an object of type T
      return decode(this.reader.getCurrentValue());
    }

    @Override
    public void close() {
      this.reader.close();
    }
  }
}
