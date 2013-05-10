package com.continuuity.data.dataset;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.StatusCode;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.ReflectionDatumWriter;
import com.google.common.reflect.TypeToken;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * This is the implementation of object store that is injected as the delegate at runtime. It has actual
 * implementations of the data APIs.
 * @param <T> the type of the objects in the store
 */
public final class ObjectStoreImpl<T> extends ObjectStore<T> {

  private final DatumWriter<T> datumWriter; // to serialize an object
  private final ReflectionDatumReader<T> datumReader; // to deserialize an object

  /**
   * Given an object store, create an implementation and set that as the delegate for the store.
   * @param store the object store
   * @param loader the class loader for T, or null to use the default class loader
   * @param <T> the type of the objects in the store
   */
  static <T> void setImplementation(ObjectStore<T> store, @Nullable ClassLoader loader) {
    ObjectStoreImpl<T> impl = new ObjectStoreImpl<T>(store, loader);
    store.setDelegate(impl);
  }

  /**
   * Given an object store, create an implementation for that store.
   * @param store the object store
   * @param loader the class loader for the object type (it may be a user-defined type requiring its own clas loader).
   *               If null, then the default class loader is used.
   */
  protected ObjectStoreImpl(ObjectStore<T> store, @Nullable ClassLoader loader) {
    super(store);
    this.typeRep.setClassLoader(loader);
    this.datumWriter = new ReflectionDatumWriter<T>(this.schema);
    this.datumReader = new ReflectionDatumReader<T>(this.schema, getTypeToken());
  }

  // this function only exists to reduce the scope of the SuppressWarnings annotation to a single cast.
  @SuppressWarnings("unchecked")
  private TypeToken<T> getTypeToken() {
    return (TypeToken<T>) TypeToken.of(this.typeRep.toType());
  }

  @Override
  public void write(byte[] key, T object) throws OperationException {
    // encode T using schema
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(bos);
    try {
      this.datumWriter.encode(object, encoder);
    } catch (IOException e) {
      throw new OperationException(StatusCode.INCOMPATIBLE_TYPE,
                                   "Failed to encode object to be written: " + e.getMessage(), e);
    }
    // write to key value table
    this.kvTable.write(key, bos.toByteArray());
  }

  @Override
  public T read(byte[] key) throws OperationException {
    byte[] bytes = this.kvTable.read(key);
    return decode(bytes);
  }

  private T decode(byte[] bytes) throws OperationException {
    if (bytes == null) {
      return null;
    }
    // decode T using schema
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    BinaryDecoder decoder = new BinaryDecoder(bis);
    try {
      return this.datumReader.read(decoder, this.schema);
    } catch (IOException e) {
      throw new OperationException(StatusCode.INCOMPATIBLE_TYPE,
                                   "Failed to decode the read object: " + e.getMessage(), e);
    }
  }

  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) throws OperationException {
    return this.kvTable.getSplits(numSplits, start, stop);
  }

  @Override
  public List<Split> getSplits() throws OperationException {
    return this.kvTable.getSplits();
  }

  @Override
  public SplitReader<byte[], T> createSplitReader(Split split) {
    return new ObjectScanner(split);
  }

  public class ObjectScanner extends SplitReader<byte[],T> {

    private SplitReader<byte[], byte[]> reader;

    public ObjectScanner(Split split) {
      this.reader = kvTable.createSplitReader(split);
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
    public T getCurrentValue() throws InterruptedException, OperationException {
      return decode(this.reader.getCurrentValue());
    }

    @Override
    public void close() {
      this.reader.close();
    }
  }
}
