package com.continuuity.data.dataset;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.StatusCode;
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

/**
 * This is the implementation of object store that is injected as the delegate at runtime. It has actual
 * implementations of the data APIs.
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
    return (TypeToken<T>)TypeToken.of(this.typeRep.toType());
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
}
