/*
 * Copyright 2012-2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.data.dataset;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaTypeAdapter;
import com.continuuity.internal.io.TypeRepresentation;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.base.Supplier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.reflect.Type;
import java.util.List;

/**
 * This data set allows to store objects of a particular class into a table. The types that are supported are:
 * <ul>
 *   <li>a plain java class</li>
 *   <li>a parametrized class</li>
 *   <li>a static inner class of one of the above</li>
 * </ul>
 * Interfaces and not-static inner classes are not supported.
 * @param <T> the type of objects in the store
 *
 * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.dataset.lib.ObjectStore}
 */
@Deprecated
public class ObjectStore<T> extends DataSet implements BatchReadable<byte[], T>, BatchWritable<byte[], T> {

  private static final Gson GSON = new GsonBuilder()
  .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  // the (write) schema of the objects in the store
  protected Schema schema;
  // representation of the type of the objects in the store. needed for decoding (we need to tell the decoder what
  // type is should return - otherwise it would have to return an avro generic).
  protected TypeRepresentation typeRep;
  // the underlying key/value table that we use to store the objects
  protected KeyValueTable kvTable;

  // The actual ObjectStore to delegate operations to. The value is injected by the runtime system.
  private Supplier<ObjectStore<T>> delegate = new Supplier<ObjectStore<T>>() {
    @Override
    public ObjectStore<T> get() {
      throw new IllegalStateException("Delegate is not set");
    }
  };


  /**
   * Constructor for an object store from its name and the type of the objects it stores.
   * @param name the name of the data set/object store
   * @param type the type of the objects in the store
   * @throws UnsupportedTypeException if the type cannot be supported
   */
  public ObjectStore(String name, Type type) throws UnsupportedTypeException {
    this(name, type, -1);
  }

  /**
   * Constructor for an object store from its name and the type of the objects it stores.
   * @param name the name of the data set/object store
   * @param type the type of the objects in the store
   * @param ttl time to live for the data in ms, negative means unlimited.
   * @throws UnsupportedTypeException if the type cannot be supported
   */
  public ObjectStore(String name, Type type, int ttl) throws UnsupportedTypeException {
    super(name);
    this.kvTable = new KeyValueTable("objects", ttl);
    this.schema = new ReflectionSchemaGenerator().generate(type);
    this.typeRep = new TypeRepresentation(type);
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
      .property("schema", GSON.toJson(this.schema))
      .property("type", GSON.toJson(this.typeRep))
      .create();
  }

  @Override
  public void initialize(DataSetSpecification spec, DataSetContext context) {
    super.initialize(spec, context);
    this.schema = GSON.fromJson(spec.getProperty("schema"), Schema.class);
    this.typeRep = GSON.fromJson(spec.getProperty("type"), TypeRepresentation.class);
  }

  /**
   * Read an object with a given key.
   * @param key the key of the object
   * @return the object if found, or null if not found
   */
  public T read(byte[] key) {
    return delegate.get().read(key);
  }

  /**
   * Write an object with a given key.
   *
   * @param key the key of the object
   * @param object the object to be stored
   */
  public void write(byte[] key, T object) {
    delegate.get().write(key, object);
  }

  /**
   * Returns splits for a range of keys in the table.
   * @param numSplits Desired number of splits. If greater than zero, at most this many splits will be returned.
   *                  If less or equal to zero, any number of splits can be returned.
   * @param start If non-null, the returned splits will only cover keys that are greater or equal.
   * @param stop If non-null, the returned splits will only cover keys that are less.
   * @return list of {@link Split}
   */
  @Beta
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    return delegate.get().getSplits(numSplits, start, stop);
  }

  @Override
  public List<Split> getSplits() {
    return delegate.get().getSplits();
  }

  @Override
  public SplitReader<byte[], T> createSplitReader(Split split) {
    return delegate.get().createSplitReader(split);
  }

  /**
   * Return a split reader that exposes the raw byte arrays from the underlying key value store.
   * @param split the split
   * @return a byte array split reader
   */
  @Beta
  public SplitReader<byte[], byte[]> createRawSplitReader(Split split) {
    return delegate.get().createRawSplitReader(split);
  }
}
