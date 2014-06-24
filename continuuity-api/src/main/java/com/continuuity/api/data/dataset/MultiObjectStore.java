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
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.data.dataset.table.Table;
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
import java.util.Map;

/**
 * This data set allows to store objects of a particular class into a table. The types that are supported are:
 * <ul>
 *   <li>a plain java class</li>
 *   <li>a parametrized class</li>
 *   <li>a static inner class of one of the above</li>
 * </ul>
 * Interfaces and not-static inner classes are not supported.
 * ObjectStore supports storing one or more objects for the same key. Multiple objects can be stored using different
 * column names for each object. If no column name is specified in read or write operations a default column 'c' will
 * be used.
 *
 * @param <T> the type of objects in the store
 * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.dataset.lib.MultiObjectStore}
 */
@Deprecated
public class MultiObjectStore<T> extends DataSet implements BatchReadable<byte[], Map<byte[], T>>,
                                                            BatchWritable<byte[], Map<byte[], T>> {

  // the default column to use for the key
  protected static final byte[] DEFAULT_COLUMN = { 'c' };

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  // the (write) schema of the objects in the store
  protected Schema schema;
  // representation of the type of the objects in the store. needed for decoding (we need to tell the decoder what
  // type is should return - otherwise it would have to return an avro generic).
  protected TypeRepresentation typeRep;
  // the underlying table that we use to store the objects
  protected Table table;

  // The actual ObjectStore to delegate operations to. The value is injected by the runtime system.
  private Supplier<MultiObjectStore<T>> delegate = new Supplier<MultiObjectStore<T>>() {
    @Override
    public MultiObjectStore<T> get() {
      throw new IllegalStateException("Delegate is not set");
    }
  };

  /**
   * Constructor for an object store from its name and the type of the objects it stores.
   * @param name the name of the data set/object store
   * @param type the type of the objects in the store
   * @throws UnsupportedTypeException if the type cannot be supported
   */
  public MultiObjectStore(String name, Type type) throws UnsupportedTypeException {
    this(name, type, -1);
  }

  /**
   * Constructor for an object store from its name and the type of the objects it stores.
   * @param name the name of the data set/object store
   * @param type the type of the objects in the store
   * @param ttl time to live for the data in ms, negative means unlimited.
   * @throws UnsupportedTypeException if the type cannot be supported
   */
  public MultiObjectStore(String name, Type type, int ttl) throws UnsupportedTypeException {
    super(name);
    this.table = new Table("multiobjects", ttl);
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
   * Delete the object specified with specified key and column.
   * @param key key of the object to be deleted
   * @param column col of the object to be deleted
   */
  public void delete(byte[] key, byte[] column) {
    delegate.get().delete(key, column);
  }

  /**
   * Delete the object in the default column for the specified key.
   * @param key key of the object to be deleted in the default column
   */
  public void delete(byte[] key) {
    delegate.get().delete(key);
  }

  /**
   * Delete the objects across all the columns for the given key.
   * @param key key of the object to be deleted
   */
  public void deleteAll(byte[] key) {
    delegate.get().deleteAll(key);
  }

  /**
   * Read an object with a given key.
   * @param key the key of the object
   * @param col to read
   * @return the object if found, or null if not found
   */
  public T read(byte[] key, byte[] col) {
    return delegate.get().read(key, col);
  }


  /**
   * Read all the objects with the given key.
   * @param key the key of the object
   * @return Map of column key and Object, null if entry for the key doesn't exist
   */
  public Map<byte[], T> readAll(byte[] key) {
    return delegate.get().readAll(key);
  }


  /**
   * Write an object with a given key. Writes the object to the default column 'c'
   * @param key the key of the object
   * @param object the object to be stored
   */
  public void write(byte[] key, T object) {
    delegate.get().write(key, object);
  }

  /**
   * Write an object with a given key and a column.
   * @param key the key of the object.
   * @param col column where the object should be written.
   * @param object object to be stored.
   */
  public void write(byte[] key, byte[] col, T object) {
    delegate.get().write(key, col, object);
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
  public SplitReader<byte[], Map<byte[], T>> createSplitReader(Split split) {
    return delegate.get().createSplitReader(split);
  }

  /**
   * Return a split reader that exposes the raw byte arrays from the underlying key value store.
   * @param split the split
   * @return a byte array split reader
   */
  @Beta
  public SplitReader<byte[], Row> createRawSplitReader(Split split) {
    return delegate.get().createRawSplitReader(split);
  }

  @Override
  public void write(byte[] key, Map<byte[], T> columnValues) {
    delegate.get().write(key, columnValues);
  }
}
