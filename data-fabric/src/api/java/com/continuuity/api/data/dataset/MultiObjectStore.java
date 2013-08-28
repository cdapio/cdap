package com.continuuity.api.data.dataset;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaTypeAdapter;
import com.continuuity.internal.io.TypeRepresentation;
import com.continuuity.internal.io.UnsupportedTypeException;
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
 * @param <T> the type of objects in the store
 */
@Beta
public class MultiObjectStore<T> extends DataSet implements BatchReadable<byte[], Map<byte[], T>>,
                                                       BatchWritable<byte[], Map<byte[], T>> {

  // the (write) schema of the objects in the store
  protected final Schema schema;
  // representation of the type of the objects in the store. needed for decoding (we need to tell the decoder what
  // type is should return - otherwise it would have to return an avro generic).
  protected final TypeRepresentation typeRep;
  // the underlying table that we use to store the objects
  protected final Table table;

  // the default column to use for the key
  protected final byte[] DEFAULT_COLUMN = { 'c' };

   // this is the dataset that executes the actual operations. using a delegate
   // allows us to inject a different implementation.
   private MultiObjectStore<T> delegate = null;

  /**
   * sets the ObjectStore to which all operations are delegated. This can be used
   * to inject different implementations.
   * @param store the implementation to delegate to
   */
  public void setDelegate(MultiObjectStore<T> store) {
    this.delegate = store;
  }

  /**
   * Constructor for an object store from its name and the type of the objects it stores.
   * @param name the name of the data set/object store
   * @param type the type of the objects in the store
   * @throws UnsupportedTypeException if the type cannot be supported
   */
  public MultiObjectStore(String name, Type type) throws UnsupportedTypeException {
    super(name);
    this.table = new Table(name + "_kv");
    this.schema = new ReflectionSchemaGenerator().generate(type);
    this.typeRep = new TypeRepresentation(type);
  }

  /**
   * Constructor that takes in an existing key/value store. This can be called by subclasses to inject a different
   * key/value store implementation.
   * @param name the name of the data set/object store
   * @param type the type of the objects in the store
   * @param table existing table
   * @throws UnsupportedTypeException if the type cannot be supported
   */
  public MultiObjectStore(String name, Type type, Table table) throws UnsupportedTypeException {
    super(name);
    this.table = table;
    this.schema = new ReflectionSchemaGenerator().generate(type);
    this.typeRep = new TypeRepresentation(type);
  }

  /**
   * Constructor from a data set specification.
   * @param spec the specification
   */
  public MultiObjectStore(DataSetSpecification spec) {
    super(spec);
    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .create();
    this.schema = gson.fromJson(spec.getProperty("schema"), Schema.class);
    this.typeRep = gson.fromJson(spec.getProperty("type"), TypeRepresentation.class);
    this.table = new Table(spec.getSpecificationFor(this.getName() + "_kv"));
  }

  /**
   * Constructor from specification that also takes in an existing key/value store. This can be called by subclasses
   * to injecta different
   * key/value store implementation.
   * @param spec the data set specification
   * @param table existing table
   */
  protected MultiObjectStore(DataSetSpecification spec, Table table) {
    super(spec);
    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .create();
    this.schema = gson.fromJson(spec.getProperty("schema"), Schema.class);
    this.typeRep = gson.fromJson(spec.getProperty("type"), TypeRepresentation.class);
    this.table = table;
  }

  @Override
  public DataSetSpecification configure() {
    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .create();
    return new DataSetSpecification.Builder(this)
      .property("schema", gson.toJson(this.schema))
      .property("type", gson.toJson(this.typeRep))
      .dataset(this.table.configure())
      .create();
  }

  /**
   * Constructor from another object store. Should only be called from the constructor
   * of implementing sub classes.
   * @param store the other object store
   */
  protected MultiObjectStore(MultiObjectStore<T> store) {
    super(store.getName());
    this.schema = store.schema;
    this.typeRep = store.typeRep;
    this.table = store.table;
  }

  /**
   * Read an object with a given key.
   * @param key the key of the object
   * @return the object if found, or null if not found
   * @throws OperationException in case of errors
   */
  public T read(byte[] key) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.read(key);
  }

  /**
   * Delete the object specified with specified key and column.
   * @param key key of the object to be deleted
   * @param column col of the object to be deleted
   * @throws OperationException in case of errors
   */
  public void delete(byte[] key, byte[] column) throws OperationException{
    if (null == this.delegate){
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    this.delegate.delete(key, column);
  }

  /**
   * Delete the object in the default column for the specified key.
   * @param key key of the object to be deleted in the default column
   * @throws OperationException in case of errors
   */
  public void delete(byte[] key) throws OperationException{
    if (null == this.delegate){
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    this.delegate.delete(key);
  }

  /**
   * Delete the objects across all the columns for the given key.
   * @param key key of the object to be deleted
   * @throws OperationException
   */
  public void deleteAll(byte[] key) throws OperationException{
    if (null == this.delegate){
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    this.delegate.deleteAll(key);
  }

  /**
   * Read an object with a given key.
   * @param key the key of the object
   * @param col to read
   * @return the object if found, or null if not found
   * @throws OperationException in case of errors
   */
  public T read(byte[] key, byte[] col) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.read(key, col);
  }


  /**
   * Read all the objects with the given key.
   * @param key the key of the object
   * @return Map of column key and Object, null if entry for the key doesn't exist
   * @throws OperationException incase of errors.
   */
  public Map<byte[], T> readAll(byte[] key) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.readAll(key);
  }


  /**
   * Write an object with a given key. Writes the object to the default column 'c'
   * @param key the key of the object
   * @param object the object to be stored
   * @throws OperationException in case of errors
   */
  public void write(byte[] key, T object) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    this.delegate.write(key, object);
  }

  /**
   * Write an object with a given key and a column.
   * @param key the key of the object.
   * @param col column where the object should be written.
   * @param object object to be stored.
   * @throws OperationException incase of errors.
   */
  public void write(byte[] key, byte[] col, T object) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    this.delegate.write(key, col, object);
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
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.getSplits(numSplits, start, stop);
  }

  @Override
  public List<Split> getSplits() throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.getSplits();
  }

  @Override
  public SplitReader<byte[], Map<byte[], T>> createSplitReader(Split split) {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    return this.delegate.createSplitReader(split);
  }

  /**
   * Return a split reader that exposes the raw byte arrays from the underlying key value store.
   * @param split the split
   * @return a byte array split reader
   */
  @Beta
  public SplitReader<byte[], Map<byte[], byte[]>> createRawSplitReader(Split split) {
    return this.table.createSplitReader(split);
  }

  @Override
  public void write(byte[] key, Map<byte[], T> columnValues) throws OperationException {
    if (null == this.delegate) {
      throw new IllegalStateException("Not supposed to call runtime methods at configuration time.");
    }
    this.delegate.write(key, columnValues);
  }
}
