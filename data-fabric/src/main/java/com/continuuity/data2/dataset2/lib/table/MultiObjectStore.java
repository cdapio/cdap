package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.RowScannable;
import com.continuuity.api.data.batch.Scannables;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.batch.SplitRowScanner;
import com.continuuity.api.data.dataset.DataSetException;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data2.dataset2.lib.AbstractDataset;
import com.continuuity.internal.data.dataset.lib.table.Put;
import com.continuuity.internal.data.dataset.lib.table.Row;
import com.continuuity.internal.data.dataset.lib.table.Table;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.ReflectionDatumWriter;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaTypeAdapter;
import com.continuuity.internal.io.TypeRepresentation;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

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
public class MultiObjectStore<T> extends AbstractDataset
  implements BatchReadable<byte[], Map<byte[], T>>,
    BatchWritable<byte[], Map<byte[], T>>,
    RowScannable<ImmutablePair<byte[], Map<byte[], T>>> {

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
  public MultiObjectStore(String name, Table table, TypeRepresentation typeRep,
                     Schema schema, @Nullable ClassLoader classLoader) {
    super(name, table);
    this.table = table;
    this.typeRep = typeRep;
    this.typeRep.setClassLoader(classLoader);
    this.schema = schema;
    this.datumWriter = new ReflectionDatumWriter<T>(this.schema);
    this.datumReader = new ReflectionDatumReader<T>(this.schema, getTypeToken());
  }

  public MultiObjectStore(String name, Table table, TypeRepresentation typeRep, Schema schema) {
    this(name, table, typeRep, schema, null);
  }

  // this function only exists to reduce the scope of the SuppressWarnings annotation to a single cast.
  @SuppressWarnings("unchecked")
  private TypeToken<T> getTypeToken() {
    return (TypeToken<T>) TypeToken.of(this.typeRep.toType());
  }

  /**
   * Write an object with a given key. Writes the object to the default column 'c'
   * @param key the key of the object
   * @param object the object to be stored
   */
  public void write(byte[] key, T object) {
    table.put(key, DEFAULT_COLUMN, encode(object));
  }

  public void write(String key, T object) throws Exception {
    table.put(Bytes.toBytes(key), DEFAULT_COLUMN, encode(object));
  }

  /**
   * Write an object with a given key and a column.
   * @param key the key of the object.
   * @param col column where the object should be written.
   * @param object object to be stored.
   */
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

  /**
   * Read an object with a given key.
   * @param key the key of the object
   * @return the object if found, or null if not found
   */
  public T read(byte[] key) {
    byte[] bytes = table.get(key, DEFAULT_COLUMN);
    return decode(bytes);
  }

  /**
   * Read an object with a given key.
   * @param key the key of the object
   * @param col to read
   * @return the object if found, or null if not found
   */
  public T read(byte[] key, byte[] col) {
    byte[] bytes = table.get(key, col);
    return decode(bytes);
  }

  /**
   * Delete the object specified with specified key and column.
   * @param key key of the object to be deleted
   * @param col col of the object to be deleted
   */
  public void delete(byte[] key, byte[] col) {
    table.delete(key, col);
  }

  /**
   * Delete the object in the default column for the specified key.
   * @param key key of the object to be deleted in the default column
   */
  public void delete(byte[] key) {
    table.delete(key, DEFAULT_COLUMN);
  }

  /**
   * Delete the objects across all the columns for the given key.
   * @param key key of the object to be deleted
   */
  public void deleteAll(byte[] key) {
    table.delete(key);
  }

  /**
   * Read all the objects with the given key.
   * @param key the key of the object
   * @return Map of column key and Object, null if entry for the key doesn't exist
   */
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

  /**
   * Returns splits for a range of keys in the table.
   * @param numSplits Desired number of splits. If greater than zero, at most this many splits will be returned.
   *                  If less or equal to zero, any number of splits can be returned.
   * @param start If non-null, the returned splits will only cover keys that are greater or equal.
   * @param stop If non-null, the returned splits will only cover keys that are less.
   * @return list of {@link com.continuuity.api.data.batch.Split}
   */
  @Beta
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    return table.getSplits(numSplits, start, stop);
  }

  @Override
  public Type getRowType() {
    return new TypeToken<ImmutablePair<byte[], Map<byte[], T>>>() { }.getType();
  }

  @Override
  public List<Split> getSplits() {
    return table.getSplits();
  }

  @Override
  public SplitRowScanner<ImmutablePair<byte[], Map<byte[], T>>> createSplitScanner(Split split) {
    return Scannables.splitRowScanner(createSplitReader(split), new MultiObjectRowMaker());
  }

  @Override
  public SplitReader<byte[], Map<byte[], T>> createSplitReader(Split split) {
    return new MultiObjectScanner(table.createSplitReader(split));
  }

  /**
   * {@link com.continuuity.api.data.batch.Scannables.RowMaker} for {@link MultiObjectStore}.
   */
  public class MultiObjectRowMaker
    implements Scannables.RowMaker<byte[], Map<byte[], T>, ImmutablePair<byte[], Map<byte[], T>>> {

    @Override
    public ImmutablePair<byte[], Map<byte[], T>> makeRow(byte[] key, Map<byte[], T> value) {
      return new ImmutablePair<byte[], Map<byte[], T>>(key, value);
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
