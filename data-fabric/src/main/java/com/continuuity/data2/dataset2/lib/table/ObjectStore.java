package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.RowScannable;
import com.continuuity.api.data.batch.Scannables;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.data.batch.SplitRowScanner;
import com.continuuity.api.data.dataset.DataSetException;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data2.dataset2.lib.AbstractDataset;
import com.continuuity.internal.io.ReflectionDatumReader;
import com.continuuity.internal.io.ReflectionDatumWriter;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.TypeRepresentation;
import com.continuuity.internal.io.codec.BinaryDecoder;
import com.continuuity.internal.io.codec.BinaryEncoder;
import com.google.common.reflect.TypeToken;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import javax.annotation.Nullable;

/**
 * This data set allows to store objects of a particular class into a table. The types that are supported are:
 * <ul>
 *   <li>a plain java class</li>
 *   <li>a parametrized class</li>
 *   <li>a static inner class of one of the above</li>
 * </ul>
 * Interfaces and not-static inner classes are not supported.
 * @param <T> the type of objects in the store
 */
@Beta
public class ObjectStore<T> extends AbstractDataset
  implements BatchReadable<byte[], T>, RowScannable<ImmutablePair<byte[], T>> {

  private final KeyValueTable kvTable;
  private final TypeRepresentation typeRep;
  private final Schema schema;

  private final ReflectionDatumWriter<T> datumWriter;
  private final ReflectionDatumReader<T> datumReader;

  public ObjectStore(String name, KeyValueTable kvTable, TypeRepresentation typeRep,
                     Schema schema, @Nullable ClassLoader classLoader) {
    super(name, kvTable);
    this.kvTable = kvTable;
    this.typeRep = typeRep;
    this.typeRep.setClassLoader(classLoader);
    this.schema = schema;
    this.datumWriter = new ReflectionDatumWriter<T>(this.schema);
    this.datumReader = new ReflectionDatumReader<T>(this.schema, getTypeToken());
  }

  public ObjectStore(String name, KeyValueTable kvTable,
                     TypeRepresentation typeRep, Schema schema) {
    this(name, kvTable, typeRep, schema, null);
  }

  public void write(String key, T object) throws Exception {
    kvTable.write(Bytes.toBytes(key), encode(object));
  }

  public void write(byte[] key, T object) throws Exception {
    kvTable.write(key, encode(object));
  }

  public T read(String key) throws Exception {
    return decode(kvTable.read(Bytes.toBytes(key)));
  }

  public T read(byte[] key) throws Exception {
    byte[] read = kvTable.read(key);
    return decode(read);
  }

  // this function only exists to reduce the scope of the SuppressWarnings annotation to a single cast.
  @SuppressWarnings("unchecked")
  private TypeToken<T> getTypeToken() {
    return (TypeToken<T>) TypeToken.of(this.typeRep.toType());
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

  @Override
  public SplitRowScanner<ImmutablePair<byte[], T>> createSplitScanner(Split split) {
    return Scannables.splitRowScanner(createSplitReader(split), new ObjectRowMaker());
  }

  @Override
  public Type getRowType() {
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
   * {@link com.continuuity.api.data.batch.Scannables.RowMaker} for {@link ObjectStore}.
   */
  public class ObjectRowMaker implements Scannables.RowMaker<byte[], T, ImmutablePair<byte[], T>> {
    @Override
    public ImmutablePair<byte[], T> makeRow(byte[] key, T value) {
      return new ImmutablePair<byte[], T>(key, value);
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
