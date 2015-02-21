/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.internal.io.ReflectionPutWriter;
import co.cask.cdap.internal.io.ReflectionRowReader;
import co.cask.cdap.internal.io.ReflectionRowRecordReader;
import co.cask.cdap.internal.io.TypeRepresentation;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Default implementation for {@link ObjectMappedTable}.
 *
 * @param <T> the type of objects in the table
 */
@Beta
public class ObjectMappedTableDataset<T> extends AbstractDataset implements ObjectMappedTable<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectMappedTableDataset.class);

  private final Table table;
  private final Schema objectSchema;
  private final Schema tableSchema;
  private final String keyName;
  private final Schema.Type keyType;
  private final TypeRepresentation typeRepresentation;
  private final ReflectionPutWriter<T> putWriter;
  // we get this lazily, since we may not have the actual Type when using this as a RecordScannable,
  // but we do expect to have it when using it in a program context
  private ReflectionRowReader<T> rowReader;

  // schema is passed in as an argument because it is a required dataset property for validation purposes, so
  // the ObjectMappedTableDefinition will always have it. We could always derive the schema from the type,
  // but it is simpler to just pass it in.
  public ObjectMappedTableDataset(String name, Table table, TypeRepresentation typeRep,
                                  Schema objectSchema, String keyName, Schema.Type keyType,
                                  @Nullable ClassLoader classLoader) {
    super(name, table);
    Preconditions.checkArgument(keyType == Schema.Type.STRING || keyType == Schema.Type.BYTES,
                                "Key type must be string or bytes.");
    this.table = table;
    this.objectSchema = objectSchema;
    // table schema is used for exploration and contains row key plus object schema. Object schema used for read/write.
    this.tableSchema  = getTableSchema(objectSchema, keyName, keyType);
    this.keyName = keyName;
    this.keyType = keyType;
    this.typeRepresentation = typeRep;
    this.typeRepresentation.setClassLoader(classLoader);
    this.putWriter = new ReflectionPutWriter<T>(objectSchema);
  }

  @SuppressWarnings("unchecked")
  private ReflectionRowReader<T> getReflectionRowReader() {
    if (rowReader == null) {
      try {
        // this can throw a runtime exception from a ClassNotFoundException
        Type type = typeRepresentation.toType();
        this.rowReader = new ReflectionRowReader<T>(objectSchema, (TypeToken<T>) TypeToken.of(type));
      } catch (RuntimeException e) {
        String missingClass = isClassNotFoundException(e);
        if (missingClass != null) {
          LOG.error("Cannot load dataset because class {} could not be found. This is probably because the " +
                      "type parameter of the dataset is not present in the dataset's jar file. See the developer " +
                      "guide for more information.", missingClass);
        }
        throw e;
      }
    }
    return this.rowReader;
  }

  private String isClassNotFoundException(Throwable e) {
    if (e instanceof ClassNotFoundException) {
      return e.getMessage();
    }
    if (e.getCause() != null) {
      return isClassNotFoundException(e.getCause());
    }
    return null;
  }

  @Override
  public void write(String key, T object) {
    write(Bytes.toBytes(key), object);
  }

  @Override
  public void write(byte[] key, T object) {
    Put put = new Put(key);
    try {
      putWriter.write(object, put);
      table.put(put);
    } catch (IOException e) {
      // should never happen
      throw new DataSetException("Failed to encode object to be written: " + e.getMessage(), e);
    }
  }

  @Override
  public T read(String key) {
    return read(Bytes.toBytes(key));
  }

  @Override
  public T read(byte[] key) {
    return readRow(table.get(key));
  }

  @Override
  public CloseableIterator<KeyValue<byte[], T>> scan(@Nullable String startRow, @Nullable String stopRow) {
    return scan(startRow == null ? null : Bytes.toBytes(startRow), stopRow == null ? null : Bytes.toBytes(stopRow));
  }

  @Override
  public CloseableIterator<KeyValue<byte[], T>> scan(byte[] startRow, byte[] stopRow) {
    return new ObjectIterator(table.scan(startRow, stopRow));
  }

  @Override
  public void delete(String key) {
    delete(Bytes.toBytes(key));
  }

  @Override
  public void delete(byte[] key) {
    table.delete(key);
  }

  @Override
  public Type getRecordType() {
    return StructuredRecord.class;
  }

  @Override
  public List<Split> getSplits() {
    return table.getSplits();
  }

  @Override
  public RecordScanner<StructuredRecord> createSplitRecordScanner(Split split) {
    return new ObjectScanner(table.createSplitReader(split));
  }

  @Override
  public SplitReader<byte[], T> createSplitReader(Split split) {
    return new ObjectSplitReader(table.createSplitReader(split));
  }

  private class ObjectScanner extends RecordScanner<StructuredRecord> {
    private final ReflectionRowRecordReader rowRecordReader;
    private final SplitReader<byte[], Row> tableSplitReader;

    private ObjectScanner(SplitReader<byte[], Row> tableSplitReader) {
      this.tableSplitReader = tableSplitReader;
      this.rowRecordReader = new ReflectionRowRecordReader(tableSchema);
    }

    @Override
    public void initialize(Split split) throws InterruptedException {
      tableSplitReader.initialize(split);
    }

    @Override
    public boolean nextRecord() throws InterruptedException {
      return tableSplitReader.nextKeyValue();
    }

    @Override
    public StructuredRecord getCurrentRecord() throws InterruptedException {
      byte[] key = tableSplitReader.getCurrentKey();
      Row row = tableSplitReader.getCurrentValue();
      try {
        StructuredRecord.Builder builder = rowRecordReader.read(row, objectSchema);
        // only string or bytes are currently allowed
        if (keyType == Schema.Type.STRING) {
          builder.set(keyName, Bytes.toString(key));
        } else {
          builder.set(keyName, key);
        }
        return builder.build();
      } catch (IOException e) {
        LOG.error("Unable to read row.", e);
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void close() {
      tableSplitReader.close();
    }
  }

  private class ObjectIterator extends AbstractCloseableIterator<KeyValue<byte[], T>> {
    private final Scanner scanner;
    private boolean closed = false;

    private ObjectIterator(Scanner scanner) {
      this.scanner = scanner;
    }

    @Override
    protected KeyValue<byte[], T> computeNext() {
      Preconditions.checkState(!closed);
      Row row = scanner.next();
      if (row != null) {
        return new KeyValue<byte[], T>(row.getRow(), readRow(row));
      }
      close();
      return endOfData();
    }

    @Override
    public void close() {
      scanner.close();
      endOfData();
      closed = true;
    }
  }

  /**
   * The split reader for objects is reading a table split using the underlying Table's split reader.
   */
  private class ObjectSplitReader extends SplitReader<byte[], T> {

    // the underlying Table's split reader
    private SplitReader<byte[], Row> reader;

    public ObjectSplitReader(SplitReader<byte[], Row> reader) {
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
      return readRow(this.reader.getCurrentValue());
    }

    @Override
    public void close() {
      this.reader.close();
    }
  }

  private T readRow(Row row) {
    try {
      if (row.isEmpty()) {
        return null;
      }
      return getReflectionRowReader().read(row, objectSchema);
    } catch (Exception e) {
      // should not happen. Can happen if somebody changes the type in an incompatible way?
      throw new DataSetException("Failed to decode object: " + e.getMessage(), e);
    }
  }

  private Schema getTableSchema(Schema objectSchema, String keyName, Schema.Type keyType) {
    List<Schema.Field> fields = Lists.newArrayListWithCapacity(objectSchema.getFields().size());
    fields.add(Schema.Field.of(keyName, Schema.of(keyType)));
    fields.addAll(objectSchema.getFields());
    return Schema.recordOf("record", fields);
  }
}
