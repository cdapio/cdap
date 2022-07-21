/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.table;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.annotation.ReadOnly;
import io.cdap.cdap.api.annotation.WriteOnly;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.batch.RecordScanner;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.data.batch.SplitReader;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DataSetException;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.AbstractDataset;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.lib.ObjectMappedTable;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scan;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.internal.io.ReflectionPutWriter;
import io.cdap.cdap.internal.io.ReflectionRowReader;
import io.cdap.cdap.internal.io.TypeRepresentation;
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
  private final TypeRepresentation typeRepresentation;
  private final ReflectionPutWriter<T> putWriter;
  // we get this lazily, since we may not have the actual Type when using this as a RecordScannable,
  // but we do expect to have it when using it in a program context
  private ReflectionRowReader<T> rowReader;

  // schema is passed in as an argument because it is a required dataset property for validation purposes, so
  // the ObjectMappedTableDefinition will always have it. We could always derive the schema from the type,
  // but it is simpler to just pass it in.
  public ObjectMappedTableDataset(String name, Table table, TypeRepresentation typeRep,
                                  Schema objectSchema, @Nullable ClassLoader classLoader) {
    super(name, table);
    this.table = table;
    this.objectSchema = objectSchema;
    this.typeRepresentation = typeRep;
    this.typeRepresentation.setClassLoader(classLoader);
    this.putWriter = new ReflectionPutWriter<>(objectSchema);
  }

  @SuppressWarnings("unchecked")
  private ReflectionRowReader<T> getReflectionRowReader() {
    if (rowReader == null) {
      try {
        // this can throw a runtime exception from a ClassNotFoundException
        Type type = typeRepresentation.toType();
        rowReader = new ReflectionRowReader<>(objectSchema, (TypeToken<T>) TypeToken.of(type));
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
    return rowReader;
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

  @WriteOnly
  @Override
  public void write(String key, T object) {
    write(Bytes.toBytes(key), object);
  }

  @WriteOnly
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

  @ReadOnly
  @Override
  public T read(String key) {
    return read(Bytes.toBytes(key));
  }

  @ReadOnly
  @Override
  public T read(byte[] key) {
    return readRow(table.get(key));
  }

  @ReadOnly
  @Override
  public CloseableIterator<KeyValue<byte[], T>> scan(@Nullable String startRow, @Nullable String stopRow) {
    return scan(startRow == null ? null : Bytes.toBytes(startRow), stopRow == null ? null : Bytes.toBytes(stopRow));
  }

  @ReadOnly
  @Override
  public CloseableIterator<KeyValue<byte[], T>> scan(byte[] startRow, byte[] stopRow) {
    return new ObjectIterator(table.scan(startRow, stopRow));
  }

  @ReadOnly
  @Override
  public CloseableIterator<KeyValue<byte[], T>> scan(Scan scan) {
    return new ObjectIterator(table.scan(scan));
  }

  @WriteOnly
  @Override
  public void delete(String key) {
    delete(Bytes.toBytes(key));
  }

  @WriteOnly
  @Override
  public void delete(byte[] key) {
    table.delete(key);
  }

  @Override
  public Type getRecordType() {
    return table.getRecordType();
  }

  @Override
  public List<Split> getSplits() {
    return table.getSplits();
  }

  @Override
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    return table.getSplits(numSplits, start, stop);
  }

  @ReadOnly
  @Override
  public RecordScanner<StructuredRecord> createSplitRecordScanner(Split split) {
    return table.createSplitRecordScanner(split);
  }

  @ReadOnly
  @Override
  public SplitReader<byte[], T> createSplitReader(Split split) {
    return new ObjectSplitReader(table.createSplitReader(split));
  }

  @Override
  public void write(StructuredRecord structuredRecord) throws IOException {
    table.write(structuredRecord);
  }

  private class ObjectIterator extends AbstractCloseableIterator<KeyValue<byte[], T>> {
    private final Scanner scanner;
    private boolean closed;

    private ObjectIterator(Scanner scanner) {
      this.scanner = scanner;
    }

    @Override
    protected KeyValue<byte[], T> computeNext() {
      Preconditions.checkState(!closed);
      Row row = scanner.next();
      if (row != null) {
        return new KeyValue<>(row.getRow(), readRow(row));
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

    ObjectSplitReader(SplitReader<byte[], Row> reader) {
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
}
