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
import co.cask.cdap.internal.io.TypeRepresentation;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

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

  private final Table table;
  private final Schema schema;

  private final ReflectionPutWriter<T> putWriter;
  private final ReflectionRowReader<T> rowReader;

  @SuppressWarnings("unchecked")
  public ObjectMappedTableDataset(String name, Table table, TypeRepresentation typeRep,
                                  Schema schema, @Nullable ClassLoader classLoader) {
    super(name, table);
    this.table = table;
    this.schema = schema;
    this.putWriter = new ReflectionPutWriter<T>(schema);
    typeRep.setClassLoader(classLoader);
    this.rowReader = new ReflectionRowReader<T>(schema, (TypeToken<T>) TypeToken.of(typeRep.toType()));
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
    // TODO: implement (CDAP-1472)
    throw new UnsupportedOperationException();
  }

  @Override
  public SplitReader<byte[], T> createSplitReader(Split split) {
    return new ObjectScanner(table.createSplitReader(split));
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
  private class ObjectScanner extends SplitReader<byte[], T> {

    // the underlying Table's split reader
    private SplitReader<byte[], Row> reader;

    public ObjectScanner(SplitReader<byte[], Row> reader) {
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
      return rowReader.read(row, schema);
    } catch (Exception e) {
      // should not happen. Can happen if somebody changes the type in an incompatible way?
      throw new DataSetException("Failed to decode object: " + e.getMessage(), e);
    }
  }
}
