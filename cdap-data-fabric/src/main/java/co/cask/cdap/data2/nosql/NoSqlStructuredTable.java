/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.data2.nosql;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.spi.data.InvalidFieldException;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.table.TableSchema;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.FieldType;
import co.cask.cdap.spi.data.table.field.Range;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 *
 */
public final class NoSqlStructuredTable implements StructuredTable {
  private static final Logger LOG = LoggerFactory.getLogger(NoSqlStructuredTable.class);
  private final StructuredTableId tableId;
  private final Table table;
  private final TableSchema schema;

  public NoSqlStructuredTable(Table table, TableSchema schema) {
    this.tableId = schema.getTableId();
    this.table = table;
    this.schema = schema;
  }

  @Override
  public void write(Collection<Field<?>> fields) {
    LOG.trace("Table {}: Write fields {}", tableId, fields);
    Put put = convertFieldsToBytes(fields);
    table.put(put.key, put.columns, put.values);
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys) throws InvalidFieldException {
    return read(keys, Collections.emptySet());
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys, Collection<String> columns) {
    LOG.trace("Table {}: Read with keys {} and columns", tableId, keys, columns);
    co.cask.cdap.api.dataset.table.Row row = table.get(convertKeyToBytes(keys, false),
                                                       convertColumnsToBytes(columns));
    return row.isEmpty() ? Optional.empty() : Optional.of(new NoSqlStructuredRow(row, schema));
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit) {
    LOG.trace("Table {}: Scan range {} with limit {}", tableId, keyRange, limit);
    byte[] begin = keyRange.getBegin() == null ? null : convertKeyToBytes(keyRange.getBegin(), true);
    byte[] end = keyRange.getEnd() == null ? null : convertKeyToBytes(keyRange.getEnd(), true);

    // Table.scan() start key is inclusive by default
    if (begin != null && keyRange.getBeginBound() == Range.Bound.EXCLUSIVE) {
      begin = Bytes.stopKeyForPrefix(begin);
    }

    // Table.scan() stop key is exclusive by default
    if (end != null && keyRange.getEndBound() == Range.Bound.INCLUSIVE) {
      end = Bytes.stopKeyForPrefix(end);
    }

    Scanner scanner = table.scan(begin, end);
    return new LimitIterator(new ScannerIterator(scanner, schema), limit);
  }

  @Override
  public void delete(Collection<Field<?>> keys) {
    LOG.trace("Table {}: Delete with keys {}", tableId, keys);
    table.delete(convertKeyToBytes(keys, false));
  }


  @Override
  public void close() throws Exception {
    table.close();
  }

  // TODO: implement check to make sure all the keys are provided when doing read or delete
  private byte[] convertKeyToBytes(Collection<Field<?>> keys, boolean allowPartialKey) {
    MDSKey.Builder byteKey = new MDSKey.Builder();
    for (Field<?> key : keys) {
      // TODO: check schema
      if (schema.isKey(key.getName())) {
        addKey(byteKey, key, schema.getType(key.getName()));
      } else {
        throw new InvalidFieldException(schema.getTableId(), key.getName());
      }
    }

    // TODO: Verify if all the keys are provided
    return byteKey.build().getKey();
  }

  private byte[][] convertColumnsToBytes(Collection<String> columns) {
    // Empty columns means to read all columns. The corresponding parameter for Table is null
    if (columns.isEmpty()) {
      return null;
    }

    byte[][] bytes = new byte[columns.size()][];
    int i = 0;
    for (String column : columns) {
      bytes[i] = Bytes.toBytes(column);
      i++;
    }
    return bytes;
  }

  private Put convertFieldsToBytes(Collection<Field<?>> fields) {
    int numColumns = fields.size() - schema.getPrimaryKeys().size();
    if (numColumns < 0) {
      // TODO: can be removed after full primary key check
      throw new InvalidFieldException(tableId, "");
    }

    MDSKey.Builder key = new MDSKey.Builder();
    byte[][] columns = new byte[numColumns][];
    byte[][] values = new byte[numColumns][];

    int i = 0;
    for (Field<?> field : fields) {
      if (schema.isKey(field.getName())) {
        addKey(key, field, schema.getType(field.getName()));
      } else {
        columns[i] = Bytes.toBytes(field.getName());
        values[i] = fieldToBytes(field, schema.getType(field.getName()));
      }
    }

    // TODO: Verify if all the keys are provided
    return new Put(key.build().getKey(), columns, values);
  }

  private void addKey(MDSKey.Builder key, Field<?> field, FieldType.Type type) {
    if (field.getValue() == null) {
      throw new InvalidFieldException(schema.getTableId(), field.getName(), "is a primary key and value is null");
    }

    switch (type) {
      case INTEGER:
        key.add((Integer) field.getValue());
        return;
      case LONG:
        key.add((Long) field.getValue());
        return;
      case STRING:
        key.add((String) field.getValue());
        return;
      default:
        throw new InvalidFieldException(schema.getTableId(), field.getName());
    }
  }

  private byte[] fieldToBytes(Field<?> field, FieldType.Type type) {
    if (field.getValue() == null) {
      return null;
    }

    switch (type) {
      case INTEGER:
        return Bytes.toBytes((Integer) field.getValue());
      case LONG:
        return Bytes.toBytes((Long) field.getValue());
      case FLOAT:
        return Bytes.toBytes((Float) field.getValue());
      case DOUBLE:
        return Bytes.toBytes((Double) field.getValue());
      case STRING:
        return Bytes.toBytes((String) field.getValue());
        default:
          throw new InvalidFieldException(schema.getTableId(), field.getName());
    }
  }

  private static final class Put {
    private final byte[] key;
    private final byte[][] columns;
    private final byte[][] values;

    Put(byte[] key, byte[][] columns, byte[][] values) {
      this.key = key;
      this.columns = columns;
      this.values = values;
    }
  }

  /**
   * Limit the number of elements returned by a {@link ScannerIterator}.
   */
  @VisibleForTesting
  static final class LimitIterator extends AbstractCloseableIterator<StructuredRow> {
    private final ScannerIterator scannerIterator;
    private final int limit;
    private int count;

    LimitIterator(ScannerIterator scannerIterator, int limit) {
      this.scannerIterator = scannerIterator;
      this.limit = limit;
    }

    @Override
    protected StructuredRow computeNext() {
      if (count >= limit) {
        return endOfData();
      }
      StructuredRow row = scannerIterator.computeNext();
      if (row == null) {
        return endOfData();
      }
      ++count;
      return row;
    }

    @Override
    public void close() {
      scannerIterator.close();
    }
  }

  /**
   * Create a {@link CloseableIterator} from a {@link Scanner}.
   */
  @VisibleForTesting
  static final class ScannerIterator extends AbstractCloseableIterator<StructuredRow> {
    private final Scanner scanner;
    private final TableSchema schema;

    ScannerIterator(Scanner scanner, TableSchema schema) {
      this.scanner = scanner;
      this.schema = schema;
    }

    @Override
    protected StructuredRow computeNext() {
      co.cask.cdap.api.dataset.table.Row row = scanner.next();
      if (row == null) {
        return endOfData();
      }
      return new NoSqlStructuredRow(row, schema);
    }

    @Override
    public void close() {
      scanner.close();
    }
  }
}
