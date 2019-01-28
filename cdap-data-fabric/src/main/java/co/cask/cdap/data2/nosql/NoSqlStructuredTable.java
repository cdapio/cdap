/*
 * Copyright © 2019 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.spi.data.InvalidFieldException;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.table.StructuredTableSchema;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.FieldType;
import co.cask.cdap.spi.data.table.field.FieldValidator;
import co.cask.cdap.spi.data.table.field.Range;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Nosql structured table implementation. This table will prepend the table name as the prefix for each row key.
 */
public final class NoSqlStructuredTable implements StructuredTable {
  private static final Logger LOG = LoggerFactory.getLogger(NoSqlStructuredTable.class);
  private final Table table;
  private final StructuredTableSchema schema;
  private final FieldValidator fieldValidator;
  // this key prefix will be used for any row in this table
  private final MDSKey keyPrefix;

  public NoSqlStructuredTable(Table table, StructuredTableSchema schema) {
    this.table = table;
    this.schema = schema;
    this.keyPrefix = new MDSKey.Builder().add(schema.getTableId().getName()).build();
    this.fieldValidator = new FieldValidator(schema);
  }

  @Override
  public void upsert(Collection<Field<?>> fields) throws InvalidFieldException {
    LOG.trace("Table {}: Write fields {}", schema.getTableId(), fields);
    table.put(convertFieldsToBytes(fields));
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys) throws InvalidFieldException {
    return readRow(keys, null);
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys,
                                      Collection<String> columns) throws InvalidFieldException {
    if (columns == null || columns.isEmpty()) {
      throw new InvalidFieldException(schema.getTableId(), columns, "No columns are specified in reading.");
    }
    return readRow(keys, columns);
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit) throws InvalidFieldException {
    LOG.trace("Table {}: Scan range {} with limit {}", schema.getTableId(), keyRange, limit);
    // the method will always prepend the table name as prefix
    byte[] begin = convertKeyToBytes(keyRange.getBegin(), true);
    byte[] end = convertKeyToBytes(keyRange.getEnd(), true);

    // Table.scan() start key is inclusive by default, and if it is EXCLUSTIVE, we want to ensure the start keys are
    // not empty so that we do not scan from the start of some other table
    if (!keyRange.getBegin().isEmpty() && keyRange.getBeginBound() == Range.Bound.EXCLUSIVE) {
      begin = Bytes.stopKeyForPrefix(begin);
    }

    // Table.scan() stop key is exclusive by default, so when the end keys are not specifies, we will need to scan to
    // the end of table, which will be the default table prefix + 1.
    if (keyRange.getEnd().isEmpty() || keyRange.getEndBound() == Range.Bound.INCLUSIVE) {
      end = Bytes.stopKeyForPrefix(end);
    }

    Scanner scanner = table.scan(begin, end);
    return new LimitIterator(new ScannerIterator(scanner, schema), limit);
  }

  @Override
  public void delete(Collection<Field<?>> keys) throws InvalidFieldException {
    LOG.trace("Table {}: Delete with keys {}", schema.getTableId(), keys);
    table.delete(convertKeyToBytes(keys, false));
  }

  @Override
  public void close() throws IOException {
    table.close();
  }

  /**
   * Read a row from the table. Null columns mean read from all columns.
   *
   * @param keys key of the row
   * @param columns columns to read, null means read from all
   * @return an optional containing the row or empty optional if the row does not exist
   */
  private Optional<StructuredRow> readRow(Collection<Field<?>> keys,
                                          @Nullable Collection<String> columns) throws InvalidFieldException {
    LOG.trace("Table {}: Read with keys {} and columns {}", schema.getTableId(), keys, columns);
    Row row = table.get(convertKeyToBytes(keys, false),
                        convertColumnsToBytes(columns));
    return row.isEmpty() ? Optional.empty() : Optional.of(new NoSqlStructuredRow(row, schema));
  }

  /**
   * Convert the keys to corresponding byte array. The keys can either be a prefix or complete primary keys depending
   * on the value of allowPrefix. The method will always prepend the table name as a prefix for the row keys.
   *
   * @param keys keys to convert
   * @param allowPrefix true if the keys can be prefix false if the keys have to contain all the primary keys.
   * @return the byte array converted
   * @throws InvalidFieldException if the key are not prefix or complete primary keys
   */
  private byte[] convertKeyToBytes(Collection<Field<?>> keys, boolean allowPrefix) throws InvalidFieldException {
    schema.validatePrimaryKeys(keys.stream().map(Field::getName).collect(Collectors.toList()), allowPrefix);
    MDSKey.Builder mdsKey = new MDSKey.Builder(keyPrefix);
    for (Field<?> key : keys) {
      fieldValidator.validateField(key);
      addKey(mdsKey, key, schema.getType(key.getName()));
    }
    return mdsKey.build().getKey();
  }

  /**
   * Convert the columns to corresponding byte array, each column has to be part of the schema.
   *
   * @param columns columns to convert
   * @return the converted byte array, null if read for all columns
   * @throws InvalidFieldException some column is not part of the schema
   */
  @Nullable
  private byte[][] convertColumnsToBytes(@Nullable Collection<String> columns) throws InvalidFieldException {
    // Empty columns means to read all columns. The corresponding parameter for Table is null
    if (columns == null) {
      return null;
    }

    byte[][] bytes = new byte[columns.size()][];
    int i = 0;
    for (String column : columns) {
      if (schema.getType(column) == null) {
        throw new InvalidFieldException(schema.getTableId(), column);
      }
      bytes[i] = Bytes.toBytes(column);
      i++;
    }
    return bytes;
  }

  /**
   * Convert the fields to a {@link Put} to write to table. The primary key must all be provided. The method will
   * add the table name as prefix to the row key.
   *
   * @param fields the fields to write
   * @return a PUT object
   * @throws InvalidFieldException if primary keys are missing or the column is not in schema
   */
  private Put convertFieldsToBytes(Collection<Field<?>> fields) throws InvalidFieldException {
    Set<String> fieldNames = fields.stream().map(Field::getName).collect(Collectors.toSet());
    if (!fieldNames.containsAll(schema.getPrimaryKeys())) {
      throw new InvalidFieldException(schema.getTableId(), fieldNames,
                                      String.format("Given fields %s does not contain all the " +
                                                      "primary keys %s", fieldNames, schema.getPrimaryKeys()));
    }
    int numColumns = fields.size() - schema.getPrimaryKeys().size();

    // add the table name as the prefix
    MDSKey.Builder key = new MDSKey.Builder(keyPrefix);
    byte[][] columns = new byte[numColumns][];
    byte[][] values = new byte[numColumns][];

    int i = 0;
    for (Field<?> field : fields) {
      fieldValidator.validateField(field);
      if (schema.isPrimaryKeyColumn(field.getName())) {
        addKey(key, field, schema.getType(field.getName()));
      } else {
        if (schema.getType(field.getName()) == null) {
          throw new InvalidFieldException(schema.getTableId(), field.getName());
        }
        columns[i] = Bytes.toBytes(field.getName());
        values[i] = fieldToBytes(field, schema.getType(field.getName()));
        i++;
      }
    }

    Put put = new Put(key.build().getKey());
    for (int index = 0; index < columns.length; index++) {
      put.add(columns[index], values[index]);
    }
    return put;
  }

  private void addKey(MDSKey.Builder key, Field<?> field, FieldType.Type type) throws InvalidFieldException {
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
      case BYTES:
        key.add((byte[]) field.getValue());
        return;
      default:
        throw new InvalidFieldException(schema.getTableId(), field.getName());
    }
  }

  private byte[] fieldToBytes(Field<?> field, FieldType.Type type) throws InvalidFieldException {
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
      case BYTES:
        return (byte[]) field.getValue();
      default:
        throw new InvalidFieldException(schema.getTableId(), field.getName());
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
    private final StructuredTableSchema schema;

    ScannerIterator(Scanner scanner, StructuredTableSchema schema) {
      this.scanner = scanner;
      this.schema = schema;
    }

    @Override
    protected StructuredRow computeNext() {
      Row row = scanner.next();
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
