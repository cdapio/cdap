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

package io.cdap.cdap.spi.data.nosql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.dataset.lib.IndexedTable;
import io.cdap.cdap.api.dataset.table.Get;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Row;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.data2.dataset2.lib.table.MDSKey;
import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.FieldValidator;
import io.cdap.cdap.spi.data.table.field.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Nosql structured table implementation. This table will prepend the table name as the prefix for each row key.
 */
public final class NoSqlStructuredTable implements StructuredTable {
  private static final Logger LOG = LoggerFactory.getLogger(NoSqlStructuredTable.class);
  private final IndexedTable table;
  private final StructuredTableSchema schema;
  private final FieldValidator fieldValidator;
  // this key prefix will be used for any row in this table
  private final MDSKey keyPrefix;

  public NoSqlStructuredTable(IndexedTable table, StructuredTableSchema schema) {
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
    LOG.trace("Table {}: Read with keys {}", schema.getTableId(), keys);
    Row row = table.get(convertKeyToBytes(keys, false));
    return row.isEmpty() ? Optional.empty() : Optional.of(new NoSqlStructuredRow(row, schema));
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys,
                                      Collection<String> columns) throws InvalidFieldException {
    LOG.trace("Table {}: Read with keys {} and columns {}", schema.getTableId(), keys, columns);
    if (columns == null || columns.isEmpty()) {
      throw new IllegalArgumentException("No columns are specified to read");
    }
    Row row = table.get(convertKeyToBytes(keys, false),
                        convertColumnsToBytes(columns));
    return row.isEmpty() ? Optional.empty() : Optional.of(new NoSqlStructuredRow(row, schema));
  }

  @Override
  public Collection<StructuredRow> multiRead(Collection<? extends Collection<Field<?>>> multiKeys)
    throws InvalidFieldException {
    List<Get> gets = multiKeys.stream()
      .map(k -> convertKeyToBytes(k, false))
      .map(Get::new)
      .collect(Collectors.toList());
    return table.get(gets).stream()
      .filter(r -> !r.isEmpty())
      .map(r -> new NoSqlStructuredRow(r, schema))
      .collect(Collectors.toList());
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit) throws InvalidFieldException {
    LOG.trace("Table {}: Scan range {} with limit {}", schema.getTableId(), keyRange, limit);
    return new LimitIterator(Collections.singleton(new ScannerIterator(getScanner(keyRange), schema)).iterator(),
                             limit);
  }

  @Override
  public CloseableIterator<StructuredRow> multiScan(Collection<Range> keyRanges,
                                                    int limit) throws InvalidFieldException, IOException {
    // Sort the scan keys by the start key and merge overlapping ranges.
    Deque<ImmutablePair<byte[], byte[]>> scanKeys = new LinkedList<>();
    keyRanges.stream()
      .map(this::createScanKeys)
      .sorted((o1, o2) -> Bytes.compareTo(o1.getFirst(), o2.getFirst()))
      .forEach(range -> {
        if (scanKeys.isEmpty()) {
          scanKeys.add(range);
        } else {
          ImmutablePair<byte[], byte[]> last = scanKeys.getLast();
          if (Bytes.compareTo(last.getSecond(), range.getFirst()) < 0) {
            // No overlap
            scanKeys.add(range);
          } else {
            // Combine overlapping ranges
            scanKeys.pollLast();
            byte[] end = Bytes.compareTo(last.getSecond(), range.getSecond()) > 0
              ? last.getSecond() : range.getSecond();
            scanKeys.addLast(ImmutablePair.of(last.getFirst(), end));
          }
        }
      });

    Iterator<ImmutablePair<byte[], byte[]>> rangeIterator = scanKeys.iterator();
    return new LimitIterator(new AbstractIterator<ScannerIterator>() {
      @Override
      protected ScannerIterator computeNext() {
        if (!rangeIterator.hasNext()) {
          return endOfData();
        }
        ImmutablePair<byte[], byte[]> range = rangeIterator.next();
        return new ScannerIterator(table.scan(range.getFirst(), range.getSecond()), schema);
      }
    }, limit);
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Field<?> index) throws InvalidFieldException {
    LOG.trace("Table {}: Scan index {}", schema.getTableId(), index);
    fieldValidator.validateField(index);
    if (!schema.isIndexColumn(index.getName())) {
      throw new InvalidFieldException(schema.getTableId(), index.getName(), "is not an indexed column");
    }
    Scanner scanner = table.readByIndex(convertColumnsToBytes(Collections.singleton(index.getName()))[0],
                                        fieldToBytes(index));
    return new ScannerIterator(scanner, schema);
  }

  @Override
  public boolean compareAndSwap(Collection<Field<?>> keys, Field<?> oldValue, Field<?> newValue) {
    LOG.trace("Table {}: CompareAndSwap with keys {}, oldValue {}, newValue {}", schema.getTableId(), keys,
              oldValue, newValue);
    fieldValidator.validateField(oldValue);
    if (oldValue.getFieldType() != newValue.getFieldType()) {
      throw new IllegalArgumentException(
        String.format("Field types of oldValue (%s) and newValue (%s) are not the same",
                      oldValue.getFieldType(), newValue.getFieldType()));
    }
    if (!oldValue.getName().equals(newValue.getName())) {
      throw new IllegalArgumentException(
        String.format("Trying to compare and swap different fields. Old Value = %s, New Value = %s",
                      oldValue, newValue));
    }
    if (schema.isPrimaryKeyColumn(oldValue.getName())) {
      throw new IllegalArgumentException("Cannot use compare and swap on a primary key field");
    }

    return table.compareAndSwap(convertKeyToBytes(keys, false), Bytes.toBytes(oldValue.getName()),
                                fieldToBytes(oldValue),
                                fieldToBytes(newValue));
  }

  @Override
  public void increment(Collection<Field<?>> keys, String column, long amount) {
    LOG.trace("Table {}: Increment with keys {}, column {}, amount {}", schema.getTableId(), keys, column, amount);
    FieldType.Type colType = schema.getType(column);
    if (colType == null) {
      throw new InvalidFieldException(schema.getTableId(), column);
    } else if (colType != FieldType.Type.LONG) {
      throw new IllegalArgumentException(
        String.format("Trying to increment a column of type %s. Only %s column type can be incremented",
                      colType, FieldType.Type.LONG));
    }
    if (schema.isPrimaryKeyColumn(column)) {
      throw new IllegalArgumentException("Cannot use increment on a primary key field");
    }

    table.increment(convertKeyToBytes(keys, false), Bytes.toBytes(column), amount);
  }

  @Override
  public void delete(Collection<Field<?>> keys) throws InvalidFieldException {
    LOG.trace("Table {}: Delete with keys {}", schema.getTableId(), keys);
    table.delete(convertKeyToBytes(keys, false));
  }

  @Override
  public void deleteAll(Range keyRange) throws InvalidFieldException, IOException {
    LOG.trace("Table {}: DeleteAll with range {}", schema.getTableId(), keyRange);
    try (Scanner scanner = getScanner(keyRange)) {
      Row row;
      while ((row = scanner.next()) != null) {
        table.delete(row.getRow());
      }
    }
  }

  @Override
  public long count(Collection<Range> keyRanges) throws IOException {
    LOG.trace("Table {}: count with ranges {}", schema.getTableId(), keyRanges);
    long count = 0;
    for (Range keyRange: keyRanges) {
      try (Scanner scanner = getScanner(keyRange)) {
        while (scanner.next() != null) {
          count++;
        }
      }
    }
    return count;
  }

  @Override
  public void close() throws IOException {
    table.close();
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
    fieldValidator.validatePrimaryKeys(keys, allowPrefix);
    MDSKey.Builder mdsKey = new MDSKey.Builder(keyPrefix);
    for (Field<?> key : keys) {
      addKey(mdsKey, key, schema.getType(key.getName()));
    }
    return mdsKey.build().getKey();
  }

  /**
   * Convert the columns to corresponding byte array, each column has to be part of the schema.
   *
   * @param columns columns to convert
   * @return the converted byte array
   * @throws InvalidFieldException some column is not part of the schema
   */
  private byte[][] convertColumnsToBytes(Collection<String> columns) throws InvalidFieldException {
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
      throw new InvalidFieldException(schema.getTableId(), fields,
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
        values[i] = fieldToBytes(field);
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

  private byte[] fieldToBytes(Field<?> field) throws InvalidFieldException {
    if (field.getValue() == null) {
      return null;
    }

    switch (field.getFieldType()) {
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

  private Scanner getScanner(Range keyRange) {
    ImmutablePair<byte[], byte[]> keys = createScanKeys(keyRange);
    return table.scan(keys.getFirst(), keys.getSecond());
  }

  private ImmutablePair<byte[], byte[]> createScanKeys(Range keyRange) {
    // the method will always prepend the table name as prefix
    byte[] begin = convertKeyToBytes(keyRange.getBegin(), true);
    byte[] end = convertKeyToBytes(keyRange.getEnd(), true);

    // Table.scan() start key is inclusive by default, and if it is EXCLUSIVE, we want to ensure the start keys are
    // not empty so that we do not scan from the start of some other table
    if (!keyRange.getBegin().isEmpty() && keyRange.getBeginBound() == Range.Bound.EXCLUSIVE) {
      begin = Bytes.stopKeyForPrefix(begin);
    }

    // Table.scan() stop key is exclusive by default, so when the end keys are not specifies, we will need to scan to
    // the end of table, which will be the default table prefix + 1.
    if (keyRange.getEnd().isEmpty() || keyRange.getEndBound() == Range.Bound.INCLUSIVE) {
      end = Bytes.stopKeyForPrefix(end);
    }

    return ImmutablePair.of(begin, end);
  }

  /**
   * Limit the number of elements returned by a {@link ScannerIterator}.
   */
  @VisibleForTesting
  static final class LimitIterator extends AbstractCloseableIterator<StructuredRow> {
    private final Iterator<? extends CloseableIterator<StructuredRow>> scannerIterator;
    private final int limit;
    private CloseableIterator<StructuredRow> currentScanner;
    private int count;

    LimitIterator(Iterator<? extends CloseableIterator<StructuredRow>> scannerIterator, int limit) {
      this.scannerIterator = scannerIterator;
      this.limit = limit;
      this.currentScanner = scannerIterator.hasNext() ? scannerIterator.next() : CloseableIterator.empty();
    }

    @Override
    protected StructuredRow computeNext() {
      if (count >= limit) {
        return endOfData();
      }

      while (!currentScanner.hasNext() && scannerIterator.hasNext()) {
        closeScanner();
        currentScanner = scannerIterator.next();
      }

      if (!currentScanner.hasNext()) {
        return endOfData();
      }

      StructuredRow row = currentScanner.next();
      ++count;
      return row;
    }

    @Override
    public void close() {
      closeScanner();
    }

    private void closeScanner() {
      if (currentScanner != null) {
        currentScanner.close();
        currentScanner = null;
      }
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
