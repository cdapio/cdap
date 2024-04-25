/*
 * Copyright © 2019-2022 Cask Data, Inc.
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
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.data2.dataset2.lib.table.MDSKey;
import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.FieldValidator;
import io.cdap.cdap.spi.data.table.field.Range;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Nosql structured table implementation. This table will prepend the table name as the prefix for
 * each row key.
 */
public final class NoSqlStructuredTable implements StructuredTable {

  private static final Logger LOG = LoggerFactory.getLogger(NoSqlStructuredTable.class);
  private static final Logger LOG_RATE_LIMITED =
      Loggers.sampling(LOG, LogSamplers.limitRate(TimeUnit.MINUTES.toMillis(5L)));
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
  public void update(Collection<Field<?>> fields) throws InvalidFieldException {
    LOG.trace("Table {}: Update fields {}", schema.getTableId(), fields);
    Put put = updateFieldsToBytes(fields);
    // Put will not have values if a row is not being updated
    if (!put.getValues().isEmpty()) {
      table.put(put);
    }
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
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit)
      throws InvalidFieldException {
    LOG.trace("Table {}: Scan range {} with limit {}", schema.getTableId(), keyRange, limit);
    return new LimitIterator(Collections.singleton(getFilterByRangeIterator(keyRange)).iterator(),
        limit);
  }

  /**
   * Scan the nosql db with a key range. It gets the longest prefix keys, scan the DB and do a post
   * filtering on the result.
   *
   * @param keyRange prefix key range to scan
   * @return the iterator after filtering
   * @throws InvalidFieldException if the key is not a primary key
   */
  private CloseableIterator<StructuredRow> getFilterByRangeIterator(Range keyRange) {
    Range prefixRange = getLongestPrefixRange(keyRange);
    ScannerIterator scannerIterator = new ScannerIterator(getScanner(prefixRange), schema);

    Range filterRange = getFilterRange(keyRange, prefixRange);
    // Return scannerIterator once we found that there's no need to further filter
    if (filterRange.getBegin().isEmpty() && filterRange.getEnd().isEmpty()) {
      return scannerIterator;
    }

    // TODO: remove this warning after CDAP-20177
    LOG_RATE_LIMITED.warn("Potential performance impact while scanning table {} with range {} "
            + "which does in-memory filtering with filterRange {}",
        schema.getTableId(), keyRange, filterRange);
    return new FilterByRangeIterator(Collections.singleton(scannerIterator).iterator(),
        Collections.singleton(filterRange));
  }

  private Range getLongestPrefixRange(Range range) {
    fieldValidator.validateScanRange(range);

    List<Field<?>> beginPrefixKeys = getPrefixPrimaryKeys(range.getBegin());
    List<Field<?>> endPrefixKeys = getPrefixPrimaryKeys(range.getEnd());

    // Edge case for table with primary keys of [KEY, KEY2, KEY3...]
    // We give range of ([KEY: "ns1", KEY3: 123], EXCLUSIVE, [KEY: "ns1", KEY3: 789], EXCLUSIVE)
    // KEY: "ns1" should be INCLUSIVE while scanning
    // The longest prefix range should only be EXCLUSIVE when there's no keyholes and bound is EXCLUSIVE
    Range.Bound beginBound =
        beginPrefixKeys.size() == range.getBegin().size()
            && range.getBeginBound() == Range.Bound.EXCLUSIVE
            ? Range.Bound.EXCLUSIVE : Range.Bound.INCLUSIVE;

    Range.Bound endBound =
        endPrefixKeys.size() == range.getEnd().size()
            && range.getEndBound() == Range.Bound.EXCLUSIVE
            ? Range.Bound.EXCLUSIVE : Range.Bound.INCLUSIVE;

    return Range.create(beginPrefixKeys, beginBound, endPrefixKeys, endBound);
  }

  private Range getFilterRange(Range keyRange, Range prefixRange) {
    List<Field<?>> beginFilterKeys = getFilterKeys(keyRange.getBegin(), prefixRange.getBegin());
    List<Field<?>> endFilterKeys = getFilterKeys(keyRange.getEnd(), prefixRange.getEnd());

    return Range.create(beginFilterKeys, keyRange.getBeginBound(), endFilterKeys,
        keyRange.getEndBound());
  }

  private List<Field<?>> getFilterKeys(Collection<Field<?>> keys, Collection<Field<?>> prefixKeys) {
    Set<String> prefixKeysSet = prefixKeys.stream().map(Field::getName).collect(Collectors.toSet());

    return keys.stream()
        .filter(key -> !prefixKeysSet.contains(key.getName()))
        .collect(Collectors.toList());
  }

  private List<Field<?>> getPrefixPrimaryKeys(Collection<Field<?>> partialKeys) {
    List<Field<?>> prefixKeys = new ArrayList<>();
    List<String> primaryKeys = schema.getPrimaryKeys();
    int i = 0;
    for (Field<?> key : partialKeys) {
      if (!key.getName().equals(primaryKeys.get(i))) {
        return prefixKeys;
      }
      prefixKeys.add(key);
      i++;
    }
    return prefixKeys;
  }

  /*
   *  Sorting in memory for the no-sql implementation. We sort post table scan.
   * */
  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit, String orderByField,
      SortOrder sortOrder)
      throws InvalidFieldException, IOException {
    LOG.trace("Table {}: Scan range {} with limit {}, sort field {} and sort order {}",
        schema.getTableId(),
        keyRange, limit, orderByField, sortOrder);
    if (!schema.isIndexColumn(orderByField) && !schema.isPrimaryKeyColumn(orderByField)) {
      throw new InvalidFieldException(schema.getTableId(), orderByField,
          "is not an indexed column or primary key");
    }
    Comparator<StructuredRow> comparator =
        sortOrder.equals(SortOrder.ASC) ? getComparator(orderByField) :
            getComparator(orderByField).reversed();
    PriorityQueue<StructuredRow> rows = new PriorityQueue<>(comparator);

    try (CloseableIterator<StructuredRow> filterIterator = getFilterByRangeIterator(keyRange)) {
      while (filterIterator.hasNext()) {
        rows.offer(filterIterator.next());
      }
    }
    // return an iterator for the sorted elements
    CloseableIterator<StructuredRow> abstractCloseableIterator = new AbstractCloseableIterator<StructuredRow>() {
      @Override
      protected StructuredRow computeNext() {
        if (rows.isEmpty()) {
          return endOfData();
        }
        return rows.poll();
      }

      @Override
      public void close() {
        // no-op
      }
    };
    return new LimitIterator(abstractCloseableIterator, limit);
  }

  private Comparator<StructuredRow> getComparator(String orderByField)
      throws InvalidFieldException {
    switch (schema.getType(orderByField)) {
      case INTEGER:
        return (row1, row2) -> Objects.compare(row1.getInteger(orderByField),
            row2.getInteger(orderByField),
            Integer::compare);
      case LONG:
        return (row1, row2) -> Objects.compare(row1.getLong(orderByField),
            row2.getLong(orderByField), Long::compare);
      case FLOAT:
        return (row1, row2) -> Objects.compare(row1.getFloat(orderByField),
            row2.getFloat(orderByField),
            Float::compare);
      case DOUBLE:
        return (row1, row2) -> Objects.compare(row1.getDouble(orderByField),
            row2.getDouble(orderByField),
            Double::compare);
      case STRING:
        return (row1, row2) -> Objects.compare(row1.getString(orderByField),
            row2.getString(orderByField),
            String::compareTo);
      case BYTES:
        return (row1, row2) -> new Bytes.ByteArrayComparator().compare(row1.getBytes(orderByField),
            row2.getBytes(orderByField));
      case BOOLEAN:
        return (row1, row2) -> Objects.compare(row1.getBoolean(orderByField),
            row2.getBoolean(orderByField),
            Boolean::compareTo);
      default:
        throw new InvalidFieldException(schema.getTableId(), orderByField);
    }
  }

  /*
   * Filtering post table scan.
   * */
  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit,
      Collection<Field<?>> filterIndexes)
      throws InvalidFieldException, IOException {
    LOG.trace("Table {}: Scan range {} with limit {} and index {}", schema.getTableId(),
        keyRange, limit, filterIndexes);
    filterIndexes.forEach(fieldValidator::validateField);
    if (!schema.isIndexColumns(
        filterIndexes.stream().map(Field::getName).collect(Collectors.toList()))) {
      throw new InvalidFieldException(schema.getTableId(), filterIndexes,
          "are not all indexed columns");
    }
    FilterByFieldIterator filterByIndexIterator =
        new FilterByFieldIterator(getFilterByRangeIterator(keyRange), filterIndexes, schema);
    return new LimitIterator(Collections.singleton(filterByIndexIterator).iterator(), limit);
  }

  /*
   * Scan the nosql DB with provided ranges.
   * Find out the longest prefix ranges, merge them if possible, scan these intervals
   * then filter by ranges, the result should already be sorted by primary keys.
   * */
  @Override
  public CloseableIterator<StructuredRow> multiScan(Collection<Range> keyRanges, int limit)
      throws InvalidFieldException, IOException {

    if (keyRanges.isEmpty()) {
      return CloseableIterator.empty();
    }

    // Call scan single range when there's only range
    if (keyRanges.size() == 1) {
      return scan(keyRanges.iterator().next(), limit);
    }

    FilterByRangeIterator filterIterator =
        new FilterByRangeIterator(getPrefixRangeScannerIterator(keyRanges), keyRanges);
    return new LimitIterator(filterIterator, limit);
  }

  /*
   * Merge the longest prefix ranges and scan one by one
   * */
  private AbstractIterator<ScannerIterator> getPrefixRangeScannerIterator(
      Collection<Range> keyRanges) {
    Deque<ImmutablePair<byte[], byte[]>> scanKeys = new LinkedList<>();
    // Sort the scan keys by the start key and merge overlapping ranges.
    keyRanges.stream()
        .map(this::getLongestPrefixRange)
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
    return new AbstractIterator<ScannerIterator>() {
      @Override
      protected ScannerIterator computeNext() {
        if (!rangeIterator.hasNext()) {
          return endOfData();
        }
        ImmutablePair<byte[], byte[]> range = rangeIterator.next();
        return new ScannerIterator(table.scan(range.getFirst(), range.getSecond()), schema);
      }
    };
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Field<?> index) throws InvalidFieldException {
    LOG.trace("Table {}: Scan index {}", schema.getTableId(), index);
    fieldValidator.validateField(index);
    if (!schema.isIndexColumn(index.getName())) {
      throw new InvalidFieldException(schema.getTableId(), index.getName(),
          "is not an indexed column");
    }
    Scanner scanner = table.readByIndex(
        convertColumnsToBytes(Collections.singleton(index.getName()))[0],
        fieldToBytes(index));
    return new ScannerIterator(scanner, schema);
  }

  @Override
  public boolean compareAndSwap(Collection<Field<?>> keys, Field<?> oldValue, Field<?> newValue) {
    LOG.trace("Table {}: CompareAndSwap with keys {}, oldValue {}, newValue {}",
        schema.getTableId(), keys,
        oldValue, newValue);
    fieldValidator.validateField(oldValue);
    if (oldValue.getFieldType() != newValue.getFieldType()) {
      throw new IllegalArgumentException(
          String.format("Field types of oldValue (%s) and newValue (%s) are not the same",
              oldValue.getFieldType(), newValue.getFieldType()));
    }
    if (!oldValue.getName().equals(newValue.getName())) {
      throw new IllegalArgumentException(
          String.format(
              "Trying to compare and swap different fields. Old Value = %s, New Value = %s",
              oldValue, newValue));
    }
    if (schema.isPrimaryKeyColumn(oldValue.getName())) {
      throw new IllegalArgumentException("Cannot use compare and swap on a primary key field");
    }

    return table.compareAndSwap(convertKeyToBytes(keys, false), Bytes.toBytes(oldValue.getName()),
        fieldToBytes(oldValue), fieldToBytes(newValue));
  }

  @Override
  public void increment(Collection<Field<?>> keys, String column, long amount) {
    LOG.trace("Table {}: Increment with keys {}, column {}, amount {}", schema.getTableId(), keys,
        column, amount);
    FieldType.Type colType = schema.getType(column);
    if (colType == null) {
      throw new InvalidFieldException(schema.getTableId(), column);
    } else if (colType != FieldType.Type.LONG) {
      throw new IllegalArgumentException(
          String.format(
              "Trying to increment a column of type %s. Only %s column type can be incremented",
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
    try (CloseableIterator<StructuredRow> iterator = getFilterByRangeIterator(keyRange)) {
      while (iterator.hasNext()) {
        table.delete(convertKeyToBytes(iterator.next().getPrimaryKeys(), false));
      }
    }
  }

  @Override
  public void updateAll(Range keyRange, Collection<Field<?>> fields) throws InvalidFieldException {
    LOG.trace("Table {}: Update fields {} in range {}", schema.getTableId(), fields, keyRange);
    // validate that the range is strictly a primary key prefix
    fieldValidator.validatePrimaryKeys(keyRange.getBegin(), true);
    fieldValidator.validatePrimaryKeys(keyRange.getEnd(), true);
    // validate that we cannot update the primary key
    fieldValidator.validateNotPrimaryKeys(fields);
    try (CloseableIterator<StructuredRow> iterator = getFilterByRangeIterator(keyRange)) {
      while (iterator.hasNext()) {
        Collection<Field<?>> primaryKeys = iterator.next().getPrimaryKeys();
        List<Field<?>> fieldsToUpdate = Stream.concat(primaryKeys.stream(), fields.stream())
            .collect(Collectors.toList());
        table.put(convertFieldsToBytes(fieldsToUpdate));
      }
    }
  }

  @Override
  public long count(Collection<Range> keyRanges) throws IOException {
    LOG.trace("Table {}: count with ranges {}", schema.getTableId(), keyRanges);
    long count = 0;
    // Instead of scanning ranges one by one, we call multiScan,
    // which can deduplicate and sort the result
    try (CloseableIterator<StructuredRow> iterator = multiScan(keyRanges, Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        iterator.next();
        count++;
      }
    }
    return count;
  }

  @Override
  public long count(Collection<Range> keyRanges,
      Collection<Field<?>> filterIndexes) throws IOException {

    filterIndexes.forEach(fieldValidator::validateField);
    if (!schema.isIndexColumns(
        filterIndexes.stream().map(Field::getName).collect(Collectors.toList()))) {
      throw new InvalidFieldException(schema.getTableId(), filterIndexes,
          "are not all indexed columns");
    }

    LOG.trace("Table {}: count with ranges {}", schema.getTableId(), keyRanges);
    long count = 0;
    // Instead of scanning ranges one by one, we call multiScan,
    // which can deduplicate and sort the result
    try (CloseableIterator<StructuredRow> iterator = multiScan(keyRanges, Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        for (Field<?> field : filterIndexes) {
          boolean match = fieldMatch(field, row);
          if (match) {
            count++;
            break;
          }
        }
      }
    }
    return count;
  }

  private boolean fieldMatch(Field<?> filterField, StructuredRow row) {
    switch (filterField.getFieldType()) {
      case INTEGER:
        return Objects.equals(row.getInteger(filterField.getName()),
            filterField.getValue());
      case LONG:
        return Objects.equals(row.getLong(filterField.getName()), filterField.getValue());
      case FLOAT:
        return Objects.equals(row.getFloat(filterField.getName()), filterField.getValue());
      case DOUBLE:
        return Objects.equals(row.getDouble(filterField.getName()),
            filterField.getValue());
      case STRING:
        return Objects.equals(row.getString(filterField.getName()),
            filterField.getValue());
      case BYTES:
        return Arrays.equals(row.getBytes(filterField.getName()),
            (byte[]) filterField.getValue());
      case BOOLEAN:
        return Objects.equals(row.getBoolean(filterField.getName()),
            filterField.getValue());
      default:
        throw new InvalidFieldException(schema.getTableId(), filterField.getName());
    }
  }

  @Override
  public void close() throws IOException {
    table.close();
  }

  /**
   * Convert the keys to corresponding byte array. The keys can either be a prefix or complete
   * primary keys depending on the value of allowPrefix. The method will always prepend the table
   * name as a prefix for the row keys.
   *
   * @param keys keys to convert
   * @param allowPrefix true if the keys can be prefixed false if the keys have to contain all
   *     the primary keys.
   * @return the byte array converted
   * @throws InvalidFieldException if the key are not prefix or complete primary keys
   */
  private byte[] convertKeyToBytes(Collection<Field<?>> keys, boolean allowPrefix)
      throws InvalidFieldException {
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
   * Convert the fields to a {@link Put} to write to table. The primary key must all be provided.
   * The method will add the table name as prefix to the row key.
   *
   * @param fields the fields to write
   * @return a PUT object
   * @throws InvalidFieldException if primary keys are missing or the column is not in schema
   */
  private Put convertFieldsToBytes(Collection<Field<?>> fields) throws InvalidFieldException {
    Set<String> fieldNames = fields.stream().map(Field::getName).collect(Collectors.toSet());
    if (!fieldNames.containsAll(schema.getPrimaryKeys())) {
      throw new InvalidFieldException(schema.getTableId(), fields,
          String.format("Given fields %s does not contain all the "
              + "primary keys %s", fieldNames, schema.getPrimaryKeys()));
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

  /**
   * Updates fields in the row and converts to a {@link Put} to write to table. The primary key must
   * be provided.
   *
   * @param fields the fields to update
   * @return a PUT object
   * @throws InvalidFieldException if primary keys are missing or the column is not in schema
   */
  private Put updateFieldsToBytes(Collection<Field<?>> fields) throws InvalidFieldException {
    Set<String> fieldNames = fields.stream().map(Field::getName).collect(Collectors.toSet());
    if (!fieldNames.containsAll(schema.getPrimaryKeys())) {
      throw new InvalidFieldException(schema.getTableId(), fields,
          String.format("Given fields %s does not contain all the "
              + "primary keys %s", fieldNames, schema.getPrimaryKeys()));
    }
    Set<String> primaryKeys = new HashSet<>(schema.getPrimaryKeys());
    Collection<Field<?>> keyFields = fields.stream()
        .filter(field -> primaryKeys.contains(field.getName())).collect(Collectors.toList());
    byte[] primaryKey = convertKeyToBytes(keyFields, false);
    Put put = new Put(primaryKey);
    Row row = table.get(primaryKey);

    Map<String, Field<?>> fieldMap = fields.stream()
        .collect(Collectors.toMap(Field::getName, Function.identity()));
    for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
      String columnName = Bytes.toString(entry.getKey());
      if (!fieldMap.containsKey(columnName) || primaryKeys.contains(columnName)) {
        put.add(entry.getKey(), entry.getValue());
      } else {
        put.add(entry.getKey(), fieldToBytes(fieldMap.get(columnName)));
      }
    }
    return put;
  }

  private void addKey(MDSKey.Builder key, Field<?> field, FieldType.Type type)
      throws InvalidFieldException {
    if (field.getValue() == null) {
      throw new InvalidFieldException(schema.getTableId(), field.getName(),
          "is a primary key and value is null");
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
      case BOOLEAN:
        key.add((Boolean) field.getValue());
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
      case BOOLEAN:
        return Bytes.toBytes((Boolean) field.getValue());
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
      this.currentScanner =
          scannerIterator.hasNext() ? scannerIterator.next() : CloseableIterator.empty();
    }

    LimitIterator(CloseableIterator<StructuredRow> scannerIterator, int limit) {
      this(Collections.singleton(scannerIterator).iterator(), limit);
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
   * Filters elements matching a field {@link ScannerIterator}.
   */
  @VisibleForTesting
  static final class FilterByFieldIterator extends AbstractCloseableIterator<StructuredRow> {

    private final CloseableIterator<StructuredRow> scannerIterator;
    private final StructuredTableSchema schema;
    private final Collection<Predicate<StructuredRow>> predicates;

    FilterByFieldIterator(CloseableIterator<StructuredRow> scannerIterator,
        Collection<Field<?>> filterFields,
        StructuredTableSchema schema) {
      this.scannerIterator = scannerIterator;
      this.predicates = filterFields.stream().map(this::buildPredicate)
          .collect(Collectors.toList());
      this.schema = schema;
    }

    private Predicate<StructuredRow> buildPredicate(Field<?> filterField) {
      switch (filterField.getFieldType()) {
        case INTEGER:
          return row -> Objects.equals(row.getInteger(filterField.getName()),
              filterField.getValue());
        case LONG:
          return row -> Objects.equals(row.getLong(filterField.getName()), filterField.getValue());
        case FLOAT:
          return row -> Objects.equals(row.getFloat(filterField.getName()), filterField.getValue());
        case DOUBLE:
          return row -> Objects.equals(row.getDouble(filterField.getName()),
              filterField.getValue());
        case STRING:
          return row -> Objects.equals(row.getString(filterField.getName()),
              filterField.getValue());
        case BYTES:
          return row -> Arrays.equals(row.getBytes(filterField.getName()),
              (byte[]) filterField.getValue());
        case BOOLEAN:
          return row -> Objects.equals(row.getBoolean(filterField.getName()),
              filterField.getValue());
        default:
          throw new InvalidFieldException(schema.getTableId(), filterField.getName());
      }
    }

    @Override
    protected StructuredRow computeNext() {
      // Post filtering on scanned rows by specified field
      while (scannerIterator.hasNext()) {
        StructuredRow row = scannerIterator.next();
        for (Predicate<StructuredRow> predicate : predicates) {
          if (predicate.test(row)) {
            return row;
          }
        }
      }
      return endOfData();
    }

    @Override
    public void close() {
      scannerIterator.close();
    }
  }

  /**
   * Filters elements matching a range {@link ScannerIterator}.
   */
  @VisibleForTesting
  static final class FilterByRangeIterator extends AbstractCloseableIterator<StructuredRow> {

    private final Iterator<? extends CloseableIterator<StructuredRow>> scannerIterator;
    private CloseableIterator<StructuredRow> currentScanner;
    private final Collection<Predicate<StructuredRow>> predicates;

    FilterByRangeIterator(Iterator<? extends CloseableIterator<StructuredRow>> scannerIterator,
        Collection<Range> filterRanges) {
      this.scannerIterator = scannerIterator;
      this.currentScanner =
          scannerIterator.hasNext() ? scannerIterator.next() : CloseableIterator.empty();
      this.predicates = filterRanges.stream().map(this::buildPredicate)
          .collect(Collectors.toList());
    }

    private Predicate<StructuredRow> buildPredicate(Range filterRange) {
      return row -> {
        int beginIndex = 1;
        int beginFieldsCount = filterRange.getBegin().size();
        for (Field<?> beginField : filterRange.getBegin()) {
          int comp = compareRowAndField(row, beginField);
          // comp should be at least >= 0
          if (comp < 0) {
            return false;
          }
          // Edge case for = when we filter by the last field
          // This is for case like ([KEY: "ns1", KEY2: 123], EXCLUSIVE, [KEY: "ns1", KEY2: 250], INCLUSIVE)
          // We need to check the ending bound for KEY2, not KEY
          if (beginIndex == beginFieldsCount
              && filterRange.getBeginBound().equals(Range.Bound.EXCLUSIVE)
              && comp == 0) {
            return false;
          }
          beginIndex++;
        }

        int endIndex = 1;
        int endFieldsCount = filterRange.getEnd().size();
        for (Field<?> endField : filterRange.getEnd()) {
          int comp = compareRowAndField(row, endField);
          // comp should be at least <= 0
          if (comp > 0) {
            return false;
          }
          // Edge case for = when we filter by the last field
          // This is for case like ([KEY: "ns1", KEY2: 123], INCLUSIVE, [KEY: "ns1", KEY2: 250], EXCLUSIVE)
          // We need to check the ending bound for KEY2, not KEY
          if (endIndex == endFieldsCount
              && filterRange.getEndBound().equals(Range.Bound.EXCLUSIVE)
              && comp == 0) {
            return false;
          }
          endIndex++;
        }

        return true;
      };
    }

    private int compareRowAndField(StructuredRow row, Field<?> field) {
      String fieldName = field.getName();
      switch (field.getFieldType()) {
        case INTEGER:
          return Objects.compare(row.getInteger(fieldName), (Integer) field.getValue(),
              Integer::compare);
        case LONG:
          return Objects.compare(row.getLong(fieldName), (Long) (field.getValue()), Long::compare);
        case FLOAT:
          return Objects.compare(row.getFloat(fieldName), (Float) (field.getValue()),
              Float::compare);
        case DOUBLE:
          return Objects.compare(row.getDouble(fieldName), (Double) (field.getValue()),
              Double::compare);
        case STRING:
          return Objects.compare(row.getString(fieldName), (String) (field.getValue()),
              String::compareTo);
        case BYTES:
          return new Bytes.ByteArrayComparator().compare(row.getBytes(fieldName),
              (byte[]) field.getValue());
        case BOOLEAN:
          return Objects.compare(row.getBoolean(fieldName), (Boolean) (field.getValue()),
              Boolean::compareTo);
        default:
          throw new RuntimeException(
              String.format("Exception while comparing row: %s and field: %s", row, field));
      }
    }

    @Override
    protected StructuredRow computeNext() {
      // Find the next Scanner that is not iterated
      while (!currentScanner.hasNext() && scannerIterator.hasNext()) {
        closeScanner();
        currentScanner = scannerIterator.next();
      }

      // The case that we iterate to the end of last scanner
      if (!currentScanner.hasNext()) {
        return endOfData();
      }

      // Iterator the current scanner, return when we find a matching element
      while (currentScanner.hasNext()) {
        StructuredRow row = currentScanner.next();
        for (Predicate<StructuredRow> predicate : predicates) {
          if (predicate.test(row)) {
            return row;
          }
        }
      }

      // Recursion call
      // Done with the current scanner, got to next scanner and repeat
      return computeNext();
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
