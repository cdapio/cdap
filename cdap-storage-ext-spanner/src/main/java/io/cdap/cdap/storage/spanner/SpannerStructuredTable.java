/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.storage.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.Value;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.FieldValidator;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link StructuredTable} implementation backed by GCP Cloud Spanner.
 */
public class SpannerStructuredTable implements StructuredTable {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerStructuredTable.class);

  private final TransactionContext transactionContext;
  private final StructuredTableSchema schema;
  private final FieldValidator fieldValidator;

  public SpannerStructuredTable(TransactionContext transactionContext,
      StructuredTableSchema schema) {
    this.transactionContext = transactionContext;
    this.schema = schema;
    this.fieldValidator = new FieldValidator(schema);
  }

  @Override
  public void upsert(Collection<Field<?>> fields) throws InvalidFieldException {
    Map<String, Field<?>> fieldMap = fields.stream()
        .collect(Collectors.toMap(Field::getName, Function.identity()));
    List<Field<?>> primaryKeyFields = new ArrayList<>();

    for (String key : schema.getPrimaryKeys()) {
      Field<?> field = fieldMap.get(key);
      if (field == null) {
        throw new InvalidFieldException(schema.getTableId(), key,
            "Missing primary key field " + key);
      }
      primaryKeyFields.add(field);
    }

    // Cloud Spanner doesn't support upsert. The best we can do is to read the existing row and update it if it exists
    // in the same transaction.
    Optional<StructuredRow> row = read(primaryKeyFields,
        Collections.singleton(primaryKeyFields.get(0).getName()));
    if (row.isPresent()) {
      update(fields);
    } else {
      insert(fields);
    }
  }

  @Override
  public void update(Collection<Field<?>> fields) throws InvalidFieldException {
    List<Field<?>> primaryKeyFields = new ArrayList<>();
    List<Field<?>> updateFields = new ArrayList<>();
    Set<String> fieldNames = new HashSet<>();

    for (Field<?> field : fields) {
      fieldValidator.validateField(field);
      if (schema.isPrimaryKeyColumn(field.getName())) {
        primaryKeyFields.add(field);
      } else {
        updateFields.add(field);
      }
      fieldNames.add(field.getName());
    }

    if (!fieldNames.containsAll(schema.getPrimaryKeys())) {
      throw new InvalidFieldException(schema.getTableId(), fields,
          String.format("Given fields %s do not contain all the "
              + "primary keys %s", fieldNames, schema.getPrimaryKeys()));
    }

    String sql = "UPDATE " + escapeName(schema.getTableId().getName())
        + " SET " + updateFields.stream().map(this::fieldToParam).collect(Collectors.joining(", "))
        + " WHERE " + primaryKeyFields.stream().map(this::fieldToParam)
        .collect(Collectors.joining(" AND "));

    LOG.trace("Updating row: {}", sql);

    Statement statement = fields.stream()
        .reduce(Statement.newBuilder(sql),
            (builder, field) -> builder.bind(field.getName()).to(getValue(field)),
            (builder1, builder2) -> builder1)
        .build();

    transactionContext.executeUpdate(statement);
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys) throws InvalidFieldException {
    return read(keys, schema.getFieldNames());
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys,
      Collection<String> columns) throws InvalidFieldException {
    if (columns.isEmpty()) {
      throw new IllegalArgumentException("No column is specified to read");
    }
    fieldValidator.validatePrimaryKeys(keys, false);

    Set<String> missingColumns = columns.stream()
        .filter(f -> !schema.getFieldNames().contains(f))
        .collect(Collectors.toSet());

    if (!missingColumns.isEmpty()) {
      throw new IllegalArgumentException(
          "Some columns do not exists in the table schema " + missingColumns);
    }

    // Adds all the keys to the result column as well. This is mirroring the PostgreSQL implementation.
    Set<String> queryColumns = keys.stream().map(Field::getName).collect(Collectors.toSet());
    queryColumns.addAll(columns);

    Struct row = transactionContext.readRow(schema.getTableId().getName(), createKey(keys),
        queryColumns);
    return Optional.ofNullable(row).map(r -> new SpannerStructuredRow(schema, r));
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit)
      throws InvalidFieldException {
    if (!isPrefixedPrimaryKeyRange(keyRange)) {
      // Spanner KeySet is requiring prefixed primary keys, instead we use SQL statement
      return multiScan(Collections.singleton(keyRange), limit);
    }

    KeySet keySet;
    if (keyRange.getBegin().isEmpty() && keyRange.getEnd().isEmpty()) {
      keySet = KeySet.all();
    } else {
      keySet = KeySet.range(KeyRange.newBuilder()
          .setStart(getKey(keyRange.getBegin()))
          .setStartType(getEndpoint(keyRange.getBeginBound()))
          .setEnd(getKey(keyRange.getEnd()))
          .setEndType(getEndpoint(keyRange.getEndBound()))
          .build());
    }

    return new ResultSetIterator(schema, transactionContext.read(schema.getTableId().getName(),
        keySet, schema.getFieldNames()));
  }

  private boolean isPrefixedPrimaryKeyRange(Range range) {
    try {
      fieldValidator.validatePrimaryKeys(range.getBegin(), true);
      fieldValidator.validatePrimaryKeys(range.getEnd(), true);
      return true;
    } catch (InvalidFieldException ex) {
      LOG.trace("Scanning a range that is not primary key prefixed: {}", range);
      return false;
    }
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Field<?> index) throws InvalidFieldException {
    fieldValidator.validateField(index);
    if (!schema.isIndexColumn(index.getName())) {
      throw new InvalidFieldException(schema.getTableId(), index.getName(),
          "is not an indexed column");
    }

    KeySet keySet = KeySet.singleKey(createKey(Collections.singleton(index)));
    String indexName = SpannerStructuredTableAdmin.getIndexName(schema.getTableId(),
        index.getName());
    ResultSet resultSet = transactionContext.readUsingIndex(schema.getTableId().getName(),
        indexName,
        keySet, schema.getFieldNames());
    return new ResultSetIterator(schema, resultSet);
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit,
      Collection<Field<?>> filterIndexes)
      throws InvalidFieldException {
    fieldValidator.validateScanRange(keyRange);
    filterIndexes.forEach(fieldValidator::validateField);
    if (!schema.isIndexColumns(
        filterIndexes.stream().map(Field::getName).collect(Collectors.toList()))) {
      throw new InvalidFieldException(schema.getTableId(), filterIndexes,
          "are not all indexed columns");
    }

    Map<String, Value> parameters = new HashMap<>();
    String rangeClause = getRangeWhereClause(keyRange, parameters);
    String indexClause = getIndexesFilterClause(filterIndexes, parameters);

    Statement.Builder builder = Statement.newBuilder(
        "SELECT * FROM " + escapeName(schema.getTableId().getName())
            + " WHERE " + (rangeClause.isEmpty() ? "true" : rangeClause) + " AND " + indexClause
            + " ORDER BY " + schema.getPrimaryKeys().stream().map(this::escapeName)
            .collect(Collectors.joining(","))
            + " LIMIT " + limit);
    parameters.forEach((name, value) -> builder.bind(name).to(value));
    return new ResultSetIterator(schema, transactionContext.executeQuery(builder.build()));
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit, String orderByField,
      SortOrder sortOrder)
      throws InvalidFieldException {
    fieldValidator.validateScanRange(keyRange);
    if (!schema.isIndexColumn(orderByField) && !schema.isPrimaryKeyColumn(orderByField)) {
      throw new InvalidFieldException(schema.getTableId(), orderByField,
          "is not an indexed column or primary key");
    }

    Map<String, Value> parameters = new HashMap<>();
    String rangeClause = getRangeWhereClause(keyRange, parameters);

    Statement.Builder builder = Statement.newBuilder(
        "SELECT * FROM " + escapeName(schema.getTableId().getName())
            + " WHERE " + (rangeClause.isEmpty() ? "true" : rangeClause)
            + " ORDER BY " + (sortOrder == SortOrder.ASC ? orderByField : orderByField + " DESC")
            + " LIMIT " + limit);
    parameters.forEach((name, value) -> builder.bind(name).to(value));
    return new ResultSetIterator(schema, transactionContext.executeQuery(builder.build()));
  }

  @Override
  public CloseableIterator<StructuredRow> multiScan(Collection<Range> keyRanges,
      int limit) throws InvalidFieldException {
    // If nothing to scan, just return
    if (keyRanges.isEmpty()) {
      return CloseableIterator.empty();
    }

    Map<String, Value> parameters = new HashMap<>();
    String whereClause = getRangesWhereClause(keyRanges, parameters);
    if (whereClause == null) {
      return scan(Range.all(), limit);
    }

    Statement.Builder builder = Statement.newBuilder(
        "SELECT * FROM " + escapeName(schema.getTableId().getName())
            + " WHERE " + whereClause
            + " ORDER BY " + schema.getPrimaryKeys().stream().map(this::escapeName)
            .collect(Collectors.joining(","))
            + " LIMIT " + limit);
    parameters.forEach((name, value) -> builder.bind(name).to(value));
    return new ResultSetIterator(schema, transactionContext.executeQuery(builder.build()));
  }

  @Override
  public boolean compareAndSwap(Collection<Field<?>> keys, Field<?> oldValue,
      Field<?> newValue) throws InvalidFieldException, IllegalArgumentException {
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

    StructuredRow existing = read(keys, Collections.singleton(oldValue.getName())).orElse(null);

    // Check if the existing value is as expected in the oldValue
    if (!isFieldEquals(oldValue, existing)) {
      return false;
    }

    List<Field<?>> updateFields = new ArrayList<>(keys);
    updateFields.add(newValue);
    if (existing == null) {
      insert(updateFields);
    } else {
      update(updateFields);
    }
    return true;
  }

  @Override
  public void increment(Collection<Field<?>> keys,
      String column, long amount) throws InvalidFieldException, IllegalArgumentException {
    if (schema.isPrimaryKeyColumn(column)) {
      throw new IllegalArgumentException("Cannot use increment on a primary key field");
    }
    FieldType.Type type = schema.getType(column);
    if (type == null) {
      throw new InvalidFieldException(schema.getTableId(), column,
          "Column " + column + " does not exist");
    }
    if (type != FieldType.Type.LONG) {
      throw new IllegalArgumentException(
          String.format(
              "Trying to increment a column of type %s. Only %s column type can be incremented",
              type, FieldType.Type.LONG));
    }
    fieldValidator.validatePrimaryKeys(keys, false);

    StructuredRow existing = read(keys, Collections.singleton(column)).orElse(null);
    List<Field<?>> fields = new ArrayList<>(keys);
    fields.add(Fields.longField(column,
        amount + (existing == null ? 0L : Objects.requireNonNull(existing.getLong(column)))));

    if (existing == null) {
      // Insert a new row if there is no existing row
      insert(fields);
    } else {
      // Update the row by incrementing the amount
      update(fields);
    }
  }

  @Override
  public void delete(Collection<Field<?>> keys) throws InvalidFieldException {
    fieldValidator.validatePrimaryKeys(keys, false);
    String sql = "DELETE FROM " + escapeName(schema.getTableId().getName()) + " WHERE "
        + keys.stream().map(f -> escapeName(f.getName()) + " = @" + f.getName())
        .collect(Collectors.joining(" AND "));

    Statement statement = keys.stream()
        .reduce(Statement.newBuilder(sql),
            (builder, field) -> builder.bind(field.getName()).to(getValue(field)),
            (builder1, builder2) -> builder1)
        .build();

    transactionContext.executeUpdate(statement);
  }

  @Override
  public void deleteAll(Range range) throws InvalidFieldException {
    fieldValidator.validateScanRange(range);

    Map<String, Value> parameters = new HashMap<>();
    String condition = getRangeWhereClause(range, parameters);

    Statement.Builder builder = Statement.newBuilder(
        "DELETE FROM " + escapeName(schema.getTableId().getName())
            + " WHERE " + (condition.isEmpty() ? "true" : condition));
    parameters.forEach((name, value) -> builder.bind(name).to(value));
    transactionContext.executeUpdate(builder.build());
  }

  @Override
  public void updateAll(Range keyRange, Collection<Field<?>> fields) throws InvalidFieldException {
    // validate that the range is strictly a primary key prefix
    fieldValidator.validatePrimaryKeys(keyRange.getBegin(), true);
    fieldValidator.validatePrimaryKeys(keyRange.getEnd(), true);
    // validate that we cannot update the primary key
    fieldValidator.validateNotPrimaryKeys(fields);

    Map<String, Value> parameters = new HashMap<>();
    String condition = getRangeWhereClause(keyRange, parameters);

    String sql = "UPDATE " + escapeName(schema.getTableId().getName())
        + " SET " + fields.stream().map(this::fieldToParam).collect(Collectors.joining(", "))
        + " WHERE " + (condition.isEmpty() ? "true" : condition);

    LOG.trace("Updating rows: {}", sql);

    Statement.Builder stmtBuilder = fields.stream().reduce(Statement.newBuilder(sql),
        (builder, field) -> builder.bind(field.getName())
            .to(getValue(field)),
        (builder1, builder2) -> builder1);
    parameters.forEach((name, value) -> stmtBuilder.bind(name).to(value));
    transactionContext.executeUpdate(stmtBuilder.build());
  }

  @Override
  public long count(Collection<Range> keyRanges) {
    try (ResultSet resultSet = transactionContext.executeQuery(getCountStatement(keyRanges))) {
      if (!resultSet.next()) {
        return 0L;
      }
      return resultSet.getCurrentRowAsStruct().getLong(0);
    }
  }

  @Override
  public void close() {
    // No-op
  }

  private Statement getCountStatement(Collection<Range> ranges) {
    Map<String, Value> parameters = new HashMap<>();
    String whereClause = getRangesWhereClause(ranges, parameters);

    if (whereClause == null) {
      return Statement.of("SELECT COUNT(*) FROM " + escapeName(schema.getTableId().getName()));
    }

    Statement.Builder builder = Statement.newBuilder(
        "SELECT COUNT(*) FROM " + escapeName(schema.getTableId().getName())
            + " WHERE " + whereClause);
    parameters.forEach((name, value) -> builder.bind(name).to(value));
    return builder.build();
  }

  private String getRangeWhereClause(Range range, Map<String, Value> parameters) {
    List<String> conditions = new ArrayList<>();
    int paramIndex = parameters.size();

    int beginIndex = 1;
    int beginFieldsCount = range.getBegin().size();
    for (Field<?> field : range.getBegin()) {
      // Edge case for = when we filter by the last field
      // This is for case like ([KEY: "ns1", KEY2: 123], EXCLUSIVE, [KEY: "ns1", KEY2: 250], INCLUSIVE)
      // We need to check the ending bound for KEY2, not KEY
      String symbol =
          beginIndex == beginFieldsCount && range.getBeginBound().equals(Range.Bound.EXCLUSIVE)
              ? " > " : " >= ";
      conditions.add(escapeName(field.getName()) + symbol + "@p_" + paramIndex);
      parameters.put("p_" + paramIndex, getValue(field));
      paramIndex++;
      beginIndex++;
    }

    int endIndex = 1;
    int endFieldsCount = range.getEnd().size();
    for (Field<?> field : range.getEnd()) {
      // Edge case for = when we filter by the last field
      // This is for case like ([KEY: "ns1", KEY2: 123], INCLUSIVE, [KEY: "ns1", KEY2: 250], EXCLUSIVE)
      // We need to check the ending bound for KEY2, not KEY
      String symbol =
          endIndex == endFieldsCount && range.getEndBound().equals(Range.Bound.EXCLUSIVE) ? " < "
              : " <= ";
      conditions.add(escapeName(field.getName()) + symbol + "@p_" + paramIndex);
      parameters.put("p_" + paramIndex, getValue(field));
      paramIndex++;
      endIndex++;
    }

    return String.join(" AND ", conditions);
  }

  private String getFieldsWhereClause(Collection<Field<?>> fields, Map<String, Value> parameters) {
    List<String> conditions = new ArrayList<>();
    int paramIndex = parameters.size();

    for (Field<?> field : fields) {
      conditions.add(escapeName(field.getName()) + " = @p_" + paramIndex);
      parameters.put("p_" + paramIndex, getValue(field));
      paramIndex++;
    }

    return String.join(" AND ", conditions);
  }

  private String getIndexesFilterClause(Collection<Field<?>> filterIndexes,
      Map<String, Value> parameters) {
    List<String> conditions = new ArrayList<>();
    int paramIndex = parameters.size();

    for (Field<?> field : filterIndexes) {
      if (field.getValue() == null) {
        conditions.add(escapeName(field.getName()) + " is NULL");
      } else {
        conditions.add(escapeName(field.getName()) + " = @p_" + paramIndex);
        parameters.put("p_" + paramIndex, getValue(field));
        paramIndex++;
      }
    }

    return conditions.stream().collect(Collectors.joining(" OR ", "(", ")"));
  }

  /**
   * Generates the WHERE clause for a set of ranges.
   *
   * @param ranges the set of ranges to query for
   * @param parameters the parameters name and value used in the WHERE clause
   * @return the WHERE clause or {@code null} if the ranges resulted in a full table query.
   */
  @Nullable
  private String getRangesWhereClause(Collection<Range> ranges, Map<String, Value> parameters) {
    // Validate all ranges. Also, find if there is any range that is open on both ends.
    // Also split the scans into actual range scans and singleton scan, which can be done via equal condition.
    List<Range> singletonScans = new ArrayList<>();
    List<Range> rangeScans = new ArrayList<>();
    boolean scanAll = false;
    for (Range range : ranges) {
      fieldValidator.validateScanRange(range);

      if (range.isSingleton()) {
        singletonScans.add(range);
      } else {
        if (range.getBegin().isEmpty() && range.getEnd().isEmpty()) {
          // We continue the loop so that all keyRanges are still validated
          scanAll = true;
        }
        rangeScans.add(range);
      }
    }
    if (scanAll) {
      return null;
    }

    // Generates "(key1 = ? AND key2 = ?) OR (key1 = ? AND key2 = ?)..." clause
    StringBuilder query = new StringBuilder();
    String separator = "";
    for (Range singleton : singletonScans) {
      query.append(separator)
          .append("(")
          .append(getFieldsWhereClause(singleton.getBegin(), parameters))
          .append(")");
      separator = " OR ";
    }

    // Generates the ((key3 >= ?) AND (key3 <= ?)) OR ((key4 >= ?) AND (key4 <= ?))
    if (!rangeScans.isEmpty()) {
      separator = singletonScans.isEmpty() ? "(" : " OR (";
      for (Range range : rangeScans) {
        query.append(separator).append("(").append(getRangeWhereClause(range, parameters))
            .append(")");
        separator = " OR ";
      }
      query.append(")");
    }

    return query.toString();
  }

  private void insert(Collection<Field<?>> fields) throws InvalidFieldException {
    List<Field<?>> insertFields = new ArrayList<>();
    for (Field<?> field : fields) {
      fieldValidator.validateField(field);
      insertFields.add(field);
    }

    String sql = "INSERT INTO " + escapeName(schema.getTableId().getName()) + " ("
        + insertFields.stream().map(Field::getName).map(this::escapeName)
        .collect(Collectors.joining(",")) + ") VALUES ("
        + insertFields.stream().map(f -> "@" + f.getName()).collect(Collectors.joining(","))
        + ")";

    LOG.trace("Inserting row: {}", sql);

    Statement statement = fields.stream()
        .reduce(Statement.newBuilder(sql),
            (builder, field) -> builder.bind(field.getName()).to(getValue(field)),
            (builder1, builder2) -> builder1)
        .build();

    transactionContext.executeUpdate(statement);
  }

  private Key createKey(Collection<Field<?>> fields) {
    return Key.of(fields.stream().map(Field::getValue).toArray());
  }

  /**
   * Converts a {@link Field} into spanner {@link Value}.
   */
  private Value getValue(Field<?> field) {
    Object value = field.getValue();

    switch (field.getFieldType()) {
      case INTEGER:
        return Value.int64(value == null ? null : Long.valueOf((Integer) value));
      case LONG:
        return Value.int64((Long) value);
      case FLOAT:
        return Value.float64(value == null ? null : Double.valueOf((Float) value));
      case DOUBLE:
        return Value.float64((Double) value);
      case STRING:
        return Value.string((String) value);
      case BYTES:
        return Value.bytes(value == null ? null : (ByteArray.copyFrom((byte[]) value)));
      case BOOLEAN:
        return Value.bool((Boolean) value);
    }

    // This shouldn't happen
    throw new IllegalArgumentException("Unsupported field type " + field.getFieldType());
  }

  /**
   * Converts a {@link Field} into spanner {@link Key}.
   */
  private Key getKey(Collection<Field<?>> fields) {
    Key.Builder builder = Key.newBuilder();

    for (Field<?> field : fields) {
      Object value = field.getValue();
      if (value == null) {
        return Key.of((Object) null);
      }

      switch (field.getFieldType()) {
        case INTEGER:
          builder.append((int) value).build();
          break;
        case LONG:
          builder.append((long) value).build();
          break;
        case FLOAT:
          builder.append((float) value).build();
          break;
        case DOUBLE:
          builder.append((double) value).build();
          break;
        case STRING:
          builder.append((String) value).build();
          break;
        case BYTES:
          builder.append(ByteArray.copyFrom((byte[]) value)).build();
          break;
        case BOOLEAN:
          builder.append((Boolean) value).build();
          break;
        default:
          throw new IllegalArgumentException("Unsupported field type " + field.getFieldType());
      }
    }

    return builder.build();
  }

  private KeyRange.Endpoint getEndpoint(Range.Bound bound) {
    return bound == Range.Bound.INCLUSIVE ? KeyRange.Endpoint.CLOSED : KeyRange.Endpoint.OPEN;
  }

  private boolean isFieldEquals(Field<?> field, @Nullable StructuredRow row) {
    Object fieldValue = field.getValue();
    if (row == null) {
      return fieldValue == null;
    }

    switch (field.getFieldType()) {
      case INTEGER:
        return Objects.equals(fieldValue, row.getInteger(field.getName()));
      case LONG:
        return Objects.equals(fieldValue, row.getLong(field.getName()));
      case FLOAT:
        return Objects.equals(fieldValue, row.getFloat(field.getName()));
      case DOUBLE:
        return Objects.equals(fieldValue, row.getDouble(field.getName()));
      case STRING:
        return Objects.equals(fieldValue, row.getString(field.getName()));
      case BYTES:
        return Objects.deepEquals(fieldValue, row.getBytes(field.getName()));
      case BOOLEAN:
        return Objects.equals(fieldValue, row.getBoolean(field.getName()));
    }
    return false;
  }

  /**
   * Returns a {@link String} in the form of {@code [tableName.]fieldName = @fieldName}.
   */
  private String fieldToParam(Field<?> field) {
    String fieldName = field.getName();
    if (fieldName.equalsIgnoreCase(schema.getTableId().getName())) {
      fieldName = escapeName(schema.getTableId().getName()) + "." + escapeName(fieldName);
    } else {
      fieldName = escapeName(fieldName);
    }
    return fieldName + " = @" + field.getName();
  }

  private String escapeName(String name) {
    return "`" + name + "`";
  }

  /**
   * A {@link CloseableIterator} to iterate over a Spanner {@link ResultSet}.
   */
  private static final class ResultSetIterator extends AbstractCloseableIterator<StructuredRow> {

    private final StructuredTableSchema schema;
    private final ResultSet resultSet;

    ResultSetIterator(StructuredTableSchema schema, ResultSet resultSet) {
      this.schema = schema;
      this.resultSet = resultSet;
    }

    @Override
    protected StructuredRow computeNext() {
      if (!resultSet.next()) {
        return endOfData();
      }
      return new SpannerStructuredRow(schema, resultSet.getCurrentRowAsStruct());
    }

    @Override
    public void close() {
      resultSet.close();
    }
  }
}
