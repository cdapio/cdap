/*
 * Copyright © 2021 Cask Data, Inc.
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
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.Value;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.FieldValidator;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A {@link StructuredTable} implementation backed by GCP Cloud Spanner.
 */
public class SpannerStructuredTable implements StructuredTable {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerStructuredTable.class);

  private final TransactionContext transactionContext;
  private final StructuredTableSchema schema;
  private final FieldValidator fieldValidator;

  public SpannerStructuredTable(TransactionContext transactionContext, StructuredTableSchema schema) {
    this.transactionContext = transactionContext;
    this.schema = schema;
    this.fieldValidator = new FieldValidator(schema);
  }

  @Override
  public void upsert(Collection<Field<?>> fields) throws InvalidFieldException {
    Map<String, Field<?>> fieldMap = fields.stream().collect(Collectors.toMap(Field::getName, Function.identity()));
    List<Field<?>> primaryKeyFields = new ArrayList<>();

    for (String key : schema.getPrimaryKeys()) {
      Field<?> field = fieldMap.get(key);
      if (field == null) {
        throw new InvalidFieldException(schema.getTableId(), key, "Missing primary key field " + key);
      }
      primaryKeyFields.add(field);
    }

    Optional<StructuredRow> row = read(primaryKeyFields, Collections.singleton(primaryKeyFields.get(0).getName()));
    if (row.isPresent()) {
      update(fields);
    } else {
      insert(fields);
    }
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
      throw new IllegalArgumentException("Some columns do not exists in the table schema " + missingColumns);
    }

    Struct row = transactionContext.readRow(schema.getTableId().getName(), createKey(keys), columns);
    return Optional.ofNullable(row).map(r -> new SpannerStructuredRow(schema, r));
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit) throws InvalidFieldException {
    fieldValidator.validatePrimaryKeys(keyRange.getBegin(), true);
    fieldValidator.validatePrimaryKeys(keyRange.getEnd(), true);

    Map<String, Value> parameters = new HashMap<>();
    String condition = getRangeWhereClause(keyRange, parameters);

    Statement.Builder builder = Statement.newBuilder(
      "SELECT * FROM " + escapeName(schema.getTableId().getName())
       + (condition.isEmpty() ? "" : " WHERE " + condition)
       + " ORDER BY " + schema.getPrimaryKeys().stream().map(this::escapeName).collect(Collectors.joining(","))
       + " LIMIT " + limit);
    parameters.forEach((name, value) -> builder.bind(name).to(value));
    Statement statement = builder.build();
    return new ResultSetIterator(schema, transactionContext.executeQuery(statement));
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Field<?> index) throws InvalidFieldException {
    fieldValidator.validateField(index);
    if (!schema.isIndexColumn(index.getName())) {
      throw new InvalidFieldException(schema.getTableId(), index.getName(), "is not an indexed column");
    }

    KeySet keySet = KeySet.singleKey(createKey(Collections.singleton(index)));
    String indexName = SpannerStructuredTableAdmin.getIndexName(schema.getTableId(), index.getName());
    ResultSet resultSet = transactionContext.readUsingIndex(schema.getTableId().getName(), indexName,
                                                            keySet, schema.getFieldNames());
    return new ResultSetIterator(schema, resultSet);
  }

  @Override
  public CloseableIterator<StructuredRow> multiScan(Collection<Range> keyRanges,
                                                    int limit) throws InvalidFieldException {
    // If nothing to scan, just return
    if (keyRanges.isEmpty()) {
      return CloseableIterator.empty();
    }

    Map<String, Value> parameters = new HashMap<>();
    Statement.Builder builder = Statement.newBuilder(
      "SELECT * FROM " + escapeName(schema.getTableId().getName())
       + " WHERE " + getRangesWhereClause(keyRanges, parameters)
       + " ORDER BY " + schema.getPrimaryKeys().stream().map(this::escapeName).collect(Collectors.joining(","))
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
        String.format("Trying to compare and swap different fields. Old Value = %s, New Value = %s",
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
      throw new InvalidFieldException(schema.getTableId(), column, "Column " + column + " does not exist");
    }
    if (type != FieldType.Type.LONG) {
      throw new IllegalArgumentException(
        String.format("Trying to increment a column of type %s. Only %s column type can be incremented",
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
      + keys.stream().map(f -> escapeName(f.getName()) + " = @" + f.getName()).collect(Collectors.joining(" AND "));

    Statement statement = keys.stream()
      .reduce(Statement.newBuilder(sql),
              (builder, field) -> builder.bind(field.getName()).to(getValue(field)),
              (builder1, builder2) -> builder1)
      .build();

    transactionContext.executeUpdate(statement);
  }

  @Override
  public void deleteAll(Range range) throws InvalidFieldException {
    fieldValidator.validatePrimaryKeys(range.getBegin(), true);
    fieldValidator.validatePrimaryKeys(range.getEnd(), true);

    Map<String, Value> parameters = new HashMap<>();
    String condition = getRangeWhereClause(range, parameters);

    Statement.Builder builder = Statement.newBuilder("DELETE FROM " + escapeName(schema.getTableId().getName())
                                                       + " WHERE " + (condition.isEmpty() ? "true" : condition));
    parameters.forEach((name, value) -> builder.bind(name).to(value));
    transactionContext.executeUpdate(builder.build());
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
    boolean allRange = ranges.stream().anyMatch(r -> r.getBegin().isEmpty() && r.getEnd().isEmpty());
    if (allRange) {
      return Statement.of("SELECT COUNT(*) FROM " + escapeName(schema.getTableId().getName()));
    }

    Map<String, Value> parameters = new HashMap<>();
    Statement.Builder builder = Statement.newBuilder("SELECT COUNT(*) FROM " + escapeName(schema.getTableId().getName())
                                                       + " WHERE " + getRangesWhereClause(ranges, parameters));
    parameters.forEach((name, value) -> builder.bind(name).to(value));
    return builder.build();
  }

  private String getRangeWhereClause(Range range, Map<String, Value> parameters) {
    List<String> conditions = new ArrayList<>();
    int paramIndex = parameters.size();

    for (Field<?> field : range.getBegin()) {
      conditions.add(escapeName(field.getName())
                       + (range.getBeginBound() == Range.Bound.INCLUSIVE ? " >= " : " > ") + "@p_" + paramIndex);
      parameters.put("p_" + paramIndex, getValue(field));
      paramIndex++;
    }

    for (Field<?> field : range.getEnd()) {
      conditions.add(escapeName(field.getName())
                       + (range.getEndBound() == Range.Bound.INCLUSIVE ? " <= " : " < ") + "@p_" + paramIndex);
      parameters.put("p_" + paramIndex, getValue(field));
      paramIndex++;
    }

    return String.join(" AND ", conditions);
  }

  private String getRangesWhereClause(Collection<Range> ranges, Map<String, Value> parameters) {
    List<String> rangeConditions = new ArrayList<>();

    for (Range range : ranges) {
      fieldValidator.validatePrimaryKeys(range.getBegin(), true);
      fieldValidator.validatePrimaryKeys(range.getEnd(), true);

      rangeConditions.add(getRangeWhereClause(range, parameters));
    }
    return rangeConditions.stream().collect(Collectors.joining(") OR (", "(", ")"));
  }

  void insert(Collection<Field<?>> fields) throws InvalidFieldException {
    List<Field<?>> insertFields = new ArrayList<>();
    for (Field<?> field : fields) {
      fieldValidator.validateField(field);
      insertFields.add(field);
    }

    String sql = "INSERT INTO " + escapeName(schema.getTableId().getName()) + " ("
      + insertFields.stream().map(Field::getName).map(this::escapeName).collect(Collectors.joining(",")) + ") VALUES ("
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

  void update(Collection<Field<?>> fields) throws InvalidFieldException {
    List<Field<?>> primaryKeyFields = new ArrayList<>();
    List<Field<?>> updateFields = new ArrayList<>();

    for (Field<?> field : fields) {
      fieldValidator.validateField(field);
      if (schema.isPrimaryKeyColumn(field.getName())) {
        primaryKeyFields.add(field);
      } else {
        updateFields.add(field);
      }
    }

    String sql = "UPDATE " + escapeName(schema.getTableId().getName())
      + " SET " + updateFields.stream().map(this::fieldToParam).collect(Collectors.joining(", "))
      + " WHERE " + primaryKeyFields.stream().map(this::fieldToParam).collect(Collectors.joining(" AND "));

    LOG.trace("Updating row: {}", sql);

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

  @Nullable
  private Value getValue(Field<?> field) {
    Object value = field.getValue();
    if (value == null) {
      return null;
    }

    switch (field.getFieldType()) {
      case INTEGER:
        return Value.int64((int) value);
      case LONG:
        return Value.int64((long) value);
      case FLOAT:
        return Value.float64((float) value);
      case DOUBLE:
        return Value.float64((double) value);
      case STRING:
        return Value.string((String) value);
      case BYTES:
        return Value.bytes(ByteArray.copyFrom((byte[]) value));
    }

    // This shouldn't happen
    throw new IllegalArgumentException("Unsupported field type " + field.getFieldType());
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
    }
    return false;
  }

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
