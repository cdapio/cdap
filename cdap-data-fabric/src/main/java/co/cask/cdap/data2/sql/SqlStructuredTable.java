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

package co.cask.cdap.data2.sql;

import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.spi.data.InvalidFieldException;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.table.TableSchema;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.FieldType;
import co.cask.cdap.spi.data.table.field.Range;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 *
 */
public class SqlStructuredTable implements StructuredTable {
  private static final Logger LOG = LoggerFactory.getLogger(SqlStructuredTable.class);

  private final Connection connection;
  private final TableSchema tableSchema;

  public SqlStructuredTable(Connection connection, TableSchema tableSchema) {
    this.connection = connection;
    this.tableSchema = tableSchema;
  }

  @Override
  public void write(Collection<Field<?>> fields) throws InvalidFieldException, IOException {
    validateSchema(fields);
    String sqlQuery = getWriteSqlQuery(fields);
    try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
      int index = 1;
      for (Field<?> field : fields) {
        FieldType.Type type = tableSchema.getType(field.getName());
        setField(statement, field, type, index);
        index++;
      }
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(String.format("Fail to write to table %s with fields %s",
                                          tableSchema.getTableId().getName(), fields), e);
    }
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys) throws InvalidFieldException, IOException {
    return read(keys, Collections.emptySet());
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys,
                                      Collection<String> columns) throws InvalidFieldException, IOException {
    validateSchema(keys);
    String readQuery = getReadQuery(keys, columns);
    try (PreparedStatement statement = connection.prepareStatement(readQuery);) {
      int index = 1;
      for (Field<?> key : keys) {
        setField(statement, key, tableSchema.getType(key.getName()), index);
        index++;
      }
      try (ResultSet resultSet = statement.executeQuery()) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int numCols = metaData.getColumnCount();
        if (resultSet.next()) {
          SqlStructuredRow row = new SqlStructuredRow(tableSchema);
          for (int i = 1; i <= numCols; i++) {
            row.add(metaData.getColumnName(i), resultSet.getObject(i));
          }
          return Optional.of(row);
        }
      }
    } catch (SQLException e) {
      throw new IOException(String.format("Fail to read from table %s with keys %s",
                                          tableSchema.getTableId().getName(), keys), e);
    }
    return Optional.empty();
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit) throws InvalidFieldException, IOException {
    if (keyRange.getBegin() != null) {
      validateSchema(keyRange.getBegin());
    }
    if (keyRange.getEnd() != null) {
      validateSchema(keyRange.getEnd());
    }
    String scanQuery = getScanQuery(keyRange, limit);
    try {
      PreparedStatement statement = connection.prepareStatement(scanQuery);
      int index = 1;
      if (keyRange.getBegin() != null) {
        for (Field<?> key : keyRange.getBegin()) {
          setField(statement, key, tableSchema.getType(key.getName()), index);
          index++;
        }
      }
      if (keyRange.getEnd() != null) {
        for (Field<?> key : keyRange.getEnd()) {
          setField(statement, key, tableSchema.getType(key.getName()), index);
          index++;
        }
      }
      ResultSet resultSet = statement.executeQuery();
      return new ResultSetIterator(statement, resultSet, tableSchema);
    } catch (SQLException e) {
      throw new IOException(String.format("Fail to scan from table %s with range %s",
                                          tableSchema.getTableId().getName(), keyRange), e);
    }
  }

  @Override
  public void delete(Collection<Field<?>> keys) throws InvalidFieldException, IOException {
    validateSchema(keys);
    String sqlQuery = getDeleteQuery(keys);
    try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
      int index = 1;
      for (Field<?> key : keys) {
        setField(statement, key, tableSchema.getType(key.getName()), index);
        index++;
      }
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(String.format("Fail to delete the row from table %s with fields %s",
                                          tableSchema.getTableId().getName(), keys), e);
    }
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }

  private void validateSchema(Collection<Field<?>> fields) throws InvalidFieldException {
    if (!fields.stream().map(Field::getName).collect(Collectors.toSet()).containsAll(tableSchema.getPrimaryKeys())) {
      throw new InvalidFieldException(
        String.format("Given fields do not contain all the primary key fields for table %s",
                      tableSchema.getTableId().getName()));
    }
    for (Field<?> field : fields) {
      FieldType.Type type = tableSchema.getType(field.getName());
      if (type == null) {
        throw new InvalidFieldException(tableSchema.getTableId(), field.getName());
      }
      if (tableSchema.isKey(field.getName()) && field.getValue() == null) {
        throw new InvalidFieldException(tableSchema.getTableId(), field.getName());
      }
      validateFieldValue(field, type);
    }
  }

  private void validateFieldValue(Field field, FieldType.Type expected) throws InvalidFieldException {
    Object value = field.getValue();
    if (value == null && tableSchema.isKey(field.getName())) {
      throw new InvalidFieldException(
        String.format("The primary key field %s cannot have a null value to write to table %s",
                      field.getName(), tableSchema.getTableId().getName()));
    }
    if (value instanceof Integer) {
      if (!expected.equals(FieldType.Type.INTEGER)) {
        throw new InvalidFieldException(tableSchema.getTableId(), field.getName(), expected, FieldType.Type.INTEGER);
      }
    } else if (value instanceof Long) {
      if (!expected.equals(FieldType.Type.LONG)) {
        throw new InvalidFieldException(tableSchema.getTableId(), field.getName(), expected, FieldType.Type.LONG);
      }
    } else if (value instanceof Float) {
      // the key can only be integer, long, string type
      if (tableSchema.isKey(field.getName()) || !expected.equals(FieldType.Type.FLOAT)) {
        throw new InvalidFieldException(tableSchema.getTableId(), field.getName(), expected, FieldType.Type.FLOAT);
      }
    } else if (value instanceof Double) {
      // the key can only be integer, long, string type
      if (tableSchema.isKey(field.getName()) || !expected.equals(FieldType.Type.DOUBLE)) {
        throw new InvalidFieldException(tableSchema.getTableId(), field.getName(), expected, FieldType.Type.DOUBLE);
      }
    } else if (value instanceof String) {
      if (!expected.equals(FieldType.Type.STRING)) {
        throw new InvalidFieldException(tableSchema.getTableId(), field.getName(), expected, FieldType.Type.STRING);
      }
    } else {
      throw new InvalidFieldException(tableSchema.getTableId(), field.getName(), "is not a valid field type");
    }
  }

  private void setField(PreparedStatement statement, Field field, FieldType.Type type,
                        int parameterIndex) throws SQLException {
    Object value = field.getValue();

    switch (type) {
      case INTEGER:
        if (value == null) {
          statement.setNull(parameterIndex, Types.INTEGER);
        } else {
          statement.setInt(parameterIndex, (int) value);
        }
        break;
      case LONG:
        if (value == null) {
          statement.setNull(parameterIndex, Types.BIGINT);
        } else {
          statement.setLong(parameterIndex, (long) value);
        }
        break;
      case FLOAT:
        if (value == null) {
          statement.setNull(parameterIndex, Types.FLOAT);
        } else {
          statement.setFloat(parameterIndex, (float) value);
        }
        break;
      case DOUBLE:
        if (value == null) {
          statement.setNull(parameterIndex, Types.DOUBLE);
        } else {
          statement.setDouble(parameterIndex, (double) value);
        }
        break;
      case STRING:
        if (value == null) {
          statement.setNull(parameterIndex, Types.VARCHAR);
        } else {
          statement.setString(parameterIndex, (String) value);
        }
        break;
      default:
        // this should not happen since we validate the field before set the field
        throw new InvalidFieldException(tableSchema.getTableId(), field.getName());
    }
  }

  private String getWriteSqlQuery(Collection<Field<?>> fields) {
    StringJoiner insertPart = new StringJoiner(",",
                                               "insert into " + tableSchema.getTableId().getName() + " (",
                                               ") ");
    StringJoiner valuePart = new StringJoiner(",", "values (", ") ");
    StringJoiner conflictPart = new StringJoiner(",", "on conflict (", ") ");
    StringJoiner updatePart = new StringJoiner(",",  "do update set ", ";");

    for (Field<?> field : fields) {
      insertPart.add(field.getName());
      valuePart.add("?");
      if (tableSchema.isKey(field.getName())) {
        conflictPart.add(field.getName());
      } else {
        updatePart.add(field.getName() + "=excluded." + field.getName());
      }
    }
    return insertPart.toString() + valuePart.toString() + conflictPart.toString() + updatePart.toString();
  }

  private String getReadQuery(Collection<Field<?>> keys, Collection<String> columns) {
    String prefix = "select " + (columns.isEmpty() ? "*" : Joiner.on(",").join(columns)) + " from " +
      tableSchema.getTableId().getName() + " where ";
    return combineWithEqualClause(prefix, keys);
  }

  private String getScanQuery(Range range, int limit) {
    StringBuilder prefix = new StringBuilder("select * from ").append(tableSchema.getTableId().getName());
    if (range.getBegin() == null && range.getEnd() == null) {
      return prefix.append(";").toString();
    }
    prefix.append(" where ");
    StringJoiner joiner = new StringJoiner(" and ", prefix, " limit " + limit + ";");
    if (range.getBegin() != null) {
      for (Field<?> key : range.getBegin()) {
        joiner.add(key.getName() + (range.getBeginBound().equals(Range.Bound.INCLUSIVE) ? ">=?" : ">?"));
      }
    }
    if (range.getEnd() != null) {
      for (Field<?> key : range.getEnd()) {
        joiner.add(key.getName() + (range.getEndBound().equals(Range.Bound.INCLUSIVE) ? "<=?" : "<?"));
      }
    }
    return joiner.toString();
  }

  private String getDeleteQuery(Collection<Field<?>> keys) {
    String prefix = "delete from " + tableSchema.getTableId().getName() + " where ";
    return combineWithEqualClause(prefix, keys);
  }

  private String combineWithEqualClause(String prefix, Collection<Field<?>> keys) {
    StringJoiner joiner = new StringJoiner(" and ", prefix, ";");
    for (Field<?> key : keys) {
      joiner.add(key.getName() + "=?");
    }
    return joiner.toString();
  }

  static final class ResultSetIterator extends AbstractCloseableIterator<StructuredRow> {
    private final Statement statement;
    private final ResultSet resultSet;
    private final Set<String> nameTypeMapping;
    private final TableSchema schema;


    ResultSetIterator(Statement statement, ResultSet resultSet, TableSchema schema) throws SQLException {
      this.statement = statement;
      this.resultSet = resultSet;
      this.nameTypeMapping = new HashSet<>();
      this.schema = schema;
      generateColNames();
    }

    @Override
    protected StructuredRow computeNext() {
      try {
        if (resultSet.next()) {
          SqlStructuredRow row = new SqlStructuredRow(schema);
          for (String colName : nameTypeMapping) {
            row.add(colName, resultSet.getObject(colName));
          }
          return row;
        }
      } catch (SQLException e) {
        throw new RuntimeException("Failed to get the next value from the sql result set", e);
      }
      return endOfData();
    }

    @Override
    public void close() {
      try {
        statement.close();
        resultSet.close();
      } catch (SQLException e) {
        LOG.warn("Failed to close the result set", e);
      }
    }

    private void generateColNames() throws SQLException {
      ResultSetMetaData metaData = resultSet.getMetaData();
      int numCols = metaData.getColumnCount();
      for (int i = 1; i <= numCols; i++) {
        nameTypeMapping.add(metaData.getColumnName(i));
      }
    }
  }
}
