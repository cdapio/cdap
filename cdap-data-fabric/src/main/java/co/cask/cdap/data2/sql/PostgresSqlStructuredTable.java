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

package co.cask.cdap.data2.sql;

import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.spi.data.InvalidFieldException;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.table.StructuredTableSchema;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.FieldType;
import co.cask.cdap.spi.data.table.field.FieldValidator;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Sql structured table implementation.
 */
public class PostgresSqlStructuredTable implements StructuredTable {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresSqlStructuredTable.class);

  private final Connection connection;
  private final StructuredTableSchema tableSchema;
  private final FieldValidator fieldValidator;

  public PostgresSqlStructuredTable(Connection connection, StructuredTableSchema tableSchema) {
    this.connection = connection;
    this.tableSchema = tableSchema;
    this.fieldValidator = new FieldValidator(tableSchema);
  }

  @Override
  public void upsert(Collection<Field<?>> fields) throws InvalidFieldException, IOException {
    LOG.trace("Table {}: Write fields {}", tableSchema.getTableId(), fields);
    Set<String> fieldNames = fields.stream().map(Field::getName).collect(Collectors.toSet());
    if (!fieldNames.containsAll(tableSchema.getPrimaryKeys())) {
      throw new InvalidFieldException(tableSchema.getTableId(), fieldNames,
                                      String.format("Given fields %s do not contain all the " +
                                                      "primary keys %s", fieldNames, tableSchema.getPrimaryKeys()));
    }
    String sqlQuery = getWriteSqlQuery(fields);
    try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
      int index = 1;
      for (Field<?> field : fields) {
        setField(statement, field, index);
        index++;
      }
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to write to table %s with fields %s",
                                          tableSchema.getTableId().getName(), fields), e);
    }
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys) throws InvalidFieldException, IOException {
    return readRow(keys, null);
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys,
                                      Collection<String> columns) throws InvalidFieldException, IOException {
    if (columns == null || columns.isEmpty()) {
      throw new InvalidFieldException(tableSchema.getTableId(), columns, "No columns are specified in reading");
    }

    // always have the primary key fields included in the columns
    Set<String> columnFields = new HashSet<>(columns);
    columnFields.addAll(keys.stream().map(Field::getName).collect(Collectors.toSet()));
    return readRow(keys, columnFields);
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit) throws InvalidFieldException, IOException {
    LOG.trace("Table {}: Scan range {} with limit {}", tableSchema.getTableId(), keyRange, limit);
    tableSchema.validatePrimaryKeys(keyRange.getBegin().stream().map(Field::getName).collect(Collectors.toList()),
                                    true);
    tableSchema.validatePrimaryKeys(keyRange.getEnd().stream().map(Field::getName).collect(Collectors.toList()),
                                    true);
    String scanQuery = getScanQuery(keyRange, limit);

    // We don't close the statement here because once it is closed, the result set is also closed.
    try {
      PreparedStatement statement = connection.prepareStatement(scanQuery);
      int index = 1;
      if (keyRange.getBegin() != null) {
        for (Field<?> key : keyRange.getBegin()) {
          setField(statement, key, index);
          index++;
        }
      }
      if (keyRange.getEnd() != null) {
        for (Field<?> key : keyRange.getEnd()) {
          setField(statement, key, index);
          index++;
        }
      }
      ResultSet resultSet = statement.executeQuery();
      return new ResultSetIterator(statement, resultSet, tableSchema);
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to scan from table %s with range %s",
                                          tableSchema.getTableId().getName(), keyRange), e);
    }
  }

  @Override
  public void delete(Collection<Field<?>> keys) throws InvalidFieldException, IOException {
    LOG.trace("Table {}: Delete with keys {}", tableSchema.getTableId(), keys);
    tableSchema.validatePrimaryKeys(keys.stream().map(Field::getName).collect(Collectors.toList()), false);
    String sqlQuery = getDeleteQuery(keys);
    try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
      int index = 1;
      for (Field<?> key : keys) {
        setField(statement, key, index);
        index++;
      }
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to delete the row from table %s with fields %s",
                                          tableSchema.getTableId().getName(), keys), e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      connection.close();
    } catch (SQLException e) {
      LOG.warn("Failed to close the SQL connection", e);
    }
  }

  /**
   * Read a row from the table. Null columns mean read from all columns.
   *
   * @param keys key of the row
   * @param columns columns to read, null means read from all
   * @return an optional containing the row or empty optional if the row does not exist
   */
  private Optional<StructuredRow> readRow(
    Collection<Field<?>> keys, @Nullable Collection<String> columns) throws InvalidFieldException, IOException {
    LOG.trace("Table {}: Read with keys {} and columns {}", tableSchema.getTableId(), keys, columns);
    tableSchema.validatePrimaryKeys(keys.stream().map(Field::getName).collect(Collectors.toList()), false);
    String readQuery = getReadQuery(keys, columns);
    try (PreparedStatement statement = connection.prepareStatement(readQuery);) {
      int index = 1;
      for (Field<?> key : keys) {
        setField(statement, key, index);
        index++;
      }
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          return Optional.empty();
        }

        ResultSetMetaData metaData = resultSet.getMetaData();
        int numCols = metaData.getColumnCount();
        Map<String, Object> row = new HashMap<>();
        for (int i = 1; i <= numCols; i++) {
          row.put(metaData.getColumnName(i), resultSet.getObject(i));
        }
        return Optional.of(new SqlStructuredRow(tableSchema, row));
      }
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to read from table %s with keys %s",
                                          tableSchema.getTableId().getName(), keys), e);
    }
  }

  private void setField(PreparedStatement statement, Field field,
                        int parameterIndex) throws SQLException, InvalidFieldException {
    fieldValidator.validateField(field);
    Object value = field.getValue();
    FieldType.Type type = tableSchema.getType(field.getName());
    if (type == null) {
      throw new InvalidFieldException(tableSchema.getTableId(), field.getName());
    }

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
      case BYTES:
        if (value == null) {
          statement.setNull(parameterIndex, Types.LONGVARBINARY);
        } else {
          statement.setBytes(parameterIndex, (byte[]) value);
        }
        break;
      default:
        // this should not happen since we validate the field before setting
        throw new InvalidFieldException(tableSchema.getTableId(), field.getName());
    }
  }

  /**
   * Get the write sql query for PreparedStatement for the fields given. For example, if "simpletable" has 5 columns,
   * (key1, key2, col1, col2, col3), this write query will generate the following query:
   * INSERT INTO simpletable (key1,key2,col1,col2,col3) VALUES (?,?,?,?,?) ON CONFLICT (key1,key2)
   * DO UPDATE SET col1=EXCLUDED.col1,col2=EXCLUDED.col2,col3=EXCLUDED.col3;
   *
   * @param fields fields to write
   * @return the sql query
   */
  private String getWriteSqlQuery(Collection<Field<?>> fields) {
    StringJoiner insertPart = new StringJoiner(",",
                                               "INSERT INTO " + tableSchema.getTableId().getName() + " (",
                                               ") ");
    StringJoiner valuePart = new StringJoiner(",", "VALUES (", ") ");
    StringJoiner conflictPart = new StringJoiner(",", "ON CONFLICT (", ") ");
    StringJoiner updatePart = new StringJoiner(",",  "DO UPDATE SET ", ";");

    for (Field<?> field : fields) {
      insertPart.add(field.getName());
      valuePart.add("?");
      if (tableSchema.isPrimaryKeyColumn(field.getName())) {
        conflictPart.add(field.getName());
      } else {
        updatePart.add(field.getName() + "=EXCLUDED." + field.getName());
      }
    }
    return insertPart.toString() + valuePart.toString() + conflictPart.toString() + updatePart.toString();
  }

  private String getReadQuery(Collection<Field<?>> keys, Collection<String> columns) {
    String prefix = "SELECT " + (columns == null || columns.isEmpty() ? "*" : Joiner.on(",").join(columns)) +
      " FROM " + tableSchema.getTableId().getName() + " WHERE ";
    return combineWithEqualClause(prefix, keys);
  }

  /**
   * Get the scan query for the range given. For example, if the range provides key1, key2 as the begin and end to
   * scan, both rows are inclusive, it will generate the following query:
   * SELECT * FROM simpletable WHERE (key1,key2)>=(?,?) AND (key1,key2)<=(?,?) LIMIT 10;
   *
   * @param range the range to scan.
   * @param limit limit number of row
   * @return the scan query
   */
  private String getScanQuery(Range range, int limit) {
    StringBuilder queryString = new StringBuilder("SELECT * FROM ").append(tableSchema.getTableId().getName());
    if (range.getBegin().isEmpty() && range.getEnd().isEmpty()) {
      return queryString.append(" LIMIT ").append(limit).append(";").toString();
    }

    queryString.append(" WHERE ");
    appendScanBound(queryString, range.getBegin(), range.getBeginBound().equals(Range.Bound.INCLUSIVE) ? ">=" : ">");
    if (!range.getBegin().isEmpty() && !range.getEnd().isEmpty()) {
      queryString.append(" AND ");
    }
    appendScanBound(queryString, range.getEnd(), range.getEndBound().equals(Range.Bound.INCLUSIVE) ? "<=" : "<");
    queryString.append(" LIMIT ").append(limit).append(";");
    return queryString.toString();
  }

  private void appendScanBound(StringBuilder sb,
                               Collection<Field<?>> keys, String comparator) {
    if (keys.isEmpty()) {
      return;
    }

    StringJoiner keyJoiner = new StringJoiner(",", "(", ")");
    StringJoiner valueJoiner = new StringJoiner(",", "(", ")");
    for (Field<?> field : keys) {
      keyJoiner.add(field.getName());
      valueJoiner.add("?");
    }

    sb.append(keyJoiner.toString())
      .append(comparator)
      .append(valueJoiner.toString());
  }

  private String getDeleteQuery(Collection<Field<?>> keys) {
    String prefix = "DELETE FROM " + tableSchema.getTableId().getName() + " WHERE ";
    return combineWithEqualClause(prefix, keys);
  }

  private String combineWithEqualClause(String prefix, Collection<Field<?>> keys) {
    StringJoiner joiner = new StringJoiner(" AND ", prefix, ";");
    for (Field<?> key : keys) {
      joiner.add(key.getName() + "=?");
    }
    return joiner.toString();
  }

  private static final class ResultSetIterator extends AbstractCloseableIterator<StructuredRow> {
    private final Statement statement;
    private final ResultSet resultSet;
    private final Set<String> columnNames;
    private final StructuredTableSchema schema;


    ResultSetIterator(Statement statement, ResultSet resultSet, StructuredTableSchema schema) throws SQLException {
      this.statement = statement;
      this.resultSet = resultSet;
      this.columnNames = createColNames(resultSet.getMetaData());
      this.schema = schema;
    }

    @Override
    protected StructuredRow computeNext() {
      try {
        if (!resultSet.next()) {
          return endOfData();
        }

        Map<String, Object> row = new HashMap<>();
        for (String colName : columnNames) {
          row.put(colName, resultSet.getObject(colName));
        }
        return new SqlStructuredRow(schema, row);
      } catch (SQLException e) {
        throw new RuntimeException("Failed to get the next value from the sql result set", e);
      }
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

    private static Set<String> createColNames(ResultSetMetaData metaData) throws SQLException {
      Set<String> columns = new HashSet<>();
      int numCols = metaData.getColumnCount();
      for (int i = 1; i <= numCols; i++) {
        columns.add(metaData.getColumnName(i));
      }
      return columns;
    }
  }
}
