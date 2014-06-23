package com.continuuity.explore.jdbc;

import com.continuuity.explore.service.ColumnDesc;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Result Set returned when executing a query using Explore JDBC {@link ExploreStatement}.
 */
public class ExploreResultSetMetaData implements ResultSetMetaData {

  private final List<ColumnDesc> columnDescs;

  public ExploreResultSetMetaData(List<ColumnDesc> columnDescs) {
    this.columnDescs = columnDescs;
  }

  public ExploreResultSetMetaData(List<String> columnNames, List<String> columnTypes) {
    Preconditions.checkArgument(columnNames.size() == columnTypes.size(),
                                "Size of columnNames list has to be equal to the size of columnTypes list.");

    this.columnDescs = Lists.newArrayListWithExpectedSize(columnNames.size());
    for (int i = 0; i < columnNames.size(); ++i) {
      this.columnDescs.add(new ColumnDesc(columnNames.get(i), columnTypes.get(i), i + 1, ""));
    }
  }

  int getColumnPosition(String name) throws SQLException {
    if (columnDescs == null) {
      throw new SQLException("Could not determine column meta data for ResultSet");
    }
    // If several columns have the same name, we have to return the one with the smaller position
    // hence we have to iterate through all the columns
    Integer min = null;
    for (ColumnDesc desc : columnDescs) {
      if (desc.getName().toLowerCase().equals(name)) {
        if (min == null || min > desc.getPosition()) {
          min = desc.getPosition();
        }
      }
    }
    if (min != null) {
      return min;
    }
    throw new SQLException("Could not find column with name: " + name);
  }

  private ColumnDesc getColumnDesc(int position) throws SQLException {
    if (columnDescs == null) {
      throw new SQLException("Could not determine column meta data for ResultSet");
    }
    for (ColumnDesc desc : columnDescs) {
      if (desc.getPosition() == position) {
        return desc;
      }
    }
    throw new SQLException("Could not find column at position: " + position);
  }

  @Override
  public int getColumnCount() throws SQLException {
    return columnDescs.size();
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    ColumnDesc desc = getColumnDesc(column);
    return JdbcColumn.hiveTypeToSqlType(desc.getType());
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    ColumnDesc desc = getColumnDesc(column);
    return JdbcColumn.getColumnTypeName(desc.getType());
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    int columnType = getColumnType(column);
    return JdbcColumn.columnClassName(columnType);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    ColumnDesc desc = getColumnDesc(column);
    return desc.getName();
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return getColumnName(column);
  }

  @Override
  public boolean isAutoIncrement(int i) throws SQLException {
    // From Hive JDBC code: Hive doesn't have an auto-increment concept
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    // Content of a column is case sensitive only if it is a string
    // This includes maps, arrays and structs, since they are passed as json
    ColumnDesc desc = getColumnDesc(column);
    return "string".equalsIgnoreCase(desc.getType());
  }

  @Override
  public int isNullable(int i) throws SQLException {
    // From Hive JDBC code: Hive doesn't have the concept of not-null
    return ResultSetMetaData.columnNullable;
  }

  @Override
  public boolean isSearchable(int i) throws SQLException {
    throw new SQLException("Method isSearchable not supported");
  }

  @Override
  public boolean isCurrency(int i) throws SQLException {
    // Hive doesn't support a currency type
    return false;
  }

  @Override
  public boolean isSigned(int i) throws SQLException {
    throw new SQLException("Method isSigned not supported");
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    int columnType = getColumnType(column);
    return JdbcColumn.columnDisplaySize(columnType);
  }

  @Override
  public String getSchemaName(int i) throws SQLException {
    throw new SQLException("Method getSchemaName not supported");
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    int columnType = getColumnType(column);
    return JdbcColumn.columnPrecision(columnType);
  }

  @Override
  public int getScale(int column) throws SQLException {
    int columnType = getColumnType(column);
    return JdbcColumn.columnScale(columnType);
  }

  @Override
  public String getTableName(int i) throws SQLException {
    throw new SQLException("Method getTableName not supported");
  }

  @Override
  public String getCatalogName(int i) throws SQLException {
    throw new SQLException("Method getCatalogName not supported");
  }

  @Override
  public boolean isReadOnly(int i) throws SQLException {
    throw new SQLException("Method isReadOnly not supported");
  }

  @Override
  public boolean isWritable(int i) throws SQLException {
    throw new SQLException("Method isWritable not supported");
  }

  @Override
  public boolean isDefinitelyWritable(int i) throws SQLException {
    throw new SQLException("Method isDefinitelyWritable not supported");
  }

  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    throw new SQLException("Method unwrap not supported");
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    throw new SQLException("Method isWrapperFor not supported");
  }
}
