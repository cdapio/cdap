package com.continuuity.explore.jdbc;

import com.continuuity.explore.service.ColumnDesc;

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

  int getColumnPosition(String name) throws SQLException {
    if (columnDescs == null) {
      throw new SQLException("Could not determine column meta data for ResultSet");
    }
    for (ColumnDesc desc : columnDescs) {
      if (desc.getName().equals(name)) {
        return desc.getPosition();
      }
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
    // Hive doesn't have an auto-increment concept
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    // Content of a column is case sensitive only if it is a string
    // TODO figure out why hive does that - does not make much sense
    ColumnDesc desc = getColumnDesc(column);
    if ("string".equalsIgnoreCase(desc.getType())) {
      return true;
    }
    return false;
  }

  @Override
  public int isNullable(int i) throws SQLException {
    // Hive doesn't have the concept of not-null
    return ResultSetMetaData.columnNullable;
  }

  @Override
  public boolean isSearchable(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean isCurrency(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean isSigned(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public int getColumnDisplaySize(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public String getSchemaName(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public int getPrecision(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public int getScale(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public String getTableName(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public String getCatalogName(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean isReadOnly(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean isWritable(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean isDefinitelyWritable(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    throw new SQLException("Method not supported");
  }
}
