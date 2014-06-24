package com.continuuity.explore.jdbc;

import com.continuuity.explore.service.ColumnDesc;
import com.google.common.base.Preconditions;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

/**
 * Result Set returned when executing a query using Explore JDBC {@link ExploreStatement}.
 */
public class ExploreResultSetMetaData implements ResultSetMetaData {

  private final List<ColumnDesc> columnDescs;

  public ExploreResultSetMetaData(List<ColumnDesc> columnDescs) {
    Preconditions.checkNotNull(columnDescs, "Column metadata list cannot be null.");
    this.columnDescs = columnDescs;
  }

  int getColumnPosition(String name) throws SQLException {
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
      return min.intValue();
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
    if ("string".equalsIgnoreCase(desc.getType())) {
      return true;
    }
    return false;
  }

  @Override
  public int isNullable(int i) throws SQLException {
    // From Hive JDBC code: Hive doesn't have the concept of not-null
    return ResultSetMetaData.columnNullable;
  }

  @Override
  public boolean isSearchable(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isCurrency(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isSigned(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getColumnDisplaySize(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getSchemaName(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getPrecision(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getScale(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getTableName(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getCatalogName(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isReadOnly(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWritable(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isDefinitelyWritable(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}
