package com.continuuity.explore.jdbc;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;


/**
 * Column metadata.
 */
public class JdbcColumn {

  /**
   * Sql types and corresponding hive type names and Java classes names.
   */
  public enum SqlTypes {
    STRING("string", Types.VARCHAR, String.class.getName()),
    VARCHAR("varchar", Types.VARCHAR, String.class.getName()),
    CHAR("char", Types.CHAR, String.class.getName()),
    FLOAT("float", Types.FLOAT, Float.class.getName()),
    DOUBLE("double", Types.DOUBLE, Double.class.getName()),
    BOOLEAN("boolean", Types.BOOLEAN, Boolean.class.getName()),
    TINYINT("tinyint", Types.TINYINT, Byte.class.getName()),
    SMALLINT("smallint", Types.SMALLINT, Short.class.getName()),
    INT("int", Types.INTEGER, Integer.class.getName()),
    BIGINT("bigint", Types.BIGINT, Long.class.getName()),
    TIMESTAMP("timestamp", Types.TIMESTAMP, Timestamp.class.getName()),
    DATE("date", Types.DATE, Date.class.getName()),
    DECIMAL("decimal", Types.DECIMAL, BigInteger.class.getName()),
    BINARY("binary", Types.BINARY, byte[].class.getName()),
    MAP("map", Types.JAVA_OBJECT, String.class.getName()),
    ARRAY("array", Types.ARRAY, String.class.getName()),
    STRUCT("struct", Types.STRUCT, String.class.getName());

    private String typeName;
    private int sqlType;
    private String className;

    SqlTypes(String typeName, int sqlType, String className) {
      this.typeName = typeName;
      this.sqlType = sqlType;
      this.className = className;
    }

    public String getTypeName() {
      return typeName;
    }

    public String getClassName() {
      return className;
    }

    public int getSqlType() {
      return sqlType;
    }
  }

  static String columnClassName(int columnType) throws SQLException {
    // according to hiveTypeToSqlType possible options are:
    for (SqlTypes t : SqlTypes.values()) {
      if (t.getSqlType() == columnType) {
        return t.getClassName();
      }
    }
    throw new SQLException("Invalid column type: " + columnType);
  }

  public static int hiveTypeToSqlType(String type) throws SQLException {
    for (SqlTypes t : SqlTypes.values()) {
      // We use startsWith instead of equals because of some types like:
      // array<int>, array<...>, map<...>
      if (type.toLowerCase().startsWith(t.getTypeName())) {
        return t.getSqlType();
      }
    }
    throw new SQLException("Unrecognized column type: " + type);
  }

  static String getColumnTypeName(String type) throws SQLException {
    // we need to convert the Hive type to the SQL type name
    for (SqlTypes t : SqlTypes.values()) {
      // We use startsWith instead of equals because of some types like:
      // array<int>, array<...>, map<...>
      if (type.toLowerCase().startsWith(t.getTypeName())) {
        return t.getTypeName();
      }
    }
    throw new SQLException("Unrecognized column type: " + type);
  }
}
