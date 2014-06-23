package com.continuuity.explore.jdbc;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;

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
