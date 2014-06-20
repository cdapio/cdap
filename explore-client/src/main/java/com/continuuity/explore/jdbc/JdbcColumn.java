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

  static String columnClassName(int columnType)
      throws SQLException {
    // according to hiveTypeToSqlType possible options are:
    switch(columnType) {
      case Types.BOOLEAN:
        return Boolean.class.getName();
      case Types.CHAR:
      case Types.VARCHAR:
        return String.class.getName();
      case Types.TINYINT:
        return Byte.class.getName();
      case Types.SMALLINT:
        return Short.class.getName();
      case Types.INTEGER:
        return Integer.class.getName();
      case Types.BIGINT:
        return Long.class.getName();
      case Types.DATE:
        return Date.class.getName();
      case Types.FLOAT:
        return Float.class.getName();
      case Types.DOUBLE:
        return Double.class.getName();
      case  Types.TIMESTAMP:
        return Timestamp.class.getName();
      case Types.DECIMAL:
        return BigInteger.class.getName();
      case Types.BINARY:
        return byte[].class.getName();
      case Types.JAVA_OBJECT:
      case Types.ARRAY:
      case Types.STRUCT:
        return String.class.getName();
      default:
        throw new SQLException("Invalid column type: " + columnType);
    }
  }

  public static int hiveTypeToSqlType(String type) throws SQLException {
    if ("string".equalsIgnoreCase(type)) {
      return Types.VARCHAR;
    } else if ("varchar".equalsIgnoreCase(type)) {
      return Types.VARCHAR;
    } else if ("char".equalsIgnoreCase(type)) {
      return Types.CHAR;
    } else if ("float".equalsIgnoreCase(type)) {
      return Types.FLOAT;
    } else if ("double".equalsIgnoreCase(type)) {
      return Types.DOUBLE;
    } else if ("boolean".equalsIgnoreCase(type)) {
      return Types.BOOLEAN;
    } else if ("tinyint".equalsIgnoreCase(type)) {
      return Types.TINYINT;
    } else if ("smallint".equalsIgnoreCase(type)) {
      return Types.SMALLINT;
    } else if ("int".equalsIgnoreCase(type)) {
      return Types.INTEGER;
    } else if ("bigint".equalsIgnoreCase(type)) {
      return Types.BIGINT;
    } else if ("date".equalsIgnoreCase(type)) {
      return Types.DATE;
    } else if ("timestamp".equalsIgnoreCase(type)) {
      return Types.TIMESTAMP;
    } else if ("decimal".equalsIgnoreCase(type)) {
      return Types.DECIMAL;
    } else if ("binary".equalsIgnoreCase(type)) {
      return Types.BINARY;
    } else if (type.toLowerCase().startsWith("map")) {
      return Types.JAVA_OBJECT;
    } else if (type.toLowerCase().startsWith("array")) {
      return Types.ARRAY;
    } else if (type.toLowerCase().startsWith("struct")) {
      return Types.STRUCT;
    }
    throw new SQLException("Unrecognized column type: " + type);
  }

  static String getColumnTypeName(String type) throws SQLException {
    // we need to convert the Hive type to the SQL type name
    // TODO: this would be better handled in an enum
    if ("string".equalsIgnoreCase(type)) {
      return "string";
    } else if ("varchar".equalsIgnoreCase(type)) {
      return "varchar";
    } else if ("char".equalsIgnoreCase(type)) {
      return "char";
    } else if ("float".equalsIgnoreCase(type)) {
      return "float";
    } else if ("double".equalsIgnoreCase(type)) {
      return "double";
    } else if ("boolean".equalsIgnoreCase(type)) {
      return "boolean";
    } else if ("tinyint".equalsIgnoreCase(type)) {
      return "tinyint";
    } else if ("smallint".equalsIgnoreCase(type)) {
      return "smallint";
    } else if ("int".equalsIgnoreCase(type)) {
      return "int";
    } else if ("bigint".equalsIgnoreCase(type)) {
      return "bigint";
    } else if ("timestamp".equalsIgnoreCase(type)) {
      return "timestamp";
    } else if ("date".equalsIgnoreCase(type)) {
      return "date";
    } else if ("decimal".equalsIgnoreCase(type)) {
      return "decimal";
    } else if ("binary".equalsIgnoreCase(type)) {
      return "binary";
    } else if ("void".equalsIgnoreCase(type)) {
      return "void";
    } else if (type.toLowerCase().startsWith("map")) {
      return "map";
    } else if (type.toLowerCase().startsWith("array")) {
      return "array";
    } else if (type.toLowerCase().startsWith("struct")) {
      return "struct";
    }
    throw new SQLException("Unrecognized column type: " + type);
  }
}
