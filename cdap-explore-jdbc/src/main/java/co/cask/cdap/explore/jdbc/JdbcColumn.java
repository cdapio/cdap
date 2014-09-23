/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.explore.jdbc;

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
   * Default precision when user doesn't specify in the column metadata, such as
   * decimal and decimal(8).
   * TODO: right now we don't return precision and scale given by a user - this should be returned with
   * columns metadata, see CDAP-14.
   */
  public static final int USER_DEFAULT_PRECISION = 10;

  /**
   * Sql types and corresponding hive type names and Java classes names.
   */
  public enum SqlTypes {
    STRING("string", Types.VARCHAR, String.class.getName(), Integer.MAX_VALUE, 0, Integer.MAX_VALUE),
    VARCHAR("varchar", Types.VARCHAR, String.class.getName(), Integer.MAX_VALUE, 0, Integer.MAX_VALUE),
    CHAR("char", Types.CHAR, String.class.getName(), Integer.MAX_VALUE, 0, Integer.MAX_VALUE),
    FLOAT("float", Types.FLOAT, Float.class.getName(), 7, 7, 24),
    DOUBLE("double", Types.DOUBLE, Double.class.getName(), 15, 15, 25),
    BOOLEAN("boolean", Types.BOOLEAN, Boolean.class.getName(), 1, 0, 1),
    TINYINT("tinyint", Types.TINYINT, Byte.class.getName(), 3, 0, 4),
    SMALLINT("smallint", Types.SMALLINT, Short.class.getName(), 5, 0, 6),
    INT("int", Types.INTEGER, Integer.class.getName(), 10, 0, 11),
    BIGINT("bigint", Types.BIGINT, Long.class.getName(), 19, 0, 20),
    TIMESTAMP("timestamp", Types.TIMESTAMP, Timestamp.class.getName(), 29, 9, 29),
    DATE("date", Types.DATE, Date.class.getName(), 10, 0, 10),
    DECIMAL("decimal", Types.DECIMAL, BigInteger.class.getName(), USER_DEFAULT_PRECISION, Integer.MAX_VALUE,
            USER_DEFAULT_PRECISION + 2),
    BINARY("binary", Types.BINARY, byte[].class.getName(), Integer.MAX_VALUE, 0, Integer.MAX_VALUE),
    MAP("map", Types.JAVA_OBJECT, String.class.getName(), Integer.MAX_VALUE, 0, Integer.MAX_VALUE),
    ARRAY("array", Types.ARRAY, String.class.getName(), Integer.MAX_VALUE, 0, Integer.MAX_VALUE),
    STRUCT("struct", Types.STRUCT, String.class.getName(), Integer.MAX_VALUE, 0, Integer.MAX_VALUE);

    private final String typeName;
    private final int sqlType;
    private final String className;
    private final int precision;
    private final int scale;

    private final int displaySize;

    SqlTypes(String typeName, int sqlType, String className, int precision, int scale, int displaySize) {
      this.typeName = typeName;
      this.sqlType = sqlType;
      this.className = className;
      this.precision = precision;
      this.scale = scale;
      this.displaySize = displaySize;
    }

    public boolean isNumber() {
      switch (this) {
        case FLOAT:
        case DOUBLE:
        case SMALLINT:
        case INT:
        case BIGINT:
        case DECIMAL:
        case TINYINT:
          return true;
        case STRING:
        case VARCHAR:
        case CHAR:
        case BOOLEAN:
        case TIMESTAMP:
        case DATE:
        case BINARY:
        case MAP:
        case ARRAY:
        case STRUCT:
          return false;
        default:
          return false;
      }
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

    public int getPrecision() {
      return precision;
    }
    public int getScale() {
      return scale;
    }

    public int getDisplaySize() {
      return displaySize;
    }

  }

  static int columnDisplaySize(int columnType) throws SQLException {
    for (SqlTypes t : SqlTypes.values()) {
      if (t.getSqlType() == columnType) {
        return t.getDisplaySize();
      }
    }
    throw new SQLException("Invalid column type: " + columnType);
  }

  static int columnScale(int columnType) throws SQLException {
    for (SqlTypes t : SqlTypes.values()) {
      if (t.getSqlType() == columnType) {
        return t.getScale();
      }
    }
    throw new SQLException("Invalid column type: " + columnType);
  }

  static int columnPrecision(int columnType) throws SQLException {
    for (SqlTypes t : SqlTypes.values()) {
      if (t.getSqlType() == columnType) {
        return t.getPrecision();
      }
    }
    throw new SQLException("Invalid column type: " + columnType);
  }

  static String columnClassName(int columnType) throws SQLException {
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

  static boolean isNumber(int columnType) throws SQLException {
    for (SqlTypes t : SqlTypes.values()) {
      if (t.getSqlType() == columnType) {
        return t.isNumber();
      }
    }
    throw new SQLException("Invalid column type: " + columnType);
  }
}
