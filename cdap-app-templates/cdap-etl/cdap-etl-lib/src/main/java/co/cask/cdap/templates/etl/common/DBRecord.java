/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Writable class for DB Source/Sink
 *
 * {@see DBWritable}, {@see DBInputFormat}, {@see DBOutputFormat}
 */
public class DBRecord implements Writable, DBWritable {

  private StructuredRecord record;

  public void readFields(DataInput in) throws IOException {
    // no-op, since we may never need to support a scenario where you read a DBRecord from a non-RDBMS source
  }

  /**
   * @return the {@link StructuredRecord} contained in this object
   */
  public StructuredRecord getRecord() {
    return record;
  }

  /**
   * Builds the {@link #record} using the specified {@link ResultSet}
   *
   * @param resultSet the {@link ResultSet} to build the {@link StructuredRecord} from
   */
  public void readFields(ResultSet resultSet) throws SQLException {
    List<Schema.Field> schemaFields = Lists.newArrayList();
    ResultSetMetaData metadata = resultSet.getMetaData();
    // ResultSetMetadata columns are numbered starting with 1
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      String columnName = metadata.getColumnName(i);
      int columnSqlType = metadata.getColumnType(i);
      Schema columnSchema = Schema.of(getType(columnSqlType));
      if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
        columnSchema = Schema.nullableOf(columnSchema);
      }
      Schema.Field field = Schema.Field.of(columnName, columnSchema);
      schemaFields.add(field);
    }
    Schema schema = Schema.recordOf("dbRecord", schemaFields);
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    for (int i = 0; i < schemaFields.size(); i++) {
      Schema.Field field = schemaFields.get(i);
      int sqlColumnType = metadata.getColumnType(i + 1);
      recordBuilder.set(field.getName(), transformValue(sqlColumnType, resultSet.getObject(field.getName())));
    }
    record = recordBuilder.build();
  }

  public void write(DataOutput out) throws IOException {
    Schema recordSchema = record.getSchema();
    List<Schema.Field> schemaFields = recordSchema.getFields();
    for (int i = 0; i < schemaFields.size(); i++) {
      Schema.Field field = schemaFields.get(i);
      String fieldName = field.getName();
      Schema.Type fieldType = field.getSchema().getType();
      Object fieldValue = record.get(fieldName);
      // In JDBC, field indices start with 1
      writeToDataOut(out, fieldType, fieldValue);
    }
  }

  /**
   * Writes the {@link #record} to the specified {@link PreparedStatement}
   *
   * @param stmt the {@link PreparedStatement} to write the {@link StructuredRecord} to
   */
  public void write(PreparedStatement stmt) throws SQLException {
    Schema recordSchema = record.getSchema();
    List<Schema.Field> schemaFields = recordSchema.getFields();
    for (int i = 0; i < schemaFields.size(); i++) {
      Schema.Field field = schemaFields.get(i);
      String fieldName = field.getName();
      Schema.Type fieldType = field.getSchema().getType();
      Object fieldValue = record.get(fieldName);
      // In JDBC, field indices start with 1
      writeToDB(stmt, fieldType, fieldValue, i + 1);
    }
  }

  private Schema.Type getType(int sqlType) throws SQLException {
    // Type.STRING covers sql types - case VARCHAR,CHAR,CLOB,LONGNVARCHAR,LONGVARCHAR,NCHAR,NCLOB,NVARCHAR
    Schema.Type type = Schema.Type.STRING;
    switch (sqlType) {
      case Types.NULL:
        type = Schema.Type.NULL;
        break;

      case Types.BOOLEAN:
      case Types.BIT:
        type = Schema.Type.BOOLEAN;
        break;

      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        type = Schema.Type.INT;
        break;

      case Types.BIGINT:
        type = Schema.Type.LONG;
        break;

      case Types.REAL:
      case Types.FLOAT:
        type = Schema.Type.FLOAT;
        break;

      case Types.NUMERIC:
      case Types.DECIMAL:
      case Types.DOUBLE:
        type = Schema.Type.DOUBLE;
        break;

      case Types.DATE:
      case Types.TIME:
      case Types.TIMESTAMP:
        type = Schema.Type.LONG;
        break;

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        type = Schema.Type.BYTES;
        break;

      case Types.ARRAY:
      case Types.DATALINK:
      case Types.DISTINCT:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
      case Types.REF:
      case Types.ROWID:
      case Types.SQLXML:
      case Types.STRUCT:
        throw new SQLException(new UnsupportedTypeException("Unsupported SQL Type: " + sqlType));
    }

    return type;
  }

  @Nullable
  private Object transformValue(int sqlColumnType, Object original) throws SQLException {
    if (original != null) {
      switch (sqlColumnType) {
        case Types.NUMERIC:
        case Types.DECIMAL:
          return ((BigDecimal) original).doubleValue();
        case Types.DATE:
          return ((Date) original).getTime();
        case Types.TIME:
          return ((Time) original).getTime();
        case Types.TIMESTAMP:
          return ((Timestamp) original).getTime();
        case Types.BLOB:
          Object toReturn;
          Blob blob = (Blob) original;
          try {
            toReturn = blob.getBytes(1, (int) blob.length());
          } finally {
            blob.free();
          }
          return toReturn;
        case Types.CLOB:
          String s;
          StringBuffer sbf = new StringBuffer();
          Clob clob = (Clob) original;
          try {
            BufferedReader br = new BufferedReader(clob.getCharacterStream(1, (int) clob.length()));
            try {
              while ((s = br.readLine()) != null) {
                sbf.append(s);
                sbf.append(System.getProperty("line.separator"));
              }
            } finally {
              br.close();
            }
          } catch (IOException e) {
            throw new SQLException(e);
          } finally {
            clob.free();
          }
          return sbf.toString();
      }
    }
    return original;
  }

  private void writeToDataOut(DataOutput out, Schema.Type fieldType, Object fieldValue) throws IOException {
    switch (fieldType) {
      case NULL:
        break;
      case STRING:
        // write string appropriately
        out.writeUTF((String) fieldValue);
        break;
      case BOOLEAN:
        out.writeBoolean((Boolean) fieldValue);
        break;
      case INT:
        // write short or int appropriately
        out.writeInt((Integer) fieldValue);
        break;
      case LONG:
        // write date, timestamp or long appropriately
        out.writeLong((Long) fieldValue);
        break;
      case FLOAT:
        // both real and float are set with the same method on prepared statement
        out.writeFloat((Float) fieldValue);
        break;
      case DOUBLE:
        out.writeDouble((Double) fieldValue);
        break;
      case BYTES:
        out.write((byte[]) fieldValue);
        break;
      default:
        throw new IOException(String.format("Unsupported datatype: %s with value: %s.", fieldType, fieldValue));
    }
  }

  private void writeToDB(PreparedStatement stmt, Schema.Type fieldType, Object fieldValue,
                         int fieldIndex) throws SQLException {
    switch (fieldType) {
      case NULL:
        stmt.setNull(fieldIndex, fieldIndex);
        break;
      case STRING:
        // write string appropriately
        writeString(stmt, fieldIndex, fieldValue);
        break;
      case BOOLEAN:
        stmt.setBoolean(fieldIndex, (Boolean) fieldValue);
        break;
      case INT:
        // write short or int appropriately
        writeInt(stmt, fieldIndex, fieldValue);
        break;
      case LONG:
        // write date, timestamp or long appropriately
        writeLong(stmt, fieldIndex, fieldValue);
        break;
      case FLOAT:
        // both real and float are set with the same method on prepared statement
        stmt.setFloat(fieldIndex, (Float) fieldValue);
        break;
      case DOUBLE:
        stmt.setDouble(fieldIndex, (Double) fieldValue);
        break;
      case BYTES:
        writeBytes(stmt, fieldIndex, fieldValue);
        break;
      default:
        throw new SQLException(String.format("Unsupported datatype: %s with value: %s.", fieldType, fieldValue));
    }
  }

  private void writeBytes(PreparedStatement stmt, int fieldIndex, Object fieldValue) throws SQLException {
    switch (stmt.getMetaData().getColumnType(fieldIndex)) {
      case Types.BLOB:
        stmt.setBlob(fieldIndex, (Blob) fieldValue);
        break;
      default:
        // handles BINARY, VARBINARY and LOGVARBINARY
        stmt.setBytes(fieldIndex, (byte []) fieldValue);
        break;
    }
  }

  private void writeInt(PreparedStatement stmt, int fieldIndex, Object fieldValue) throws SQLException {
    switch (stmt.getMetaData().getColumnType(fieldIndex)) {
      case Types.TINYINT:
      case Types.SMALLINT:
        stmt.setShort(fieldIndex, (Short) fieldValue);
        break;
      default:
        stmt.setInt(fieldIndex, (Integer) fieldValue);
    }
  }

  private void writeString(PreparedStatement stmt, int fieldIndex, Object fieldValue) throws SQLException {
    switch (stmt.getMetaData().getColumnType(fieldIndex)) {
      case Types.CLOB:
        stmt.setClob(fieldIndex, (Clob) fieldValue);
        break;
      default:
        stmt.setString(fieldIndex, (String) fieldValue);
        break;
    }
  }

  private void writeLong(PreparedStatement stmt, int fieldIndex, Object fieldValue) throws SQLException {
    switch (stmt.getMetaData().getColumnType(fieldIndex)) {
      case Types.DATE:
        stmt.setDate(fieldIndex, (Date) fieldValue);
        break;
      case Types.TIME:
        stmt.setTime(fieldIndex, (Time) fieldValue);
        break;
      case Types.TIMESTAMP:
        stmt.setTimestamp(fieldIndex, (Timestamp) fieldValue);
        break;
      default:
        stmt.setLong(fieldIndex, (Long) fieldValue);
        break;
    }
  }
}
