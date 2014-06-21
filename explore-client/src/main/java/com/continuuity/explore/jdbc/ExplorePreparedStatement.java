package com.continuuity.explore.jdbc;

import com.continuuity.explore.service.Explore;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 *
 */
public class ExplorePreparedStatement extends ExploreStatement implements PreparedStatement {

  private final String sql;

  public ExplorePreparedStatement(Connection connection, Explore exploreClient, String sql) {
    super(connection, exploreClient);
    this.sql = sql;
  }

  @Override
  public boolean execute() throws SQLException {
    // TODO update the SQL string with parameters set by setXXX methods
    return super.execute(sql);
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    // TODO update the SQL string with parameters set by setXXX methods
    return super.executeQuery(sql);
  }

  @Override
  public int executeUpdate() throws SQLException {
    return 0;
  }

  @Override
  public void setNull(int i, int i2) throws SQLException {

  }

  @Override
  public void setBoolean(int i, boolean b) throws SQLException {

  }

  @Override
  public void setByte(int i, byte b) throws SQLException {

  }

  @Override
  public void setShort(int i, short i2) throws SQLException {

  }

  @Override
  public void setInt(int i, int i2) throws SQLException {

  }

  @Override
  public void setLong(int i, long l) throws SQLException {

  }

  @Override
  public void setFloat(int i, float v) throws SQLException {

  }

  @Override
  public void setDouble(int i, double v) throws SQLException {

  }

  @Override
  public void setBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {

  }

  @Override
  public void setString(int i, String s) throws SQLException {

  }

  @Override
  public void setBytes(int i, byte[] bytes) throws SQLException {

  }

  @Override
  public void setDate(int i, Date date) throws SQLException {

  }

  @Override
  public void setTime(int i, Time time) throws SQLException {

  }

  @Override
  public void setTimestamp(int i, Timestamp timestamp) throws SQLException {

  }

  @Override
  public void setAsciiStream(int i, InputStream inputStream, int i2) throws SQLException {

  }

  @Override
  public void setUnicodeStream(int i, InputStream inputStream, int i2) throws SQLException {

  }

  @Override
  public void setBinaryStream(int i, InputStream inputStream, int i2) throws SQLException {

  }

  @Override
  public void clearParameters() throws SQLException {

  }

  @Override
  public void setObject(int i, Object o, int i2) throws SQLException {

  }

  @Override
  public void setObject(int i, Object o) throws SQLException {

  }

  @Override
  public void addBatch() throws SQLException {

  }

  @Override
  public void setCharacterStream(int i, Reader reader, int i2) throws SQLException {

  }

  @Override
  public void setRef(int i, Ref ref) throws SQLException {

  }

  @Override
  public void setBlob(int i, Blob blob) throws SQLException {

  }

  @Override
  public void setClob(int i, Clob clob) throws SQLException {

  }

  @Override
  public void setArray(int i, Array array) throws SQLException {

  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return null;
  }

  @Override
  public void setDate(int i, Date date, Calendar calendar) throws SQLException {

  }

  @Override
  public void setTime(int i, Time time, Calendar calendar) throws SQLException {

  }

  @Override
  public void setTimestamp(int i, Timestamp timestamp, Calendar calendar) throws SQLException {

  }

  @Override
  public void setNull(int i, int i2, String s) throws SQLException {

  }

  @Override
  public void setURL(int i, URL url) throws SQLException {

  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    return null;
  }

  @Override
  public void setRowId(int i, RowId rowId) throws SQLException {

  }

  @Override
  public void setNString(int i, String s) throws SQLException {

  }

  @Override
  public void setNCharacterStream(int i, Reader reader, long l) throws SQLException {

  }

  @Override
  public void setNClob(int i, NClob nClob) throws SQLException {

  }

  @Override
  public void setClob(int i, Reader reader, long l) throws SQLException {

  }

  @Override
  public void setBlob(int i, InputStream inputStream, long l) throws SQLException {

  }

  @Override
  public void setNClob(int i, Reader reader, long l) throws SQLException {

  }

  @Override
  public void setSQLXML(int i, SQLXML sqlxml) throws SQLException {

  }

  @Override
  public void setObject(int i, Object o, int i2, int i3) throws SQLException {

  }

  @Override
  public void setAsciiStream(int i, InputStream inputStream, long l) throws SQLException {

  }

  @Override
  public void setBinaryStream(int i, InputStream inputStream, long l) throws SQLException {

  }

  @Override
  public void setCharacterStream(int i, Reader reader, long l) throws SQLException {

  }

  @Override
  public void setAsciiStream(int i, InputStream inputStream) throws SQLException {

  }

  @Override
  public void setBinaryStream(int i, InputStream inputStream) throws SQLException {

  }

  @Override
  public void setCharacterStream(int i, Reader reader) throws SQLException {

  }

  @Override
  public void setNCharacterStream(int i, Reader reader) throws SQLException {

  }

  @Override
  public void setClob(int i, Reader reader) throws SQLException {

  }

  @Override
  public void setBlob(int i, InputStream inputStream) throws SQLException {

  }

  @Override
  public void setNClob(int i, Reader reader) throws SQLException {

  }
}
