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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Explore JDBC prepared statement.
 */
public class ExplorePreparedStatement extends ExploreStatement implements PreparedStatement {

  private final String sql;

  public ExplorePreparedStatement(Connection connection, Explore exploreClient, String sql) {
    super(connection, exploreClient);

    // Although a PreparedStatement is meant to precompile sql statement, our Explore client
    // interface does not allow it.
    this.sql = sql;
  }

  @Override
  public boolean execute() throws SQLException {
    // TODO update the SQL string with parameters set by setXXX methods [REACTOR-320]
    return super.execute(sql);
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    // TODO update the SQL string with parameters set by setXXX methods [REACTOR-320]
    return super.executeQuery(sql);
  }

  @Override
  public int executeUpdate() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNull(int i, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBoolean(int i, boolean b) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setByte(int i, byte b) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setShort(int i, short i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setInt(int i, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setLong(int i, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setFloat(int i, float v) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setDouble(int i, double v) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setString(int i, String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBytes(int i, byte[] bytes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setDate(int i, Date date) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTime(int i, Time time) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTimestamp(int i, Timestamp timestamp) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAsciiStream(int i, InputStream inputStream, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setUnicodeStream(int i, InputStream inputStream, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBinaryStream(int i, InputStream inputStream, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearParameters() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setObject(int i, Object o, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setObject(int i, Object o) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void addBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setCharacterStream(int i, Reader reader, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setRef(int i, Ref ref) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBlob(int i, Blob blob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClob(int i, Clob clob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setArray(int i, Array array) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setDate(int i, Date date, Calendar calendar) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTime(int i, Time time, Calendar calendar) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTimestamp(int i, Timestamp timestamp, Calendar calendar) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNull(int i, int i2, String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setURL(int i, URL url) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setRowId(int i, RowId rowId) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNString(int i, String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNCharacterStream(int i, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNClob(int i, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClob(int i, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBlob(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNClob(int i, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setSQLXML(int i, SQLXML sqlxml) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setObject(int i, Object o, int i2, int i3) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setCharacterStream(int i, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAsciiStream(int i, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBinaryStream(int i, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setCharacterStream(int i, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNCharacterStream(int i, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClob(int i, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBlob(int i, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNClob(int i, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}
