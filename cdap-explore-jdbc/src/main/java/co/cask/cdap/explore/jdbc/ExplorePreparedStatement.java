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

import co.cask.cdap.explore.client.ExploreClient;
import com.google.common.collect.Maps;

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
import java.util.Map;

/**
 * Explore JDBC prepared statement.
 */
public class ExplorePreparedStatement extends ExploreStatement implements PreparedStatement {

  private final String sql;

  private String lastUpdatedSql = null;
  private ResultSet lastResultSet = null;
  private boolean lastResultSuccess = false;

  // Save the SQL parameters {paramLoc:paramValue}
  private final Map<Integer, String> parameters = Maps.newHashMap();

  ExplorePreparedStatement(Connection connection, ExploreClient exploreClient, String sql, String namespace) {
    super(connection, exploreClient, namespace);

    // Although a PreparedStatement is meant to precompile sql statement, our Explore client
    // interface does not allow it.
    this.sql = sql;
  }

  @Override
  public boolean execute() throws SQLException {
    if (isClosed()) {
      throw new SQLException("Can't execute after statement has been closed");
    }
    String updatedSql = updateSql();
    if (lastUpdatedSql == null || !lastUpdatedSql.equals(updatedSql)) {
      lastUpdatedSql = updatedSql;
      lastResultSuccess = super.execute(lastUpdatedSql);
      lastResultSet = getResultSet();
    }
    return lastResultSuccess;
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    if (!execute()) {
      throw new SQLException("The query did not generate a result set!");
    }
    return lastResultSet;
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    this.parameters.put(parameterIndex, String.valueOf(x));
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    this.parameters.put(parameterIndex, String.valueOf(x));
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    this.parameters.put(parameterIndex, String.valueOf(x));
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    this.parameters.put(parameterIndex, String.valueOf(x));
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    this.parameters.put(parameterIndex, String.valueOf(x));
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    this.parameters.put(parameterIndex, String.valueOf(x));
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    this.parameters.put(parameterIndex, String.valueOf(x));
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    this.parameters.put(parameterIndex, x.toPlainString());
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    this.parameters.put(parameterIndex, String.format("'%s'", x.replace("'", "\\'")));
  }

  @Override
  public void setNull(int i, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBytes(int i, byte[] bytes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setDate(int parameterIndex, Date date) throws SQLException {
    this.parameters.put(parameterIndex, date.toString());
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    this.parameters.put(parameterIndex, x.toString());
  }

  @Override
  public void clearParameters() throws SQLException {
    parameters.clear();
  }

  @Override
  public void setTime(int i, Time time) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAsciiStream(int i, InputStream inputStream, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  @Deprecated
  public void setUnicodeStream(int i, InputStream inputStream, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBinaryStream(int i, InputStream inputStream, int i2) throws SQLException {
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
    if (lastResultSet != null) {
      // If the query has already been run, we return the metadata of the existing result set
      return lastResultSet.getMetaData();
    }
    // Otherwise we first run the query and return the metadata, making it expensive to call this method
    return executeQuery().getMetaData();
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

  /**
   * Update the SQL string with parameters set by setXXX methods of {@link PreparedStatement}.
   * Package visibility is for testing.
   */
  String updateSql() throws SQLException {
    StringBuilder newSql = new StringBuilder(sql);

    int paramLoc = 1;
    while (true) {
      int nextParamIndex = getCharIndexFromSqlByParamLocation(newSql, '?', 1);
      if (nextParamIndex < 0) {
        break;
      }
      String tmp = parameters.get(paramLoc);
      if (tmp == null) {
        throw new SQLException("Parameter in position " + paramLoc + " has not been set.");
      }
      newSql.replace(nextParamIndex, nextParamIndex + 1, tmp);
      paramLoc++;
    }
    return newSql.toString();
  }

  /**
   * Get the index of the paramLoc-th given cchar from the SQL string.
   * -1 will be return, if nothing found
   */
  private int getCharIndexFromSqlByParamLocation(StringBuilder sql, char cchar, int paramLoc) {
    boolean singleQuoteStr = false;
    boolean doubleQuoteStr = false;
    boolean escapeActive = false;
    int charIndex = -1;
    int num = 0;
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (escapeActive) {
        escapeActive = false;
      } else if (!singleQuoteStr && !doubleQuoteStr && c == '"') {
        doubleQuoteStr = true;
      } else if (!singleQuoteStr && !doubleQuoteStr && c == '\'') {
        singleQuoteStr = true;
      } else if ((singleQuoteStr || doubleQuoteStr) && !escapeActive && c == '\\') {
        escapeActive = true;
      } else if (singleQuoteStr && c == '\'') {
        singleQuoteStr = false;
      } else if (doubleQuoteStr && c == '"') {
        doubleQuoteStr = false;
      } else if (c == cchar && !singleQuoteStr && !doubleQuoteStr) {
        num++;
        if (num == paramLoc) {
          charIndex = i;
          break;
        }
      }
    }
    return charIndex;
  }
}
