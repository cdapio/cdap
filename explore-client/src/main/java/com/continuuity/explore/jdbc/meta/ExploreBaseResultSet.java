package com.continuuity.explore.jdbc.meta;

import com.continuuity.explore.jdbc.ExploreResultSetMetaData;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class ExploreBaseResultSet implements ResultSet {
  protected SQLWarning warningChain = null;
  protected boolean wasNull = false;
  protected List<Object> row;
  protected List<String> columnNames;
  protected List<String> columnTypes;

  public boolean absolute(int row) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void afterLast() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void beforeFirst() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void cancelRowUpdates() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void deleteRow() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int findColumn(String columnName) throws SQLException {
    int columnIndex = columnNames.indexOf(columnName);
    if (columnIndex == -1) {
      throw new SQLException();
    } else {
      return ++columnIndex;
    }
  }

  public boolean first() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Array getArray(int i) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Array getArray(String colName) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public InputStream getAsciiStream(String columnName) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof BigDecimal) {
      return ((BigDecimal) obj);
    }
    throw new SQLException("Cannot convert column " + columnIndex
                             + " to BigDecimal. Found data of type: "
                             + obj.getClass() + ", value: " + obj.toString());
  }

  public BigDecimal getBigDecimal(String columnName) throws SQLException {
    return getBigDecimal(findColumn(columnName));
  }

  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    MathContext mc = new MathContext(scale);
    return getBigDecimal(columnIndex).round(mc);
  }

  public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnName), scale);
  }

  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public InputStream getBinaryStream(String columnName) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Blob getBlob(int i) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Blob getBlob(String colName) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean getBoolean(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (Boolean.class.isInstance(obj)) {
      return (Boolean) obj;
    } else if (obj == null) {
      return false;
    } else if (Number.class.isInstance(obj)) {
      return ((Number) obj).intValue() != 0;
    } else if (String.class.isInstance(obj)) {
      return !obj.equals("0");
    }
    throw new SQLException("Cannot convert column " + columnIndex + " to boolean");
  }

  public boolean getBoolean(String columnName) throws SQLException {
    return getBoolean(findColumn(columnName));
  }

  public byte getByte(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (Number.class.isInstance(obj)) {
      return ((Number) obj).byteValue();
    } else if (obj == null) {
      return 0;
    }
    throw new SQLException("Cannot convert column " + columnIndex + " to byte");
  }

  public byte getByte(String columnName) throws SQLException {
    return getByte(findColumn(columnName));
  }

  public byte[] getBytes(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public byte[] getBytes(String columnName) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Reader getCharacterStream(String columnName) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Clob getClob(int i) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Clob getClob(String colName) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  public String getCursorName() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Date getDate(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }

    if (obj instanceof Date) {
      return (Date) obj;
    }

    try {
      if (obj instanceof String) {
        return Date.valueOf((String) obj);
      }
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex
                               + " to date: " + e.toString());
    }

    throw new SQLException("Illegal conversion");
  }

  public Date getDate(String columnName) throws SQLException {
    return getDate(findColumn(columnName));
  }

  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Date getDate(String columnName, Calendar cal) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public double getDouble(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).doubleValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Double.valueOf((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex
                               + " to double: " + e.toString());
    }
  }

  public double getDouble(String columnName) throws SQLException {
    return getDouble(findColumn(columnName));
  }

  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  public int getFetchSize() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public float getFloat(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).floatValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Float.valueOf((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex
                               + " to float: " + e.toString());
    }
  }

  public float getFloat(String columnName) throws SQLException {
    return getFloat(findColumn(columnName));
  }

  public int getHoldability() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getInt(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).intValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Integer.valueOf((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex + " to integer" + e.toString());
    }
  }

  public int getInt(String columnName) throws SQLException {
    return getInt(findColumn(columnName));
  }

  public long getLong(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).longValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Long.valueOf((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex + " to long: " + e.toString());
    }
  }

  public long getLong(String columnName) throws SQLException {
    return getLong(findColumn(columnName));
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    return new ExploreResultSetMetaData(columnNames, columnTypes);
  }

  public Reader getNCharacterStream(int arg0) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Reader getNCharacterStream(String arg0) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public NClob getNClob(int arg0) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public NClob getNClob(String columnLabel) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public String getNString(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public String getNString(String columnLabel) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Object getObject(int columnIndex) throws SQLException {
    if (row == null) {
      throw new SQLException("No row found.");
    }

    if (columnIndex > row.size()) {
      throw new SQLException("Invalid columnIndex: " + columnIndex);
    }

    try {
      wasNull = row.get(columnIndex - 1) == null;

      return row.get(columnIndex - 1);
    } catch (Exception e) {
      throw new SQLException(e.toString());
    }
  }

  public Object getObject(String columnName) throws SQLException {
    return getObject(findColumn(columnName));
  }

  @SuppressWarnings("UnusedDeclaration")
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    // TODO method required by JDK 1.7
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @SuppressWarnings("UnusedDeclaration")
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    // TODO method required by JDK 1.7
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Object getObject(String colName, Map<String, Class<?>> map) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Ref getRef(int i) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Ref getRef(String colName) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getRow() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public RowId getRowId(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public RowId getRowId(String columnLabel) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public short getShort(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).shortValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Short.valueOf((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex
                               + " to short: " + e.toString());
    }
  }

  public short getShort(String columnName) throws SQLException {
    return getShort(findColumn(columnName));
  }

  public Statement getStatement() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  /**
   * @param columnIndex - the first column is 1, the second is 2, ...
   * @see java.sql.ResultSet#getString(int)
   */

  public String getString(int columnIndex) throws SQLException {
    // Column index starts from 1, not 0.
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }

    return obj.toString();
  }

  public String getString(String columnName) throws SQLException {
    return getString(findColumn(columnName));
  }

  public Time getTime(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Time getTime(String columnName) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Time getTime(String columnName, Calendar cal) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof Timestamp) {
      return (Timestamp) obj;
    }
    if (obj instanceof String) {
      return Timestamp.valueOf((String) obj);
    }
    throw new SQLException("Illegal conversion");
  }

  public Timestamp getTimestamp(String columnName) throws SQLException {
    return getTimestamp(findColumn(columnName));
  }

  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  public URL getURL(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public URL getURL(String columnName) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public InputStream getUnicodeStream(String columnName) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void insertRow() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean isAfterLast() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean isBeforeFirst() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean isClosed() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean isFirst() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean isLast() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean last() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void moveToCurrentRow() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void moveToInsertRow() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean previous() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void refreshRow() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean relative(int rows) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean rowDeleted() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean rowInserted() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean rowUpdated() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void setFetchSize(int rows) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateArray(String columnName, Array x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateAsciiStream(int columnIndex, InputStream x, int length)
    throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateAsciiStream(String columnName, InputStream x, int length)
    throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateAsciiStream(int columnIndex, InputStream x, long length)
    throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateAsciiStream(String columnLabel, InputStream x, long length)
    throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBinaryStream(int columnIndex, InputStream x, int length)
    throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBinaryStream(String columnName, InputStream x, int length)
    throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBinaryStream(int columnIndex, InputStream x, long length)
    throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBinaryStream(String columnLabel, InputStream x, long length)
    throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBlob(String columnName, Blob x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBlob(int columnIndex, InputStream inputStream, long length)
    throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBlob(String columnLabel, InputStream inputStream,
                         long length) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBoolean(String columnName, boolean x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateByte(String columnName, byte x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateBytes(String columnName, byte[] x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateCharacterStream(int columnIndex, Reader x, int length)
    throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateCharacterStream(String columnName, Reader reader, int length)
    throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateCharacterStream(int columnIndex, Reader x, long length)
    throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateCharacterStream(String columnLabel, Reader reader,
                                    long length) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateClob(String columnName, Clob x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateDate(String columnName, Date x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateDouble(String columnName, double x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateFloat(String columnName, float x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateInt(String columnName, int x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateLong(String columnName, long x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateNCharacterStream(String columnLabel, Reader reader,
                                     long length) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateNClob(int columnIndex, NClob clob) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateNClob(String columnLabel, NClob clob) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateNString(int columnIndex, String string) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateNString(String columnLabel, String string) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateNull(String columnName) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateObject(String columnName, Object x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateObject(String columnName, Object x, int scale) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateRef(String columnName, Ref x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateRow() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateShort(String columnName, short x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateString(String columnName, String x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateTime(String columnName, Time x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  public void close() throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean wasNull() throws SQLException {
    return wasNull;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException("Method not supported ExploreBaseResultSet " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }
}
