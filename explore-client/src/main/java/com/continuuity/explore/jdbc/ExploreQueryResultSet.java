package com.continuuity.explore.jdbc;

import com.continuuity.explore.service.ColumnDesc;
import com.continuuity.explore.service.Explore;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Result;

import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Result set created by an {@link ExploreStatement}, containing the results of a query made to the Explore service.
 */
public class ExploreQueryResultSet implements ResultSet {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreQueryResultSet.class);

  private boolean isClosed = false;
  private int fetchSize;

  private boolean hasMoreResults = true;
  private Iterator<Result> rowsItr;
  private Result currentRow;
  private ExploreResultSetMetaData metaData;

  private boolean wasNull = false;
  private ExploreStatement statement;
  private Handle stmtHandle;
  private Explore exploreClient;

  public ExploreQueryResultSet(Explore exploreClient, ExploreStatement statement, Handle stmtHandle)
      throws SQLException {
    this.exploreClient = exploreClient;
    this.statement = statement;
    this.stmtHandle = stmtHandle;
    this.fetchSize = statement.getFetchSize();
  }

  @Override
  public boolean next() throws SQLException {
    if (isClosed) {
      throw new SQLException("ResultSet is closed");
    }

    if (!hasMoreResults) {
      return false;
    }

    if (rowsItr != null && rowsItr.hasNext()) {
      currentRow = rowsItr.next();
      return true;
    }

    try {
      if (stmtHandle == null) {
        throw new SQLException("Handle is null.");
      }
      List<Result> fetchedRows;
      fetchedRows = exploreClient.nextResults(stmtHandle, fetchSize);
      rowsItr = fetchedRows.iterator();
      if (!rowsItr.hasNext()) {
        hasMoreResults = false;
        currentRow = null;
        return false;
      }
      currentRow = rowsItr.next();
      return true;
    } catch (HandleNotFoundException e) {
      LOG.error("Could not fetch results with handle {}", stmtHandle);
      throw new SQLException("Could not fetch results with handle " + stmtHandle, e);
    } catch (ExploreException e) {
      LOG.error("Caught exception", e);
      throw new SQLException(e);
    }
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      // No-op
      return;
    }
    try {
      statement.closeClientOperation();
    } finally {
      exploreClient = null;
      stmtHandle = null;
      statement = null;
      isClosed = true;
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    if (isClosed) {
      throw new SQLException("ResultSet is closed");
    }
    if (currentRow == null) {
      throw new SQLException("No row found.");
    }
    List<Object> columns = currentRow.getColumns();
    if (columns.isEmpty()) {
      throw new SQLException("RowSet does not contain any columns!");
    }
    if (columnIndex < 1 || columnIndex > columns.size()) {
      throw new SQLException("Invalid columnIndex: " + columnIndex);
    }

    int columnType = getMetaData().getColumnType(columnIndex);
    try {
      Object evaluated = evaluate(columnType, columns.get(columnIndex - 1));
      wasNull = (evaluated == null);
      return evaluated;
    } catch (Exception e) {
      e.printStackTrace();
      throw new SQLException("Unrecognized column type:" + columnType, e);
    }
  }

  @Override
  public Object getObject(String s) throws SQLException {
    return getObject(findColumn(s));
  }

  private Object evaluate(int sqlType, Object value) {
    if (value == null) {
      return null;
    }
    switch (sqlType) {
      case Types.BINARY:
        if (value instanceof String) {
          return ((String) value).getBytes();
        }
        return value;
      case Types.TIMESTAMP:
        return Timestamp.valueOf((String) value);
      case Types.DECIMAL:
        return new BigDecimal((String) value);
      case Types.DATE:
        return Date.valueOf((String) value);
      case Types.ARRAY:
      case Types.JAVA_OBJECT:
      case Types.STRUCT:
        // todo: returns json string. should recreate object from it?
        // Use UDTs in the future to allow users to define custom types
        // so we know how to recreate the object (see Connection interface javadoc)
        return value;
      default:
        return value;
    }
  }

  @Override
  public boolean wasNull() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    return wasNull;
  }

  @Override
  public int getFetchSize() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    return fetchSize;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    fetchSize = rows;
  }

  @Override
  public int getType() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    // Our Explore Client interface only allows to move forward in the results
    return TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    // We cannot update a ExploreResultSet object
    return CONCUR_READ_ONLY;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    if (metaData == null) {
      try {
        List<ColumnDesc> columnDescs = exploreClient.getResultSchema(stmtHandle);
        metaData = new ExploreResultSetMetaData(columnDescs);
      } catch (ExploreException e) {
        LOG.error("Caught exception", e);
        throw new SQLException(e);
      } catch (HandleNotFoundException e) {
        LOG.error("Handle not found when retrieving result set meta data", e);
        throw new SQLException("Handle not found when retrieving result set meta data", e);
      }
    }
    return metaData;
  }

  @Override
  public int findColumn(String name) throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    if (metaData == null) {
      getMetaData();
    }
    // Column names are case insensitive, as per the ResultSet interface javadoc
    return metaData.getColumnPosition(name.toLowerCase());
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    Object value = getObject(columnIndex);
    if (wasNull) {
      return null;
    }
    if (value instanceof byte[]) {
      return new String((byte[]) value);
    }
    return value.toString();
  }

  @Override
  public String getString(String name) throws SQLException {
    return getString(findColumn(name));
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).intValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Integer.parseInt((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex + " to integer" + e.toString(), e);
    }
  }

  @Override
  public int getInt(String s) throws SQLException {
    return getInt(findColumn(s));
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (Boolean.class.isInstance(obj)) {
      return (Boolean) obj;
    } else if (obj == null) {
      return false;
    } else if (Number.class.isInstance(obj)) {
      return ((Number) obj).intValue() != 0;
    } else if (String.class.isInstance(obj)) {
      return !((String) obj).equals("0");
    }
    throw new SQLException("Cannot convert column " + columnIndex + " to boolean");
  }

  @Override
  public boolean getBoolean(String columnName) throws SQLException {
    return getBoolean(findColumn(columnName));
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (Number.class.isInstance(obj)) {
      return ((Number) obj).byteValue();
    } else if (obj == null) {
      return 0;
    }
    throw new SQLException("Cannot convert column " + columnIndex + " to byte");
  }

  @Override
  public byte getByte(String s) throws SQLException {
    return getByte(findColumn(s));
  }

  @Override
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
      throw new SQLException("Cannot convert column " + columnIndex + " to short: " + e.toString(), e);
    }
  }

  @Override
  public short getShort(String s) throws SQLException {
    return getShort(findColumn(s));
  }

  @Override
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
      throw new SQLException("Cannot convert column " + columnIndex + " to long: " + e.toString(), e);
    }
  }

  @Override
  public long getLong(String s) throws SQLException {
    return getLong(findColumn(s));
  }

  @Override
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
      throw new SQLException("Cannot convert column " + columnIndex + " to float: " + e.toString(), e);
    }
  }

  @Override
  public float getFloat(String columnName) throws SQLException {
    return getFloat(findColumn(columnName));
  }

  @Override
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
      throw new SQLException("Cannot convert column " + columnIndex + " to double: " + e.toString(), e);
    }
  }

  @Override
  public double getDouble(String s) throws SQLException {
    return getDouble(findColumn(s));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    Object val = getObject(columnIndex);
    if (val == null || val instanceof BigDecimal) {
      return (BigDecimal) val;
    }
    throw new SQLException("Illegal conversion");
  }

  @Override
  public BigDecimal getBigDecimal(String s) throws SQLException {
    return getBigDecimal(findColumn(s));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    MathContext mc = new MathContext(scale);
    return getBigDecimal(columnIndex).round(mc);
  }

  @Override
  public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnName), scale);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      return (byte[]) obj;
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex + " to double: " + e.toString(), e);
    }
  }

  @Override
  public byte[] getBytes(String s) throws SQLException {
    return getBytes(findColumn(s));
  }

  @Override
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
      throw new SQLException("Cannot convert column " + columnIndex + " to date: " + e.toString(), e);
    }
    // If we fell through to here this is not a valid type conversion
    throw new SQLException("Cannot convert column " + columnIndex + " to date: Illegal conversion");
  }

  @Override
  public Date getDate(String s) throws SQLException {
    return getDate(findColumn(s));
  }

  @Override
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

  @Override
  public Timestamp getTimestamp(String s) throws SQLException {
    return getTimestamp(findColumn(s));
  }

  @Override
  public Time getTime(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getAsciiStream(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getUnicodeStream(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getBinaryStream(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Time getTime(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getAsciiStream(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getUnicodeStream(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getBinaryStream(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getCharacterStream(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getCharacterStream(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isLast() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void afterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean first() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean last() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getRow() throws SQLException {
    // As per the javadoc:
    // "Support for the getRow method is optional for ResultSets with a result set type of TYPE_FORWARD_ONLY"
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean absolute(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean relative(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean previous() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setFetchDirection(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(int i, boolean b) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(int i, byte b) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(int i, short i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(int i, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(int i, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(int i, float v) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(int i, double v) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(int i, String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(int i, byte[] bytes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(int i, Date date) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(int i, Time time) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int i, Reader reader, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int i, Object o, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int i, Object o) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(String s, boolean b) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(String s, byte b) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(String s, short i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(String s, int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(String s, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(String s, float v) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(String s, double v) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(String s, String s2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(String s, byte[] bytes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(String s, Date date) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(String s, Time time) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(String s, Timestamp timestamp) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream, int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream, int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String s, Reader reader, int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String s, Object o, int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String s, Object o) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Statement getStatement() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Object getObject(int i, Map<String, Class<?>> stringClassMap) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Ref getRef(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob getBlob(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Object getObject(String s, Map<String, Class<?>> stringClassMap) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Ref getRef(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob getBlob(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Date getDate(int i, Calendar calendar) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Date getDate(String s, Calendar calendar) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Time getTime(int i, Calendar calendar) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Time getTime(String s, Calendar calendar) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Timestamp getTimestamp(String s, Calendar calendar) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public URL getURL(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public URL getURL(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(int i, Ref ref) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(String s, Ref ref) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int i, Blob blob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String s, Blob blob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int i, Clob clob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String s, Clob clob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(int i, Array array) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(String s, Array array) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(int i, RowId rowId) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(String s, RowId rowId) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(int i, String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(String s, String s2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int i, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String s, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getNCharacterStream(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getNCharacterStream(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int i, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String s, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int i, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String s, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int i, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String s, InputStream inputStream, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int i, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String s, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int i, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String s, Reader reader, long l) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int i, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String s, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int i, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String s, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int i, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String s, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int i, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String s, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int i, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String s, Reader reader) throws SQLException {
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

  public <T> T getObject(String columnLabel, Class<T> type)  throws SQLException {
    //JDK 1.7
    throw new SQLFeatureNotSupportedException();
  }

  public <T> T getObject(int columnIndex, Class<T> type)  throws SQLException {
    //JDK 1.7
    throw new SQLFeatureNotSupportedException();
  }
}
