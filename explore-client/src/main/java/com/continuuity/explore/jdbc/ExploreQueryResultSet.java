package com.continuuity.explore.jdbc;

import com.continuuity.explore.service.Explore;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ExploreQueryResultSet implements ResultSet {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreQueryResultSet.class);
  private static final int NEXT_RESULTS = 1000;

  private boolean isClosed = false;

  private boolean hasMoreResults = true;
  private Iterator<Row> rowsItr;
  private Row currentRow;

  private final Explore exploreClient;
  private final Statement statement;
  private final Handle stmtHandle;

  public ExploreQueryResultSet(Explore exploreClient, Statement statement, Handle stmtHandle) {
    this.exploreClient = exploreClient;
    this.statement = statement;
    this.stmtHandle = stmtHandle;
  }

  @Override
  public boolean next() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
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
      List<Row> fetchedRows = exploreClient.nextResults(stmtHandle, NEXT_RESULTS);
      if (fetchedRows.isEmpty()) {
        hasMoreResults = false;
        currentRow = null;
        return false;
      }
      rowsItr = fetchedRows.iterator();
      if (rowsItr.hasNext()) {
        currentRow = rowsItr.next();
        return true;
      }
      // Should not go there - as the previous isEmpty should already catch this case
      return false;
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
    if (this.statement != null && (this.statement instanceof ExploreStatement)) {
      ExploreStatement s = (ExploreStatement) this.statement;
      s.closeClientOperation();
    }
    // Need reset during re-open when needed
    isClosed = true;
  }

  @Override
  public boolean wasNull() throws SQLException {
    return false;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    // Columns count starts at 1
    List<Object> columns = currentRow.getColumns();
    if (columns.isEmpty()) {
      throw new SQLException("RowSet does not contain any columns!");
    }
    if (columnIndex <= 0 || columnIndex > columns.size()) {
      throw new SQLException("Invalid columnIndex: " + columnIndex);
    }
    Object value = columns.get(columnIndex - 1);
    if (value instanceof String) {
      return (String) value;
    }
    // Improve that
    return value.toString();
  }

  @Override
  public boolean getBoolean(int i) throws SQLException {
    return false;
  }

  @Override
  public byte getByte(int i) throws SQLException {
    return 0;
  }

  @Override
  public short getShort(int i) throws SQLException {
    return 0;
  }

  @Override
  public int getInt(int i) throws SQLException {
    return 0;
  }

  @Override
  public long getLong(int i) throws SQLException {
    return 0;
  }

  @Override
  public float getFloat(int i) throws SQLException {
    return 0;
  }

  @Override
  public double getDouble(int i) throws SQLException {
    return 0;
  }

  @Override
  public BigDecimal getBigDecimal(int i, int i2) throws SQLException {
    return null;
  }

  @Override
  public byte[] getBytes(int i) throws SQLException {
    return new byte[0];
  }

  @Override
  public Date getDate(int i) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(int i) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(int i) throws SQLException {
    return null;
  }

  @Override
  public InputStream getAsciiStream(int i) throws SQLException {
    return null;
  }

  @Override
  public InputStream getUnicodeStream(int i) throws SQLException {
    return null;
  }

  @Override
  public InputStream getBinaryStream(int i) throws SQLException {
    return null;
  }

  @Override
  public String getString(String s) throws SQLException {
    return null;
  }

  @Override
  public boolean getBoolean(String s) throws SQLException {
    return false;
  }

  @Override
  public byte getByte(String s) throws SQLException {
    return 0;
  }

  @Override
  public short getShort(String s) throws SQLException {
    return 0;
  }

  @Override
  public int getInt(String s) throws SQLException {
    return 0;
  }

  @Override
  public long getLong(String s) throws SQLException {
    return 0;
  }

  @Override
  public float getFloat(String s) throws SQLException {
    return 0;
  }

  @Override
  public double getDouble(String s) throws SQLException {
    return 0;
  }

  @Override
  public BigDecimal getBigDecimal(String s, int i) throws SQLException {
    return null;
  }

  @Override
  public byte[] getBytes(String s) throws SQLException {
    return new byte[0];
  }

  @Override
  public Date getDate(String s) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(String s) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(String s) throws SQLException {
    return null;
  }

  @Override
  public InputStream getAsciiStream(String s) throws SQLException {
    return null;
  }

  @Override
  public InputStream getUnicodeStream(String s) throws SQLException {
    return null;
  }

  @Override
  public InputStream getBinaryStream(String s) throws SQLException {
    return null;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public String getCursorName() throws SQLException {
    return null;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return null;
  }

  @Override
  public Object getObject(int i) throws SQLException {
    return null;
  }

  @Override
  public Object getObject(String s) throws SQLException {
    return null;
  }

  @Override
  public int findColumn(String s) throws SQLException {
    return 0;
  }

  @Override
  public Reader getCharacterStream(int i) throws SQLException {
    return null;
  }

  @Override
  public Reader getCharacterStream(String s) throws SQLException {
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(int i) throws SQLException {
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(String s) throws SQLException {
    return null;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return false;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return false;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return false;
  }

  @Override
  public boolean isLast() throws SQLException {
    return false;
  }

  @Override
  public void beforeFirst() throws SQLException {

  }

  @Override
  public void afterLast() throws SQLException {

  }

  @Override
  public boolean first() throws SQLException {
    return false;
  }

  @Override
  public boolean last() throws SQLException {
    return false;
  }

  @Override
  public int getRow() throws SQLException {
    return 0;
  }

  @Override
  public boolean absolute(int i) throws SQLException {
    return false;
  }

  @Override
  public boolean relative(int i) throws SQLException {
    return false;
  }

  @Override
  public boolean previous() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int i) throws SQLException {

  }

  @Override
  public int getFetchDirection() throws SQLException {
    return 0;
  }

  @Override
  public void setFetchSize(int i) throws SQLException {

  }

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getType() throws SQLException {
    return 0;
  }

  @Override
  public int getConcurrency() throws SQLException {
    return 0;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public void updateNull(int i) throws SQLException {

  }

  @Override
  public void updateBoolean(int i, boolean b) throws SQLException {

  }

  @Override
  public void updateByte(int i, byte b) throws SQLException {

  }

  @Override
  public void updateShort(int i, short i2) throws SQLException {

  }

  @Override
  public void updateInt(int i, int i2) throws SQLException {

  }

  @Override
  public void updateLong(int i, long l) throws SQLException {

  }

  @Override
  public void updateFloat(int i, float v) throws SQLException {

  }

  @Override
  public void updateDouble(int i, double v) throws SQLException {

  }

  @Override
  public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {

  }

  @Override
  public void updateString(int i, String s) throws SQLException {

  }

  @Override
  public void updateBytes(int i, byte[] bytes) throws SQLException {

  }

  @Override
  public void updateDate(int i, Date date) throws SQLException {

  }

  @Override
  public void updateTime(int i, Time time) throws SQLException {

  }

  @Override
  public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {

  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream, int i2) throws SQLException {

  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream, int i2) throws SQLException {

  }

  @Override
  public void updateCharacterStream(int i, Reader reader, int i2) throws SQLException {

  }

  @Override
  public void updateObject(int i, Object o, int i2) throws SQLException {

  }

  @Override
  public void updateObject(int i, Object o) throws SQLException {

  }

  @Override
  public void updateNull(String s) throws SQLException {

  }

  @Override
  public void updateBoolean(String s, boolean b) throws SQLException {

  }

  @Override
  public void updateByte(String s, byte b) throws SQLException {

  }

  @Override
  public void updateShort(String s, short i) throws SQLException {

  }

  @Override
  public void updateInt(String s, int i) throws SQLException {

  }

  @Override
  public void updateLong(String s, long l) throws SQLException {

  }

  @Override
  public void updateFloat(String s, float v) throws SQLException {

  }

  @Override
  public void updateDouble(String s, double v) throws SQLException {

  }

  @Override
  public void updateBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {

  }

  @Override
  public void updateString(String s, String s2) throws SQLException {

  }

  @Override
  public void updateBytes(String s, byte[] bytes) throws SQLException {

  }

  @Override
  public void updateDate(String s, Date date) throws SQLException {

  }

  @Override
  public void updateTime(String s, Time time) throws SQLException {

  }

  @Override
  public void updateTimestamp(String s, Timestamp timestamp) throws SQLException {

  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream, int i) throws SQLException {

  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream, int i) throws SQLException {

  }

  @Override
  public void updateCharacterStream(String s, Reader reader, int i) throws SQLException {

  }

  @Override
  public void updateObject(String s, Object o, int i) throws SQLException {

  }

  @Override
  public void updateObject(String s, Object o) throws SQLException {

  }

  @Override
  public void insertRow() throws SQLException {

  }

  @Override
  public void updateRow() throws SQLException {

  }

  @Override
  public void deleteRow() throws SQLException {

  }

  @Override
  public void refreshRow() throws SQLException {

  }

  @Override
  public void cancelRowUpdates() throws SQLException {

  }

  @Override
  public void moveToInsertRow() throws SQLException {

  }

  @Override
  public void moveToCurrentRow() throws SQLException {

  }

  @Override
  public Statement getStatement() throws SQLException {
    return null;
  }

  @Override
  public Object getObject(int i, Map<String, Class<?>> stringClassMap) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(int i) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(int i) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(int i) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(int i) throws SQLException {
    return null;
  }

  @Override
  public Object getObject(String s, Map<String, Class<?>> stringClassMap) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(String s) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(String s) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(String s) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(String s) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(int i, Calendar calendar) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(String s, Calendar calendar) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(int i, Calendar calendar) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(String s, Calendar calendar) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(String s, Calendar calendar) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(int i) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(String s) throws SQLException {
    return null;
  }

  @Override
  public void updateRef(int i, Ref ref) throws SQLException {

  }

  @Override
  public void updateRef(String s, Ref ref) throws SQLException {

  }

  @Override
  public void updateBlob(int i, Blob blob) throws SQLException {

  }

  @Override
  public void updateBlob(String s, Blob blob) throws SQLException {

  }

  @Override
  public void updateClob(int i, Clob clob) throws SQLException {

  }

  @Override
  public void updateClob(String s, Clob clob) throws SQLException {

  }

  @Override
  public void updateArray(int i, Array array) throws SQLException {

  }

  @Override
  public void updateArray(String s, Array array) throws SQLException {

  }

  @Override
  public RowId getRowId(int i) throws SQLException {
    return null;
  }

  @Override
  public RowId getRowId(String s) throws SQLException {
    return null;
  }

  @Override
  public void updateRowId(int i, RowId rowId) throws SQLException {

  }

  @Override
  public void updateRowId(String s, RowId rowId) throws SQLException {

  }

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void updateNString(int i, String s) throws SQLException {

  }

  @Override
  public void updateNString(String s, String s2) throws SQLException {

  }

  @Override
  public void updateNClob(int i, NClob nClob) throws SQLException {

  }

  @Override
  public void updateNClob(String s, NClob nClob) throws SQLException {

  }

  @Override
  public NClob getNClob(int i) throws SQLException {
    return null;
  }

  @Override
  public NClob getNClob(String s) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(int i) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(String s) throws SQLException {
    return null;
  }

  @Override
  public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {

  }

  @Override
  public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {

  }

  @Override
  public String getNString(int i) throws SQLException {
    return null;
  }

  @Override
  public String getNString(String s) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(int i) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(String s) throws SQLException {
    return null;
  }

  @Override
  public void updateNCharacterStream(int i, Reader reader, long l) throws SQLException {

  }

  @Override
  public void updateNCharacterStream(String s, Reader reader, long l) throws SQLException {

  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream, long l) throws SQLException {

  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream, long l) throws SQLException {

  }

  @Override
  public void updateCharacterStream(int i, Reader reader, long l) throws SQLException {

  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream, long l) throws SQLException {

  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream, long l) throws SQLException {

  }

  @Override
  public void updateCharacterStream(String s, Reader reader, long l) throws SQLException {

  }

  @Override
  public void updateBlob(int i, InputStream inputStream, long l) throws SQLException {

  }

  @Override
  public void updateBlob(String s, InputStream inputStream, long l) throws SQLException {

  }

  @Override
  public void updateClob(int i, Reader reader, long l) throws SQLException {

  }

  @Override
  public void updateClob(String s, Reader reader, long l) throws SQLException {

  }

  @Override
  public void updateNClob(int i, Reader reader, long l) throws SQLException {

  }

  @Override
  public void updateNClob(String s, Reader reader, long l) throws SQLException {

  }

  @Override
  public void updateNCharacterStream(int i, Reader reader) throws SQLException {

  }

  @Override
  public void updateNCharacterStream(String s, Reader reader) throws SQLException {

  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream) throws SQLException {

  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream) throws SQLException {

  }

  @Override
  public void updateCharacterStream(int i, Reader reader) throws SQLException {

  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream) throws SQLException {

  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream) throws SQLException {

  }

  @Override
  public void updateCharacterStream(String s, Reader reader) throws SQLException {

  }

  @Override
  public void updateBlob(int i, InputStream inputStream) throws SQLException {

  }

  @Override
  public void updateBlob(String s, InputStream inputStream) throws SQLException {

  }

  @Override
  public void updateClob(int i, Reader reader) throws SQLException {

  }

  @Override
  public void updateClob(String s, Reader reader) throws SQLException {

  }

  @Override
  public void updateNClob(int i, Reader reader) throws SQLException {

  }

  @Override
  public void updateNClob(String s, Reader reader) throws SQLException {

  }

  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    return false;
  }
}
