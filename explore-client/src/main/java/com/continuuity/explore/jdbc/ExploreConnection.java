package com.continuuity.explore.jdbc;

import com.continuuity.explore.client.ExploreClient;
import com.continuuity.explore.jdbc.meta.ExploreDatabaseMetaData;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

/**
 * Explore JDBC connection.
 *
 * A connection is made using a client that does not keep state. Therefore, closing a connection
 * will not affect executing statements, and results will not be lost.
 */
public class ExploreConnection implements Connection {

  private final ExploreClient exploreClient;
  private boolean isClosed = false;
  private SQLWarning warningChain = null;

  public ExploreConnection(ExploreClient exploreClient) {
    this.exploreClient = exploreClient;
  }

  @Override
  public Statement createStatement() throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't create Statement, connection is closed");
    }
    return new ExploreStatement(this, exploreClient);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't create Statement, connection is closed");
    }
    return new ExplorePreparedStatement(this, exploreClient, sql);
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new ExploreDatabaseMetaData(exploreClient);
  }

  @Override
  public void close() throws SQLException {
    // Close is a no-op, since the client does not keep a state
    isClosed = true;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    // We don't (yet) support write, but with Hive, every statement is auto commit.
    return true;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  @Override
  public CallableStatement prepareCall(String s) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public String nativeSQL(String s) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public void setAutoCommit(boolean b) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public void commit() throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public void rollback() throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public void setReadOnly(boolean b) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public void setCatalog(String s) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public String getCatalog() throws SQLException {
    return "";
  }

  @Override
  public void setTransactionIsolation(int i) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    // The resultSetType can only be TYPE_FORWARD_ONLY and concurrency CONCUR_READ_ONLY
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String s, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    // The resultSetType can only be TYPE_FORWARD_ONLY and concurrency CONCUR_READ_ONLY
    if (isClosed) {
      throw new SQLException("Can't create Statement, connection is closed");
    }
    return new ExplorePreparedStatement(this, exploreClient, s);
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i2) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> stringClassMap) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public void setHoldability(int i) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public Savepoint setSavepoint(String s) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    // The resultSetType can only be TYPE_FORWARD_ONLY and concurrency CONCUR_READ_ONLY
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String s, int resultSetType, int resultSetConcurrency,
                                            int resultSetHoldability) throws SQLException {
    // The resultSetType can only be TYPE_FORWARD_ONLY and concurrency CONCUR_READ_ONLY
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i2, int i3) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public PreparedStatement prepareStatement(String s, int autoGeneratedKeys) throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't create Statement, connection is closed");
    }
    return new ExplorePreparedStatement(this, exploreClient, s);
  }

  @Override
  public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public boolean isValid(int i) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public void setClientInfo(String s, String s2) throws SQLClientInfoException {
    throw new SQLClientInfoException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber(), null);
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    throw new SQLClientInfoException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber(), null);
  }

  @Override
  public String getClientInfo(String s) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public Array createArrayOf(String s, Object[] objects) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public Struct createStruct(String s, Object[] objects) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    throw new SQLException("Method not supported ExploreConnection " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }
}
