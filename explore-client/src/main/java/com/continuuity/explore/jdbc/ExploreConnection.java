package com.continuuity.explore.jdbc;

import com.continuuity.explore.client.ExploreClient;
import com.continuuity.explore.client.ExternalAsyncExploreClient;
import com.continuuity.explore.service.Explore;

import java.net.URI;
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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

/**
 * Explore JDBC connection.
 */
public class ExploreConnection implements Connection {

  private final ExploreClient exploreClient;
  private boolean isClosed = false;

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
  public void close() throws SQLException {
    // Close is a no-op, since the client does not keep a state
    isClosed = true;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public CallableStatement prepareCall(String s) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public String nativeSQL(String s) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setAutoCommit(boolean b) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void commit() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void rollback() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    // TODO Hive jdbc driver supports that
    throw new SQLException("Method not supported");
  }

  @Override
  public void setReadOnly(boolean b) throws SQLException {

  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setCatalog(String s) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public String getCatalog() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setTransactionIsolation(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Statement createStatement(int i, int i2) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i, int i2) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i2) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> stringClassMap) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setHoldability(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Savepoint setSavepoint(String s) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Statement createStatement(int i, int i2, int i3) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i, int i2, int i3) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public CallableStatement prepareCall(String s, int i, int i2, int i3) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public PreparedStatement prepareStatement(String s, int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean isValid(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public void setClientInfo(String s, String s2) throws SQLClientInfoException {

  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {

  }

  @Override
  public String getClientInfo(String s) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Array createArrayOf(String s, Object[] objects) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public Struct createStruct(String s, Object[] objects) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    throw new SQLException("Method not supported");
  }
}
