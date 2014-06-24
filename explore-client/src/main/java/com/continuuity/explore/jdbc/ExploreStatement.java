package com.continuuity.explore.jdbc;

import com.continuuity.explore.client.ExploreClientUtil;
import com.continuuity.explore.service.Explore;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * Reactor JDBC Statement. At most one {@link ExploreQueryResultSet} object can be produced by instances
 * of this class.
 */
public class ExploreStatement implements Statement {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreStatement.class);
  private static final int MAX_POLL_TRIES = 1000000;

  private int fetchSize = 50;

  /**
   * We need to keep a reference to the result set to support the following:
   * <code>
   *  statement.execute(String sql);
   *  statement.getResultSet();
   * </code>.
   */
  private volatile ResultSet resultSet = null;
  private volatile boolean isClosed = false;
  private volatile Handle stmtHandle = null;
  private volatile boolean stmtCompleted;

  private Connection connection;
  private Explore exploreClient;

  public ExploreStatement(Connection connection, Explore exploreClient) {
    this.connection = connection;
    this.exploreClient = exploreClient;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't execute after statement has been closed");
    }
    if (!execute(sql)) {
      throw new SQLException("The query did not generate a result set!");
    }
    return resultSet;
  }

  /*
   * Executes a query and wait until it is finished, but does not close the session.
   */
  @Override
  public boolean execute(String sql) throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't execute after statement has been closed");
    }
    stmtCompleted = false;
    if (resultSet != null) {
      // As requested by the Statement interface javadoc, "All execution methods in the Statement interface
      // implicitly close a statement's current ResultSet object if an open one exists"
      resultSet.close();
      resultSet = null;
    }

    // TODO in future, the polling logic should be in another SyncExploreClient
    try {
      stmtHandle = exploreClient.execute(sql);
      Status status = ExploreClientUtil.waitForCompletionStatus(exploreClient, stmtHandle, 200,
                                                                TimeUnit.MILLISECONDS, MAX_POLL_TRIES);
      stmtCompleted = true;
      switch (status.getStatus()) {
        case FINISHED:
          resultSet = new ExploreQueryResultSet(exploreClient, this, stmtHandle);
          // NOTE: Javadoc states: "returns false if the first result is an update count or there is no result"
          // Here we have a result, it may contain rows or may be empty, but it exists.
          return true;
        case CANCELED:
          return false;
        default:
          // Any other state can be considered as a "database" access error
          throw new SQLException(String.format("Statement '%s' execution did not finish successfully. " +
                                               "Got final state - %s", sql, status.getStatus().toString()));
      }
    } catch (HandleNotFoundException e) {
      // Cannot happen unless explore server restarted.
      LOG.error("Error executing query", e);
      throw new SQLException("Unknown state");
    } catch (InterruptedException e) {
      LOG.error("Caught exception", e);
      Thread.currentThread().interrupt();
      return false;
    } catch (ExploreException e) {
      LOG.error("Caught exception", e);
      throw new SQLException(e);
    }
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return resultSet;
  }

  @Override
  public void setFetchSize(int i) throws SQLException {
    fetchSize = i;
  }

  @Override
  public int getFetchSize() throws SQLException {
    return fetchSize;
  }

  /**
   * This method is not private to let {@link ExploreQueryResultSet} access it when closing its results.
   */
  void closeClientOperation() throws SQLException {
    if (stmtHandle != null) {
      try {
        exploreClient.close(stmtHandle);
      } catch (HandleNotFoundException e) {
        LOG.error("Ignoring cannot find handle during close.");
      } catch (ExploreException e) {
        LOG.error("Caught exception when closing statement", e);
        throw new SQLException(e.toString(), e);
      } finally {
        stmtHandle = null;
      }
    }
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }

    try {
      // As stated by ResultSet javadoc, "A ResultSet object is automatically closed when
      // the Statement object that generated it is closed"
      if (resultSet != null) {
        resultSet.close();
      }
      closeClientOperation();
    } finally {
      connection = null;
      exploreClient = null;
      resultSet = null;
      isClosed = true;
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public void cancel() throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't cancel after statement has been closed");
    }
    if (stmtHandle == null) {
      LOG.info("Trying to cancel with no query.");
      return;
    }
    try {
      exploreClient.cancel(stmtHandle);
    } catch (HandleNotFoundException e) {
      LOG.error("Ignoring cannot find handle during cancel.");
    } catch (ExploreException e) {
      LOG.error("Caught exception when closing statement", e);
      throw new SQLException(e.toString(), e);
    }
  }

  @Override
  public Connection getConnection() throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't get connection after statement has been closed");
    }
    return connection;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    // We don't support writes in explore yet
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setMaxFieldSize(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxRows() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setMaxRows(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setEscapeProcessing(boolean b) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setQueryTimeout(int i) throws SQLException {
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
  public void setCursorName(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getUpdateCount() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
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
  public int getResultSetConcurrency() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getResultSetType() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void addBatch(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean getMoreResults(int i) throws SQLException {
    // In case our client.execute returned more than one list of results, which is never the case
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String s, int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String s, int[] ints) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String s, String[] strings) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String s, int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String s, int[] ints) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String s, String[] strings) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setPoolable(boolean b) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isPoolable() throws SQLException {
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

  public void closeOnCompletion() throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException();
  }

  public boolean isCloseOnCompletion() throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException();
  }
}
