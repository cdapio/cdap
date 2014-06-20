package com.continuuity.explore.jdbc;

import com.continuuity.explore.client.ExploreClientUtil;
import com.continuuity.explore.service.Explore;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Status;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ExploreStatement implements Statement {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreStatement.class);
  private static final int MAX_POLL_TRIES = 1000000;

  /**
   * We need to keep a reference to the result set to support the following:
   * <code>
   *  statement.execute(String sql);
   *  statement.getResultSet();
   * </code>.
   */
  private ResultSet resultSet = null;

  /**
   * Keep state so we can fail certain calls made after close().
   */
  private boolean isClosed = false;

  private Handle stmtHandle = null;

  private final Explore exploreClient;

  public ExploreStatement(Explore exploreClient) {
    this.exploreClient = exploreClient;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
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

    // TODO in future, the polling logic should be in another SyncExploreClient
    try {
      stmtHandle = exploreClient.execute(sql);
      Status status = ExploreClientUtil.waitForCompletionStatus(exploreClient, stmtHandle, 300,
                                                                TimeUnit.MILLISECONDS, MAX_POLL_TRIES);

      if (status.getStatus() != Status.OpStatus.FINISHED) {
        throw new SQLException(String.format("Statement '%s' execution did not finish successfully. " +
                                             "Got final state - %s", sql, status.getStatus().toString()));
      }
      resultSet = new ExploreQueryResultSet(exploreClient, this, stmtHandle);
      return status.hasResults();
    } catch (HandleNotFoundException e) {
      // Cannot happen unless explore server restarted.
      LOG.error("Error running enable explore", e);
      throw Throwables.propagate(e);
    } catch (InterruptedException e) {
      LOG.error("Caught exception", e);
      Thread.currentThread().interrupt();
      // TODO is this the correct behavior?
      return false;
    } catch (ExploreException e) {
      LOG.error("Caught exception", e);
      throw new SQLException(e);
    }
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return null;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    execute(sql);
    return 0;
  }

  public void closeClientOperation() throws SQLException {
    if (stmtHandle != null) {
      try {
        exploreClient.close(stmtHandle);
      } catch (HandleNotFoundException e) {
        LOG.error("Ignoring cannot find handle during close.");
      } catch (ExploreException e) {
        throw new SQLException(e.toString(), e);
      }
    }
    stmtHandle = null;
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }

    closeClientOperation();
    resultSet = null;
    isClosed = true;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxFieldSize(int i) throws SQLException {

  }

  @Override
  public int getMaxRows() throws SQLException {
    return 0;
  }

  @Override
  public void setMaxRows(int i) throws SQLException {

  }

  @Override
  public void setEscapeProcessing(boolean b) throws SQLException {

  }

  @Override
  public int getQueryTimeout() throws SQLException {
    return 0;
  }

  @Override
  public void setQueryTimeout(int i) throws SQLException {

  }

  @Override
  public void cancel() throws SQLException {

  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public void setCursorName(String s) throws SQLException {

  }

  @Override
  public int getUpdateCount() throws SQLException {
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
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
  public int getResultSetConcurrency() throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return 0;
  }

  @Override
  public void addBatch(String s) throws SQLException {

  }

  @Override
  public void clearBatch() throws SQLException {

  }

  @Override
  public int[] executeBatch() throws SQLException {
    return new int[0];
  }

  @Override
  public Connection getConnection() throws SQLException {
    return null;
  }

  @Override
  public boolean getMoreResults(int i) throws SQLException {
    // In case our client.execute returned more than one list of results, which is never the case
    throw new SQLException("Method not supported");
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    return null;
  }

  @Override
  public int executeUpdate(String s, int i) throws SQLException {
    return 0;
  }

  @Override
  public int executeUpdate(String s, int[] ints) throws SQLException {
    return 0;
  }

  @Override
  public int executeUpdate(String s, String[] strings) throws SQLException {
    return 0;
  }

  @Override
  public boolean execute(String s, int i) throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String s, int[] ints) throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String s, String[] strings) throws SQLException {
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void setPoolable(boolean b) throws SQLException {

  }

  @Override
  public boolean isPoolable() throws SQLException {
    return false;
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
