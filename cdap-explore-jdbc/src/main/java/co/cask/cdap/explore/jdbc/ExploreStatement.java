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
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.explore.service.UnexpectedQueryStatusException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryStatus;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * CDAP JDBC Statement. At most one {@link ExploreResultSet} object can be produced by instances
 * of this class.
 */
public class ExploreStatement implements Statement {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreStatement.class);

  private int fetchSize = 50;

  /**
   * We need to keep a reference to the result set to support the following:
   * <code>
   *  statement.execute(String sql);
   *  statement.getResultSet();
   * </code>.
   */
  private ResultSet resultSet = null;

  /**
   * Sets the limit for the maximum number of rows that any ResultSet object produced by this
   * Statement can contain to the given number. If the limit is exceeded, the excess rows
   * are silently dropped. The value must be >= 0, and 0 means there is not limit.
   */
  private int maxRows = 0;

  /**
   * Add SQLWarnings to the warningChain if needed.
   */
  private SQLWarning warningChain = null;

  private volatile boolean isClosed = false;
  private volatile ListenableFuture<ExploreExecutionResult> futureResults = null;

  private Connection connection;
  private ExploreClient exploreClient;
  private final Id.Namespace namespace;

  ExploreStatement(Connection connection, ExploreClient exploreClient, String namespace) {
    this.connection = connection;
    this.exploreClient = exploreClient;
    this.namespace = Id.Namespace.from(namespace);
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

  /**
   * Executes a query and wait until it is finished, but does not close the session.
   */
  @Override
  public boolean execute(String sql) throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't execute after statement has been closed");
    }
    if (resultSet != null) {
      // As requested by the Statement interface javadoc, "All execution methods in the Statement interface
      // implicitly close a statement's current ResultSet object if an open one exists"
      resultSet.close();
      resultSet = null;
    }

    futureResults = exploreClient.submit(namespace, sql);
    try {
      resultSet = new ExploreResultSet(futureResults.get(), this, maxRows);
      // NOTE: Javadoc states: "returns false if the first result is an update count or there is no result"
      // Here we have a result, it may contain rows or may be empty, but it exists.
      return true;
    } catch (InterruptedException e) {
      LOG.error("Caught exception", e);
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException e) {
      Throwable t = Throwables.getRootCause(e);
      if (t instanceof HandleNotFoundException) {
        LOG.error("Error executing query", e);
        throw new SQLException("Unknown state");
      } else if (t instanceof UnexpectedQueryStatusException) {
        UnexpectedQueryStatusException sE = (UnexpectedQueryStatusException) t;
        if (QueryStatus.OpStatus.CANCELED.equals(sE.getStatus())) {
          // The query execution may have been canceled without calling futureResults.cancel(), using the right
          // REST endpoint with the handle for eg.
          return false;
        }
        throw new SQLException(String.format("Statement '%s' execution did not finish successfully. " +
                                             "Got final state - %s", sql, sE.getStatus().toString()));
      }
      LOG.error("Caught exception", e);
      throw new SQLException(Throwables.getRootCause(e));
    } catch (CancellationException e) {
      // If futureResults has been cancelled
      return false;
    }
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return resultSet;
  }

  @Override
  public int getMaxRows() throws SQLException {
    return maxRows;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    if (max < 0) {
      throw new SQLException("max rows must be >= 0");
    }
    maxRows = max;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return -1;
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
   * This method is not private to let {@link ExploreResultSet} access it when closing its results.
   */
  void closeClientOperation() throws SQLException {
    if (futureResults != null) {
      try {
        futureResults.cancel(true);
      } finally {
        futureResults = null;
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
    if (futureResults == null) {
      LOG.info("Trying to cancel with no query.");
      return;
    }
    boolean success = futureResults.cancel(true);
    if (!success) {
      throw new SQLException("Could not cancel query - query is cancelled: " + futureResults.isCancelled());
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
    return warningChain;
  }

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  @Override
  public void setCursorName(String s) throws SQLException {
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
