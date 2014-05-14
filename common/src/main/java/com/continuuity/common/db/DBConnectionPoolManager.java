package com.continuuity.common.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

/**
 * Implementation of very light-weight simple JDBC connection pool
 * manager.
 */
public class DBConnectionPoolManager {

  private static final Logger Log = LoggerFactory.getLogger(
    DBConnectionPoolManager.class
  );

  private ConnectionPoolDataSource       dataSource;
  private int                            maxConnections;
  private long                           timeoutMs;
  private Semaphore                      semaphore;
  private PoolConnectionEventListener    poolConnectionEventListener;

  // The following variables must only be accessed within synchronized blocks.
  // @GuardedBy("this") could by used in the future.

  /**
   * List of {@link PooledConnection} that are not active.
   */
  private LinkedList<PooledConnection>   recycledConnections;

  /**
   * Count of open connections in this pool.
   */
  private int                            activeConnections;

  /**
   * Set to true if this connection pool has been disposed.
   */
  private boolean                        isDisposed;

  /**
   * Purges the connections currently being closed instead of recycling.
   * true indicates to purge, false to recycle.
   */
  private boolean                        doPurgeConnection;

  /**
   * A PooledConnection which is currently within a
   * PooledConnection.getConnection() call, or null.
   */
  private PooledConnection               connectionInTransition;

  /**
   * Thrown in {@link #getConnection()} or {@link #getValidConnection()} when
   * no free connection becomes available within <code>timeout</code> seconds.
   */
  public static class TimeoutException extends RuntimeException {
    private static final long serialVersionUID = 1;

    public TimeoutException () {
      super("Timeout while waiting for a free database connection.");
    }

    public TimeoutException (String msg) {
      super(msg);
    }
  }

  /**
   * Constructs a DBConnectionPoolManager object with a timeout of 60 seconds.
   *
   * @param dataSource
   *    the data source for the connections.
   * @param maxConnections
   *    the maximum number of connections.
   */
  public DBConnectionPoolManager(ConnectionPoolDataSource dataSource,
                                 int maxConnections) {
    this(dataSource, maxConnections, 60); }

  /**
   * Constructs a DBConnectionPoolManager object.
   *
   * @param dataSource
   *    the data source for the connections.
   * @param maxConnections
   *    the maximum number of connections.
   * @param timeout
   *    the maximum time in seconds to wait for a free connection.
   */
  public DBConnectionPoolManager(ConnectionPoolDataSource dataSource,
                                 int maxConnections, int timeout) {
    this.dataSource = dataSource;
    this.maxConnections = maxConnections;
    this.timeoutMs = timeout * 1000L;

    if (maxConnections < 1) {
      throw new IllegalArgumentException("Invalid maxConnections value.");
    }

    semaphore = new Semaphore(maxConnections, true);
    recycledConnections = new LinkedList<PooledConnection>();
    poolConnectionEventListener = new PoolConnectionEventListener();
  }

  /**
   * Closes all unused pooled connections.
   */
  public synchronized void dispose() throws SQLException {
    if (isDisposed) {
      return;
    }

    isDisposed = true;
    SQLException e = null;

    while (!recycledConnections.isEmpty()) {
      PooledConnection pconn = recycledConnections.remove();
      try {
        pconn.close();
      } catch (SQLException e2) {
        if (e == null) {
          e = e2;
        }
      }
    }

    if (e != null) {
      throw e;
    }
  }

  // Due to a bug in checkstyle, it would emit false positives here of the form
  // "Unable to get class information for @throws tag '<exn>' (...)".
  // This comment disables that check up to the corresponding ON comments below

  // CHECKSTYLE OFF: @throws

  /**
   * Retrieves a connection from the connection pool.
   *
   * <p>If <code>maxConnections</code> connections are already in use, the method
   * waits until a connection becomes available or <code>timeout</code> seconds elapsed.
   * When the application is finished using the connection, it must close it
   * in order to return it to the pool.
   *
   * @return
   *    a new <code>Connection</code> object.
   * @throws TimeoutException
   *    when no connection becomes available within <code>timeout</code> seconds.
   */
  public Connection getConnection() throws SQLException {
    return getConnection2(timeoutMs);
  }

  // CHECKSTYLE ON

  private Connection getConnection2 (long timeoutMs) throws SQLException {

    // This routine is unsynchronized, because semaphore.tryAcquire() may block.
    synchronized (this) {
      if (isDisposed) {
        throw new IllegalStateException("Connection pool has been disposed.");
      }
    }

    try {
      if (!semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
        throw new TimeoutException();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for a " +
                                   "database connection.", e);
    }

    boolean ok = false;
    try {
      Connection conn = getConnection3();
      ok = true;
      return conn;
    } finally {
      if (!ok) {
        semaphore.release();
      }
    }
  }

  private synchronized Connection getConnection3() throws SQLException {
    // test again within synchronized lock
    if (isDisposed) {
      throw new IllegalStateException("Connection pool has been disposed.");
    }

    PooledConnection pconn;

    if (!recycledConnections.isEmpty()) {
      pconn = recycledConnections.remove();
    } else {
      pconn = dataSource.getPooledConnection();
      pconn.addConnectionEventListener(poolConnectionEventListener);
    }

    Connection conn;
    try {
      // The JDBC driver may call ConnectionEventListener.connectionErrorOccurred()
      // from within PooledConnection.getConnection(). To detect this within
      // disposeConnection(), we temporarily set connectionInTransition.
      connectionInTransition = pconn;
      conn = pconn.getConnection();
    } finally {
      connectionInTransition = null;
    }
    activeConnections++;
    assertInnerState();
    return conn;
  }

  // Due to a bug in checkstyle, it would emit false positives here of the form
  // "Unable to get class information for @throws tag '<exn>' (...)".
  // This comment disables that check up to the corresponding ON comments below

  // CHECKSTYLE OFF: @throws

  /**
   * Retrieves a connection from the connection pool and ensures that it is valid
   * by calling {@link Connection#isValid(int)}.
   *
   * <p>If a connection is not valid, the method tries to get another connection
   * until one is valid (or a timeout occurs).
   *
   * <p>Pooled connections may become invalid when e.g. the database server is
   * restarted.
   *
   * <p>This method is slower than {@link #getConnection()} because the JDBC
   * driver has to send an extra command to the database server to test the
   * connection.
   *
   * <p>This method requires Java 1.6 or newer.
   *
   * @throws TimeoutException
   *    when no valid connection becomes available within <code>timeout</code>
   *    seconds.
   */
  public Connection getValidConnection() throws TimeoutException {
    long time = System.currentTimeMillis();
    long timeoutTime = time + timeoutMs;
    int triesWithoutDelay = getInactiveConnections() + 1;

    while (true) {
      Connection conn = getValidConnection2(time, timeoutTime);
      if (conn != null) {
        return conn;
      }
      triesWithoutDelay--;
      if (triesWithoutDelay <= 0) {
        triesWithoutDelay = 0;
        try {
          Thread.sleep(250);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while waiting " +
                                       "for a valid database connection.", e);
        }
      }
      time = System.currentTimeMillis();
      if (time >= timeoutTime) {
        throw new TimeoutException("Timeout while waiting for a " +
                                     "valid database connection.");
      }
    }
  }

  // CHECKSTYLE ON

  private Connection getValidConnection2 (long time, long timeoutTime) {
    long rtime = Math.max(1, timeoutTime - time);
    Connection conn;

    try {
      conn = getConnection2(rtime);
    } catch (SQLException e) {
      return null;
    }

    rtime = timeoutTime - System.currentTimeMillis();
    int rtimeSecs = Math.max(1, (int) ((rtime + 999) / 1000));
    try {
      if (conn.isValid(rtimeSecs)) {
        return conn;
      }
    } catch (SQLException e) {

    }
    // This Exception should never occur. If it nevertheless occurs,
    // it's because of an error in the JDBC driver which we ignore and assume
    // that the connection is not valid. When isValid() returns false, the JDBC
    // driver should have already called  connectionErrorOccurred()
    // and the PooledConnection has been removed from the pool, i.e. the
    // PooledConnection will not be added to recycledConnections when
    // Connection.close() is called. But to be sure that this works even with
    // a faulty JDBC driver, we call purgeConnection().
    purgeConnection(conn);
    return null;
  }

  // Purges the PooledConnection associated with the passed Connection from
  // the connection pool.
  private synchronized void purgeConnection (Connection conn) {
    try {
      doPurgeConnection = true;
      // (A potential problem of this program logic is that setting the
      // doPurgeConnection flag has an effect only if the JDBC driver calls
      // connectionClosed() synchronously within Connection.close().)
      conn.close();
    } catch (SQLException e) {
      // ignore exception from close()
    } finally {
      doPurgeConnection = false;
    }
  }

  private synchronized void recycleConnection (PooledConnection pconn) {
    if (isDisposed || doPurgeConnection) {
      disposeConnection(pconn);
      return;
    }

    if (activeConnections <= 0) {
      throw new AssertionError();
    }

    activeConnections--;
    semaphore.release();
    recycledConnections.add(pconn);
    assertInnerState();
  }

  private synchronized void disposeConnection (PooledConnection pconn) {
    pconn.removeConnectionEventListener(poolConnectionEventListener);
    if (!recycledConnections.remove(pconn) && pconn != connectionInTransition) {
      // If the PooledConnection is not in the recycledConnections list
      // and is not currently within a PooledConnection.getConnection() call,
      // we assume that the connection was active.
      if (activeConnections <= 0) {
        throw new AssertionError();
      }
      activeConnections--;
      semaphore.release();
    }
    closeConnectionAndIgnoreException(pconn);
    assertInnerState();
  }

  private void closeConnectionAndIgnoreException (PooledConnection pconn) {
    try {
      pconn.close();
    } catch (SQLException e) {
      log("Error while closing database connection: " + e.toString());
    }
  }

  private void log (String msg) {
    Log.error(msg);
  }

  private synchronized void assertInnerState() {
    if (activeConnections < 0) {
      throw new AssertionError();
    }

    if (activeConnections + recycledConnections.size() > maxConnections) {
      throw new AssertionError();
    }

    if (activeConnections + semaphore.availablePermits() > maxConnections) {
      throw new AssertionError();
    }
  }

  private class PoolConnectionEventListener implements ConnectionEventListener {

    public void connectionClosed (ConnectionEvent event) {
      PooledConnection pconn = (PooledConnection) event.getSource();
      recycleConnection(pconn);
    }

    public void connectionErrorOccurred (ConnectionEvent event) {
      PooledConnection pconn = (PooledConnection) event.getSource();
      disposeConnection(pconn);
    }
  }

  /**
   * Returns the number of active (open) connections of this pool.
   *
   * <p>This is the number of <code>Connection</code> objects that have been
   * issued by {@link #getConnection()}, for which <code>Connection.close()</code>
   * has not yet been called.
   *
   * @return
   *    the number of active connections.
   **/
  public synchronized int getActiveConnections() {
    return activeConnections;
  }

  /**
   * Returns the number of inactive (unused) connections in this pool.
   *
   * <p>This is the number of internally kept recycled connections,
   * for which <code>Connection.close()</code> has been called and which
   * have not yet been reused.
   *
   * @return
   *    the number of inactive connections.
   **/
  public synchronized int getInactiveConnections() {
    return recycledConnections.size();
  }

}
