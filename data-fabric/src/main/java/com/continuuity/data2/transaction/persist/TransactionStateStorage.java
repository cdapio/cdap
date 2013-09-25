package com.continuuity.data2.transaction.persist;

import com.google.common.util.concurrent.Service;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Defines the common contract for persisting transaction state changes.
 */
public interface TransactionStateStorage extends Service {

  /**
   * Persists a snapshot of transaction state.
   */
  public void writeSnapshot(TransactionSnapshot snapshot) throws IOException;

  /**
   * Returns the most recent snapshot that has been successfully written.  Note that this may return {@code null}
   * if no completed snapshot files are found.
   */
  public TransactionSnapshot getLatestSnapshot() throws IOException;

  /**
   * Returns all {@link TransactionLog}s with a timestamp greater than or equal to the given timestamp.  Note that
   * the returned list is guaranteed to be sorted in ascending timestamp order.
   */
  public List<TransactionLog> getLogsSince(long timestamp) throws IOException;

  /**
   * Creates a new {@link TransactionLog}.
   */
  public TransactionLog createLog(long timestamp) throws IOException;

  /**
   * Returns a string representation of the location used for persistence.
   */
  public String getLocation();
}
