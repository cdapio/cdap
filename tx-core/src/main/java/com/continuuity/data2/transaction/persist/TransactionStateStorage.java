package com.continuuity.data2.transaction.persist;

import com.google.common.util.concurrent.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Defines the common contract for persisting transaction state changes.
 */
public interface TransactionStateStorage extends Service {

  /**
   * Persists a snapshot of transaction state to an output stream.
   */
  public void writeSnapshot(OutputStream out, TransactionSnapshot snapshot) throws IOException;

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
   * Removes any snapshots prior to the {@code numberToKeep} most recent.
   *
   * @param numberToKeep The number of most recent snapshots to keep.
   * @throws IOException If an error occurs while deleting old snapshots.
   * @return The timestamp of the oldest snapshot kept.
   */
  public long deleteOldSnapshots(int numberToKeep) throws IOException;

  /**
   * Returns the (non-qualified) names of available snapshots.
   */
  public List<String> listSnapshots() throws IOException;

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
   * Returns the (non-qualified) names of available logs.
   */
  public List<String> listLogs() throws IOException;

  /**
   * Removes any transaction logs with a timestamp older than the given value.  Logs must be removed based on timestamp
   * to ensure we can fully recover state based on a given snapshot.
   * @param timestamp The timestamp to delete up to.  Logs with a timestamp less than this value will be removed.
   * @throws IOException If an error occurs while removing logs.
   */
  public void deleteLogsOlderThan(long timestamp) throws IOException;

  /**
   * Returns a string representation of the location used for persistence.
   */
  public String getLocation();
}
