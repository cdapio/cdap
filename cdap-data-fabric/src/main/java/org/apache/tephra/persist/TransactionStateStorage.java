/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.persist;

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
   * Returns the most recent transaction visibility state that has been successfully written.
   * Note that this may return {@code null} if no completed snapshot files are found.
   * @return {@link TransactionVisibilityState}
   */
  public TransactionVisibilityState getLatestTransactionVisibilityState() throws IOException;

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
   * Create the directories required for the transaction state stage.
   * @throws IOException If an error occurred during the creation of required directories for transaction state storage.
   */
  public void setupStorage() throws IOException;

  /**
   * Returns a string representation of the location used for persistence.
   */
  public String getLocation();
}
