/*
 * Copyright © 2026 Cask Data, Inc.
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

package org.apache.tephra.persist;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Compatibility bridge interface for Tephra's {@link TransactionStateStorage} with Guava 30+.
 */
public interface TransactionStateStorage extends Service {

  void writeSnapshot(OutputStream out, TransactionSnapshot snapshot) throws IOException;

  void writeSnapshot(TransactionSnapshot snapshot) throws IOException;

  TransactionSnapshot getLatestSnapshot() throws IOException;

  TransactionVisibilityState getLatestTransactionVisibilityState() throws IOException;

  long deleteOldSnapshots(int numberToKeep) throws IOException;

  List<String> listSnapshots() throws IOException;

  List<TransactionLog> getLogsSince(long timestamp) throws IOException;

  TransactionLog createLog(long timestamp) throws IOException;

  List<String> listLogs() throws IOException;

  void deleteLogsOlderThan(long timestamp) throws IOException;

  void setupStorage() throws IOException;

  String getLocation();

  /**
   * Guava 13 compatibility bridge method called by TransactionManager.
   *
   * @return A completed future with the running state
   */
  default ListenableFuture<Service.State> start() {
    startAsync().awaitRunning();
    return Futures.immediateFuture(state());
  }

  /**
   * Guava 13 compatibility bridge method called by TransactionManager.
   *
   * @return The state after starting
   */
  default Service.State startAndWait() {
    startAsync().awaitRunning();
    return state();
  }

  /**
   * Guava 13 compatibility bridge method called by TransactionManager.
   *
   * @return A completed future with the terminated state
   */
  default ListenableFuture<Service.State> stop() {
    stopAsync().awaitTerminated();
    return Futures.immediateFuture(state());
  }

  /**
   * Guava 13 compatibility bridge method called by TransactionManager.
   *
   * @return The state after stopping
   */
  default Service.State stopAndWait() {
    stopAsync().awaitTerminated();
    return state();
  }
}
