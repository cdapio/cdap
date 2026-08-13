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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.tephra.snapshot.SnapshotCodecProvider;

/**
 * Compatibility bridge for Tephra's {@link NoOpTransactionStateStorage} with Guava 30+.
 */
public class NoOpTransactionStateStorage extends AbstractIdleService
    implements TransactionStateStorage {

  public NoOpTransactionStateStorage() {
  }

  public NoOpTransactionStateStorage(SnapshotCodecProvider codecProvider) {
  }

  @Override
  protected void startUp() throws Exception {
  }

  @Override
  protected void shutDown() throws Exception {
  }

  @Override
  public void writeSnapshot(OutputStream out, TransactionSnapshot snapshot) throws IOException {
  }

  @Override
  public void writeSnapshot(TransactionSnapshot snapshot) throws IOException {
  }

  @Override
  public TransactionSnapshot getLatestSnapshot() throws IOException {
    return null;
  }

  @Override
  public TransactionVisibilityState getLatestTransactionVisibilityState() throws IOException {
    return null;
  }

  @Override
  public long deleteOldSnapshots(int numberToKeep) throws IOException {
    return 0;
  }

  @Override
  public List<String> listSnapshots() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public List<TransactionLog> getLogsSince(long timestamp) throws IOException {
    return Collections.emptyList();
  }

  @Override
  public TransactionLog createLog(long timestamp) throws IOException {
    return new NoOpTransactionLog(timestamp);
  }

  @Override
  public void deleteLogsOlderThan(long timestamp) throws IOException {
  }

  @Override
  public void setupStorage() throws IOException {
  }

  @Override
  public List<String> listLogs() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public String getLocation() {
    return "in-memory";
  }

  /**
   * Guava 13 compatibility bridge method called by TransactionManager.
   *
   * @return A completed future with the running state
   */
  public ListenableFuture<Service.State> start() {
    startAsync().awaitRunning();
    return Futures.immediateFuture(state());
  }

  /**
   * Guava 13 compatibility bridge method called by TransactionManager.
   *
   * @return The state after starting
   */
  public Service.State startAndWait() {
    startAsync().awaitRunning();
    return state();
  }

  /**
   * Guava 13 compatibility bridge method called by TransactionManager.
   *
   * @return A completed future with the terminated state
   */
  public ListenableFuture<Service.State> stop() {
    stopAsync().awaitTerminated();
    return Futures.immediateFuture(state());
  }

  /**
   * Guava 13 compatibility bridge method called by TransactionManager.
   *
   * @return The state after stopping
   */
  public Service.State stopAndWait() {
    stopAsync().awaitTerminated();
    return state();
  }

  private static final class NoOpTransactionLog implements TransactionLog {
    private final long timestamp;

    private NoOpTransactionLog(long timestamp) {
      this.timestamp = timestamp;
    }

    @Override
    public String getName() {
      return "no-op";
    }

    @Override
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public void append(TransactionEdit edit) throws IOException {
    }

    @Override
    public void append(List<TransactionEdit> edits) throws IOException {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public TransactionLogReader getReader() throws IOException {
      return new TransactionLogReader() {
        @Override
        public TransactionEdit next() throws IOException {
          return null;
        }

        @Override
        public TransactionEdit next(TransactionEdit reuse) throws IOException {
          return null;
        }

        @Override
        public void close() throws IOException {
        }
      };
    }
  }
}
