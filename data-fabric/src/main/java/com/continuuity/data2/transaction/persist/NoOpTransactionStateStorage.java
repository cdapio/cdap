package com.continuuity.data2.transaction.persist;

import com.google.common.util.concurrent.AbstractIdleService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal {@link TransactionStateStorage} implementation that does nothing, i.e. does not maintain any actual state.
 */
public class NoOpTransactionStateStorage extends AbstractIdleService implements TransactionStateStorage {
  @Override
  protected void startUp() throws Exception {
  }

  @Override
  protected void shutDown() throws Exception {
  }

  @Override
  public void writeSnapshot(TransactionSnapshot snapshot) throws IOException {
  }

  @Override
  public TransactionSnapshot getLatestSnapshot() throws IOException {
    return null;
  }

  @Override
  public long deleteOldSnapshots(int numberToKeep) throws IOException {
    return 0;
  }

  @Override
  public List<String> listSnapshots() throws IOException {
    return new ArrayList<String>(0);
  }

  @Override
  public List<TransactionLog> getLogsSince(long timestamp) throws IOException {
    return new ArrayList<TransactionLog>(0);
  }

  @Override
  public TransactionLog createLog(long timestamp) throws IOException {
    return new NoOpTransactionLog();
  }

  @Override
  public void deleteLogsOlderThan(long timestamp) throws IOException {
  }

  @Override
  public List<String> listLogs() throws IOException {
    return new ArrayList<String>(0);
  }

  @Override
  public String getLocation() {
    return "no-op";
  }

  private static class NoOpTransactionLog implements TransactionLog {
    private long timestamp = System.currentTimeMillis();

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
    public void close() {
    }

    @Override
    public TransactionLogReader getReader() {
      return new TransactionLogReader() {
        @Override
        public TransactionEdit next() {
          return null;
        }

        @Override
        public TransactionEdit next(TransactionEdit reuse) {
          return null;
        }

        @Override
        public void close() {
        }
      };
    }
  }
}
