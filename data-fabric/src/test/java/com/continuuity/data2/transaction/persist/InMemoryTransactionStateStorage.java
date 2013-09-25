package com.continuuity.data2.transaction.persist;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Stores the latest transaction snapshot and logs in memory.
 */
public class InMemoryTransactionStateStorage extends AbstractIdleService implements TransactionStateStorage {
  // only keeps the most recent snapshot in memory
  private TransactionSnapshot lastSnapshot;

  private NavigableMap<Long, TransactionLog> logs = new TreeMap<Long, TransactionLog>();

  @Override
  protected void startUp() throws Exception {
  }

  @Override
  protected void shutDown() throws Exception {
    lastSnapshot = null;
    logs = new TreeMap<Long, TransactionLog>();
  }

  @Override
  public void writeSnapshot(TransactionSnapshot snapshot) throws IOException {
    lastSnapshot = snapshot;
  }

  @Override
  public TransactionSnapshot getLatestSnapshot() throws IOException {
    return lastSnapshot;
  }

  @Override
  public List<TransactionLog> getLogsSince(long timestamp) throws IOException {
    return Lists.newArrayList(logs.tailMap(timestamp).values());
  }

  @Override
  public TransactionLog createLog(long timestamp) throws IOException {
    TransactionLog log = new InMemoryTransactionLog(timestamp);
    logs.put(timestamp, log);
    return log;
  }

  @Override
  public String getLocation() {
    return "in-memory";
  }

  private static class InMemoryTransactionLog implements TransactionLog {
    private long timestamp;
    private List<TransactionEdit> edits = Lists.newArrayList();
    boolean isClosed = false;
    InMemoryTransactionLog(long timestamp) {
      this.timestamp = timestamp;
    }

    @Override
    public String getName() {
      return "in-memory@" + timestamp;
    }

    @Override
    public void append(TransactionEdit edit) throws IOException {
      if (isClosed) {
        throw new IOException("Log is closed");
      }
      edits.add(edit);
    }

    @Override
    public void append(List<TransactionEdit> edits) throws IOException {
      if (isClosed) {
        throw new IOException("Log is closed");
      }
      edits.addAll(edits);
    }

    @Override
    public void close() {
      isClosed = true;
    }

    @Override
    public TransactionLogReader getReader() throws IOException {
      return new InMemoryLogReader(edits.iterator());
    }
  }

  private static class InMemoryLogReader implements TransactionLogReader {
    private final Iterator<TransactionEdit> editIterator;

    public InMemoryLogReader(Iterator<TransactionEdit> editIterator) {
      this.editIterator = editIterator;
    }

    @Override
    public TransactionEdit next() throws IOException {
      if (editIterator.hasNext()) {
        return editIterator.next();
      }
      return null;
    }

    @Override
    public TransactionEdit next(TransactionEdit reuse) throws IOException {
      return next();
    }

    @Override
    public void close() throws IOException {
    }
  }
}
