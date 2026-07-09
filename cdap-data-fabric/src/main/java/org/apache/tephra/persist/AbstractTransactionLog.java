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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.tephra.TxConstants;
import org.apache.tephra.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/**
 * Common implementation of a transaction log, backed by file reader and writer based storage.  Classes extending
 * this class, must also implement {@link TransactionLogWriter} and {@link TransactionLogReader}.
 *
 * It is important to call close() on this class to ensure that all writes are synced and the log files are closed.
 */
public abstract class AbstractTransactionLog implements TransactionLog {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTransactionLog.class);

  private final AtomicLong logSequence = new AtomicLong();
  private final MetricsCollector metricsCollector;
  protected long timestamp;
  private volatile boolean initialized;
  private volatile boolean closing;
  private volatile boolean closed;
  private long writtenUpTo = 0L;
  private volatile long syncedUpTo = 0L;
  private final Queue<Entry> pendingWrites = new ConcurrentLinkedQueue<>();
  private TransactionLogWriter writer;

  private int countSinceLastSync = 0;
  private long positionBeforeWrite = -1L;
  private final Stopwatch stopWatch = Stopwatch.createUnstarted();

  private final long slowAppendThreshold;

  AbstractTransactionLog(long timestamp, MetricsCollector metricsCollector, Configuration conf) {
    this.timestamp = timestamp;
    this.metricsCollector = metricsCollector;
    this.slowAppendThreshold = conf.getLong(TxConstants.TransactionLog.CFG_SLOW_APPEND_THRESHOLD,
                                            TxConstants.TransactionLog.DEFAULT_SLOW_APPEND_THRESHOLD);
  }

  /**
   * Initializes the log file, opening a file writer.
   *
   * @throws java.io.IOException If an error is encountered initializing the file writer.
   */
  private synchronized void init() throws IOException {
    if (initialized) {
      return;
    }
    this.writer = createWriter();
    this.initialized = true;
  }

  /**
   * Returns a log writer to be used for appending any new {@link TransactionEdit} objects.
   */
  protected abstract TransactionLogWriter createWriter() throws IOException;

  @Override
  public abstract String getName();

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void append(TransactionEdit edit) throws IOException {
    append(Collections.singletonList(edit));
  }

  @Override
  public void append(List<TransactionEdit> edits) throws IOException {
    if (closing) { // or closed, which implies closing
      throw new IOException("Log " + getName() + " is closing or already closed, cannot append");
    }
    if (!initialized) {
      init();
    }
    // synchronizing here ensures that elements in the queue are ordered by seq number
    synchronized (logSequence) {
      for (TransactionEdit edit : edits) {
        pendingWrites.add(new Entry(new LongWritable(logSequence.getAndIncrement()), edit));
      }
    }
    // try to sync all pending edits (competing for this with other threads)
    sync();
  }

  /**
   * Return all pending writes at the time the method is called, or null if no writes are pending.
   *
   * Note that after this method returns, there can be additional pending writes,
   * added concurrently while the existing pending writes are removed.
   */
  @Nullable
  private Entry[] getPendingWrites() {
    synchronized (this) {
      if (pendingWrites.isEmpty()) {
        return null;
      }
      Entry[] entriesToSync = new Entry[pendingWrites.size()];
      for (int i = 0; i < entriesToSync.length; i++) {
        entriesToSync[i] = pendingWrites.remove();
      }
      return entriesToSync;
    }
  }

  /**
   * When multiple threads try to log edits at the same time, they all will call (@link #append}
   * followed by {@link #sync()}, concurrently. Hence, it can happen that multiple {@code append()}
   * are followed by a single {@code sync}, or vice versa.
   *
   * We want to record the time and position of the first {@code append()} after a {@code sync()},
   * then measure the time after the next {@code sync()}, and log a warning if it exceeds a threshold.
   * Therefore this is called every time before we write the pending list out to the log writer.
   *
   * See {@link #stopTimer(TransactionLogWriter)}.
   *
   * @throws IOException if the position of the writer cannot be determined
   */
  private void startTimerIfNeeded(TransactionLogWriter writer, int entryCount) throws IOException {
    // no sync needed because this is only called within a sync block
    if (positionBeforeWrite == -1L) {
      positionBeforeWrite = writer.getPosition();
      countSinceLastSync = 0;
      stopWatch.reset().start();
    }
    countSinceLastSync += entryCount;
  }

  /**
   * Called by a {@code sync()} after flushing to file system. Issues a warning if the write(s)+sync
   * together exceed a threshold.
   *
   * See {@link #startTimerIfNeeded(TransactionLogWriter, int)}.
   *
   * @throws IOException if the position of the writer cannot be determined
   */
  private void stopTimer(TransactionLogWriter writer) throws IOException {
    // this method is only called by a thread if it actually called sync(), inside a sync block
    if (positionBeforeWrite != -1L) { // actually it should never be -1, but just in case
      stopWatch.stop();
      long elapsed = stopWatch.elapsed(TimeUnit.MILLISECONDS);
      long bytesWritten = writer.getPosition() - positionBeforeWrite;
      if (elapsed >= slowAppendThreshold) {
        LOG.info("Slow append to log {}, took {} ms for {} entr{} and {} bytes.",
                 getName(), elapsed, countSinceLastSync, countSinceLastSync == 1 ? "y" : "ies", bytesWritten);
      }
      metricsCollector.histogram("wal.sync.size", countSinceLastSync);
      metricsCollector.histogram("wal.sync.bytes", (int) bytesWritten); // single sync won't exceed max int
    }
    positionBeforeWrite = -1L;
    countSinceLastSync = 0;
  }

  private void sync() throws IOException {
    // writes out pending entries to the HLog
    long latestSeq = 0;
    int entryCount = 0;
    synchronized (this) {
      if (closed) {
        if (pendingWrites.isEmpty()) {
          // this expected: close() sets closed to true after syncing all pending writes (including ours)
          return;
        }
        // this should never happen because close() only sets closed=true after syncing.
        // but if it should happen, we must fail this call because we don't know whether the edit was persisted
        throw new IOException(
          "Unexpected state: Writer is closed but there are pending edits. Cannot guarantee that edits were persisted");
      }
      Entry[] currentPending = getPendingWrites();
      if (currentPending != null) {
        entryCount = currentPending.length;
        startTimerIfNeeded(writer, entryCount);
        writer.commitMarker(entryCount);
        for (Entry e : currentPending) {
          writer.append(e);
        }
        // sequence are guaranteed to be ascending, so the last one is the greatest
        latestSeq = currentPending[currentPending.length - 1].getKey().get();
        writtenUpTo = latestSeq;
      }
    }

    // giving up the sync lock here allows other threads to write their edits before the sync happens.
    // hence, we can have the edits from n threads in one sync.

    // someone else might have already synced our edits, avoid double syncing
    // Note: latestSeq is a local variable and syncedUpTo is volatile; hence this is safe without synchronization
    if (syncedUpTo >= latestSeq) {
      return;
    }
    synchronized (this) {
      // check again - someone else might have  synced our edits while we were waiting to synchronize
      if (syncedUpTo >= latestSeq) {
        return;
      }
      if (closed) {
        // this should never happen because close() only sets closed=true after syncing.
        // but if it should happen, we must fail this call because we don't know whether the edit was persisted
        throw new IOException(String.format(
          "Unexpected state: Writer is closed but there are unsynced edits up to sequence id %d, and writes have " +
            "been synced up to sequence id %d. Cannot guarantee that edits are persisted.", latestSeq, syncedUpTo));
      }
      writer.sync();
      syncedUpTo = writtenUpTo;
      stopTimer(writer);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    // prevent other threads from adding more edits to the pending queue
    closing = true;

    // perform a final sync if any outstanding writes
    if (!pendingWrites.isEmpty()) {
      sync();
    }
    // NOTE: writer is lazy-inited, so it can be null
    if (writer != null) {
      this.writer.close();
    }
    this.closed = true;
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public abstract TransactionLogReader getReader() throws IOException;

  /**
   * Represents an entry in the transaction log.  Each entry consists of a key, generated from an incrementing sequence
   * number, and a value, the {@link TransactionEdit} being stored.
   */
  public static class Entry implements Writable {
    private LongWritable key;
    private TransactionEdit edit;

    // for Writable
    public Entry() {
      this.key = new LongWritable();
      this.edit = new TransactionEdit();
    }

    public Entry(LongWritable key, TransactionEdit edit) {
      this.key = key;
      this.edit = edit;
    }

    public LongWritable getKey() {
      return this.key;
    }

    public TransactionEdit getEdit() {
      return this.edit;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      this.key.write(out);
      this.edit.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.key.readFields(in);
      this.edit.readFields(in);
    }
  }

  // package private for testing
  @SuppressWarnings("deprecation")
  @Deprecated
  @VisibleForTesting
  static class CaskEntry implements Writable {
    private LongWritable key;
    private co.cask.tephra.persist.TransactionEdit edit;

    // for Writable
    @SuppressWarnings("unused")
    public CaskEntry() {
      this.key = new LongWritable();
      this.edit = new co.cask.tephra.persist.TransactionEdit();
    }

    CaskEntry(LongWritable key, co.cask.tephra.persist.TransactionEdit edit) {
      this.key = key;
      this.edit = edit;
    }

    public LongWritable getKey() {
      return this.key;
    }

    public co.cask.tephra.persist.TransactionEdit getEdit() {
      return this.edit;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      this.key.write(out);
      this.edit.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.key.readFields(in);
      this.edit.readFields(in);
    }
  }
}
