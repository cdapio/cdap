package com.continuuity.data2.transaction.persist;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Common implementation of a transaction log, backed by file reader and writer based storage.  Classes extending
 * this class, must also implement {@link TransactionLogWriter} and {@link TransactionLogReader}.
 */
public abstract class AbstractTransactionLog implements TransactionLog {
  /** Time limit, in milliseconds, of an append to the transaction log before we log it as "slow". */
  private static final long SLOW_APPEND_THRESHOLD = 1000L;

  private static final Logger LOG = LoggerFactory.getLogger(HDFSTransactionStateStorage.class);

  private final AtomicLong logSequence = new AtomicLong();
  protected long timestamp;
  private volatile boolean initialized;
  private volatile boolean closed;
  private AtomicLong syncedUpTo = new AtomicLong();
  private List<Entry> pendingWrites = Lists.newLinkedList();
  private TransactionLogWriter writer;

  public AbstractTransactionLog(long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * Initializes the log file, opening a file writer.  Clients calling {@code init()} should ensure that they
   * also call {@link HDFSTransactionLog#close()}.
   * @throws java.io.IOException If an error is encountered initializing the file writer.
   */
  public synchronized void init() throws IOException {
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
    long startTime = System.nanoTime();
    synchronized (this) {
      ensureAvailable();

      Entry entry = new Entry(new LongWritable(logSequence.getAndIncrement()), edit);

      // add to pending edits
      append(entry);
    }

    // wait for sync to complete
    sync();
    long durationMillis = (System.nanoTime() - startTime) / 1000000L;
    if (durationMillis > SLOW_APPEND_THRESHOLD) {
      LOG.info("Slow append to log " + getName() + ", took " + durationMillis + " msec.");
    }
  }

  @Override
  public void append(List<TransactionEdit> edits) throws IOException {
    long startTime = System.nanoTime();
    synchronized (this) {
      ensureAvailable();

      for (TransactionEdit edit : edits) {
        Entry entry = new Entry(new LongWritable(logSequence.getAndIncrement()), edit);

        // add to pending edits
        append(entry);
      }
    }

    // wait for sync to complete
    sync();
    long durationMillis = (System.nanoTime() - startTime) / 1000000L;
    if (durationMillis > SLOW_APPEND_THRESHOLD) {
      LOG.info("Slow append to log " + getName() + ", took " + durationMillis + " msec.");
    }
  }

  private void ensureAvailable() throws IOException {
    if (closed) {
      throw new IOException("Log " + getName() + " is already closed, cannot append!");
    }
    if (!initialized) {
      init();
    }
  }

  /*
   * Appends new writes to the pendingWrites. It is better to keep it in
   * our own queue rather than writing it to the HDFS output stream because
   * HDFSOutputStream.writeChunk is not lightweight at all.
   */
  private void append(Entry e) throws IOException {
    pendingWrites.add(e);
  }

  // Returns all currently pending writes. New writes
  // will accumulate in a new list.
  private List<Entry> getPendingWrites() {
    synchronized (this) {
      List<Entry> save = this.pendingWrites;
      this.pendingWrites = new LinkedList<Entry>();
      return save;
    }
  }

  private void sync() throws IOException {
    // writes out pending entries to the HLog
    TransactionLogWriter tmpWriter = null;
    long latestSeq = 0;
    synchronized (this) {
      if (closed) {
        return;
      }
      // prevent writer being dereferenced
      tmpWriter = writer;

      List<Entry> currentPending = getPendingWrites();
      // write out all accumulated Entries to hdfs.
      for (Entry e : currentPending) {
        tmpWriter.append(e);
        latestSeq = Math.max(latestSeq, e.getKey().get());
      }
    }
    long lastSynced = syncedUpTo.get();
    // someone else might have already synced our edits, avoid double syncing
    if (lastSynced < latestSeq) {
      tmpWriter.sync();
      syncedUpTo.compareAndSet(lastSynced, latestSeq);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
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
}
