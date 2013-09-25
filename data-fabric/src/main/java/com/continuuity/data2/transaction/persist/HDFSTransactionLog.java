package com.continuuity.data2.transaction.persist;

import com.continuuity.common.conf.CConfiguration;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Allows reading from and writing to a transaction write-ahead log stored in HDFS.
 */
public class HDFSTransactionLog implements TransactionLog {

  private static final Logger LOG = LoggerFactory.getLogger(HDFSTransactionStateStorage.class);

  private final FileSystem fs;
  private final CConfiguration conf;
  private final Configuration hConf;
  private final Path logPath;

  private final AtomicLong logSequence = new AtomicLong();

  private volatile boolean initialized;
  private volatile boolean closed;
  private AtomicLong syncedUpTo = new AtomicLong();
  private List<Entry> pendingWrites = Lists.newLinkedList();

  private SequenceFile.Writer writer;

  /**
   * Creates a new HDFS-backed write-ahead log for storing transaction state.
   * @param conf Continuuity configuration to use.
   * @param fs Open FileSystem instance for opening log files in HDFS.
   * @param hConf HDFS cluster configuration.
   * @param logPath Path to the log file.
   */
  public HDFSTransactionLog(final CConfiguration conf, final FileSystem fs, final Configuration hConf,
                            final Path logPath) {
    this.fs = fs;
    this.conf = conf;
    this.hConf = hConf;
    this.logPath = logPath;
  }

  /**
   * Initializes the log file, opening a file writer.  Clients calling {@code init()} should ensure that they
   * also call {@link com.continuuity.data2.transaction.persist.HDFSTransactionLog#close()}.
   * @throws IOException If an error is encountered initializing the file writer.
   */
  public synchronized void init() throws IOException {
    if (initialized) {
      return;
    }
    this.writer = createWriter();
    this.initialized = true;
  }

  private SequenceFile.Writer createWriter() throws IOException {
    // TODO: retry a few times to ride over transient failures?
    SequenceFile.Writer writer =
        SequenceFile.createWriter(fs, hConf, logPath, LongWritable.class, TransactionEdit.class);
    LOG.info("Created a new TransactionLog writer for " + logPath);
    return writer;
  }

  @Override
  public String getName() {
    return logPath.getName();
  }

  @Override
  public void append(TransactionEdit edit) throws IOException {
    synchronized (this) {
      ensureAvailable();

      Entry entry = new Entry(new LongWritable(logSequence.getAndIncrement()), edit);

      // add to pending edits
      append(entry);
    }

    // wait for sync to complete
    sync();
  }

  @Override
  public void append(List<TransactionEdit> edits) throws IOException {
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
  }

  private void ensureAvailable() throws IOException {
    if (closed) {
      throw new IOException("Log " + logPath.getName() + " is already closed, cannot append!");
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

  void sync() throws IOException {
    // writes out pending entries to the HLog
    SequenceFile.Writer tmpWriter = null;
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
        tmpWriter.append(e.getKey(), e.getEdit());
        latestSeq = Math.max(latestSeq, e.getKey().get());
      }
    }
    long lastSynced = syncedUpTo.get();
    // someone else might have already synced our edits, avoid double syncing
    if (lastSynced < latestSeq) {
      tmpWriter.syncFs();
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
    this.writer.close();
    this.closed = true;
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public TransactionLogReader getReader() throws IOException {
    FileStatus status = fs.getFileStatus(logPath);
    long length = status.getLen();

    LogReader reader = null;
    // check if this file needs to be recovered due to failure
    // Check for possibly empty file. With appends, currently Hadoop reports a
    // zero length even if the file has been sync'd. Revisit if HDFS-376 or
    // HDFS-878 is committed.
    if (length <= 0) {
      LOG.warn("File " + logPath + " might be still open, length is 0");
    }

    try {
      FSUtils.getInstance(fs, hConf).recoverFileLease(fs, logPath, hConf);
      try {
        FileStatus newStatus = fs.getFileStatus(logPath);
        LOG.info("New file size for " + logPath + " is " + newStatus.getLen());
        SequenceFile.Reader fileReader = new SequenceFile.Reader(fs, logPath, hConf);
        reader = new LogReader(fileReader);
      } catch (EOFException e) {
        if (length <= 0) {
          // TODO should we ignore an empty, not-last log file if skip.errors
          // is false? Either way, the caller should decide what to do. E.g.
          // ignore if this is the last log in sequence.
          // TODO is this scenario still possible if the log has been
          // recovered (i.e. closed)
          LOG.warn("Could not open " + logPath + " for reading. File is empty", e);
          return null;
        } else {
          // EOFException being ignored
          return null;
        }
      }
    } catch (IOException e) {
      throw e;
    }

    return reader;
  }

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

  private static final class LogReader implements TransactionLogReader {

    private boolean closed;
    private SequenceFile.Reader reader;
    private LongWritable key = new LongWritable();

    public LogReader(SequenceFile.Reader reader) {
      this.reader = reader;
    }

    @Override
    public TransactionEdit next() {
      try {
        return next(new TransactionEdit());
      } catch (IOException ioe) {
        throw Throwables.propagate(ioe);
      }
    }

    @Override
    public TransactionEdit next(TransactionEdit reuse) throws IOException {
      if (closed) {
        return null;
      }
      boolean successful = reader.next(key, reuse);
      if (successful) {
        return reuse;
      }
      return null;
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      reader.close();
      closed = true;
    }
  }
}
