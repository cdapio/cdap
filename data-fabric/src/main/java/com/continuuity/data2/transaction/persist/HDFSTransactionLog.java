package com.continuuity.data2.transaction.persist;

import com.continuuity.common.conf.CConfiguration;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;

/**
 * Allows reading from and writing to a transaction write-ahead log stored in HDFS.
 */
public class HDFSTransactionLog extends AbstractTransactionLog {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSTransactionLog.class);

  private final FileSystem fs;
  private final CConfiguration conf;
  private final Configuration hConf;
  private final Path logPath;

  /**
   * Creates a new HDFS-backed write-ahead log for storing transaction state.
   * @param conf Continuuity configuration to use.
   * @param fs Open FileSystem instance for opening log files in HDFS.
   * @param hConf HDFS cluster configuration.
   * @param logPath Path to the log file.
   */
  public HDFSTransactionLog(final CConfiguration conf, final FileSystem fs, final Configuration hConf,
                            final Path logPath, long timestamp) {
    super(timestamp);
    this.fs = fs;
    this.conf = conf;
    this.hConf = hConf;
    this.logPath = logPath;
  }

  @Override
  protected TransactionLogWriter createWriter() throws IOException {
    return new LogWriter(fs, hConf, logPath);
  }

  @Override
  public String getName() {
    return logPath.getName();
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
      HDFSUtil hdfsUtil = new HDFSUtil();
      hdfsUtil.recoverFileLease(fs, logPath, hConf);
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

  private static final class LogWriter implements TransactionLogWriter {
    private final SequenceFile.Writer internalWriter;

    public LogWriter(FileSystem fs, Configuration hConf, Path logPath) throws IOException {
      // TODO: retry a few times to ride over transient failures?
      this.internalWriter =
        SequenceFile.createWriter(fs, hConf, logPath, LongWritable.class, TransactionEdit.class);
      LOG.info("Created a new TransactionLog writer for " + logPath);
    }

    @Override
    public void append(Entry entry) throws IOException {
      internalWriter.append(entry.getKey(), entry.getEdit());
    }

    @Override
    public void sync() throws IOException {
      internalWriter.syncFs();
    }

    @Override
    public void close() throws IOException {
      internalWriter.close();
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
