package com.continuuity.data2.transaction.persist;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Persists transaction snapshots and write-ahead logs to files on the local filesystem.
 */
public class LocalFileTransactionStateStorage extends AbstractIdleService implements TransactionStateStorage {
  private static final String TMP_SNAPSHOT_FILE_PREFIX = ".in-progress.";
  private static final String SNAPSHOT_FILE_PREFIX = "snapshot.";
  private static final String LOG_FILE_PREFIX = "txlog.";
  private static final Logger LOG = LoggerFactory.getLogger(LocalFileTransactionStateStorage.class);
  static final int BUFFER_SIZE = 16384;

  private static final FilenameFilter SNAPSHOT_FILE_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File file, String s) {
      return s.startsWith(SNAPSHOT_FILE_PREFIX);
    }
  };

  private final String configuredSnapshotDir;
  private File snapshotDir;

  @Inject
  public LocalFileTransactionStateStorage(CConfiguration conf) {
    this.configuredSnapshotDir = conf.get(Constants.Transaction.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR);
  }

  @Override
  protected void startUp() throws Exception {
    Preconditions.checkState(configuredSnapshotDir != null,
        "Snapshot directory is not configured.  Please set " + Constants.Transaction.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR +
        " in configuration.");
    // create the directory if it doesn't exist
    snapshotDir = new File(configuredSnapshotDir);
    if (!snapshotDir.exists()) {
      if (!snapshotDir.mkdirs()) {
        throw new IOException("Failed to create directory " + configuredSnapshotDir +
                              " for transaction snapshot storage");
      }
    } else {
      Preconditions.checkState(snapshotDir.isDirectory(),
          "Configured snapshot directory " + configuredSnapshotDir + " is not a directory!");
      Preconditions.checkState(snapshotDir.canWrite(),
          "Configured snapshot directory " + configuredSnapshotDir + " exists but is not writable!");
    }
  }

  @Override
  protected void shutDown() throws Exception {
    // nothing to do
  }

  @Override
  public String getLocation() {
    return snapshotDir.getAbsolutePath();
  }

  @Override
  public void writeSnapshot(TransactionSnapshot snapshot) throws IOException {
    // TODO: instead of making an extra in-memory copy, serialize the snapshot directly to the file output stream
    SnapshotCodec codec = new SnapshotCodec();
    byte[] serialized = codec.encodeState(snapshot);
    // save the snapshot to a temporary file
    File snapshotTmpFile = new File(snapshotDir, TMP_SNAPSHOT_FILE_PREFIX + snapshot.getTimestamp());
    LOG.info("Writing snapshot to temporary file {}", snapshotTmpFile);

    Files.write(serialized, snapshotTmpFile);

    // move the temporary file into place with the correct filename
    File finalFile = new File(snapshotDir, SNAPSHOT_FILE_PREFIX + snapshot.getTimestamp());
    if (!snapshotTmpFile.renameTo(finalFile)) {
      throw new IOException("Failed renaming temporary snapshot file " + snapshotTmpFile.getName() + " to " +
          finalFile.getName());
    }

    LOG.info("Completed snapshot to file {}", finalFile);
  }

  @Override
  public TransactionSnapshot getLatestSnapshot() throws IOException {
    File[] snapshotFiles = snapshotDir.listFiles(SNAPSHOT_FILE_FILTER);
    TimestampedFilename mostRecent = null;
    for (File file : snapshotFiles) {
      TimestampedFilename tsFile = new TimestampedFilename(file);
      if (mostRecent == null || tsFile.compareTo(mostRecent) > 0) {
        mostRecent = tsFile;
      }
    }

    if (mostRecent == null) {
      LOG.info("No snapshot files found in {}", snapshotDir.getAbsolutePath());
      return null;
    }

    return readSnapshotFile(mostRecent.getFile());
  }

  private TransactionSnapshot readSnapshotFile(File file) throws IOException {
    byte[] serializedSnapshot = Files.toByteArray(file);
    SnapshotCodec codec = new SnapshotCodec();
    return codec.decodeState(serializedSnapshot);
  }

  @Override
  public List<TransactionLog> getLogsSince(long timestamp) throws IOException {
    File[] logFiles = snapshotDir.listFiles(new LogFileFilter(timestamp));
    return Lists.transform(Arrays.asList(logFiles), new Function<File, TransactionLog>() {
      @Nullable
      @Override
      public TransactionLog apply(@Nullable File input) {
        return new LocalFileTransactionLog(input);
      }
    });
  }

  @Override
  public TransactionLog createLog(long timestamp) throws IOException {
    return new LocalFileTransactionLog(new File(snapshotDir, LOG_FILE_PREFIX + timestamp));
  }

  private static class LogFileFilter implements FilenameFilter {
    private long startTime;

    public LogFileFilter(long startTime) {
      this.startTime = startTime;
    }

    @Override
    public boolean accept(File file, String s) {
      if (s.startsWith(LOG_FILE_PREFIX)) {
        String[] parts = s.split("\\.");
        if (parts.length == 2) {
          try {
            long fileTime = Long.parseLong(parts[1]);
            return fileTime >= startTime;
          } catch (NumberFormatException ignored) {
            LOG.warn("Filename {} did not match the expected pattern prefix.<timestamp>", s);
          }
        }
      }
      return false;
    }
  }


  /**
   * Represents a filename composed of a prefix and a ".timestamp" suffix.  This is useful for manipulating both
   * snapshot and transaction log filenames.
   */
  private static class TimestampedFilename implements Comparable<TimestampedFilename> {
    private File file;
    private String prefix;
    private long timestamp;

    public TimestampedFilename(File file) {
      this.file = file;
      String[] parts = file.getName().split("\\.");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Filename " + file.getName() +
                                           " did not match the expected pattern prefix.timestamp");
      }
      prefix = parts[0];
      timestamp = Long.parseLong(parts[1]);
    }

    public File getFile() {
      return file;
    }

    public String getPrefix() {
      return prefix;
    }

    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public int compareTo(TimestampedFilename other) {
      int res = prefix.compareTo(other.getPrefix());
      if (res == 0) {
        res = Longs.compare(timestamp, other.getTimestamp());
      }
      return res;
    }
  }

}
