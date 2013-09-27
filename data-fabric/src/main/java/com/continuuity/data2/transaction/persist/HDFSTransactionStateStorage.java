package com.continuuity.data2.transaction.persist;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Handles persistence of transaction snapshot and logs to a directory in HDFS.
 *
 * The directory used for file storage is configured using the {@code data.tx.snapshot.dir} configuration property.
 * Both snapshot and transaction log files are suffixed with a timestamp to allow easy ordering.  Snapshot files
 * are written with the filename "snapshot.&lt;timestamp&gt;".  Transaction log files are written with the filename
 * "txlog.&lt;timestamp&gt;".
 */
public class HDFSTransactionStateStorage extends AbstractIdleService implements TransactionStateStorage {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSTransactionStateStorage.class);

  private static final String SNAPSHOT_FILE_PREFIX = "snapshot.";
  private static final String TMP_SNAPSHOT_FILE_PREFIX = ".in-progress.snapshot.";
  private static final String LOG_FILE_PREFIX = "txlog.";

  private static final PathFilter SNAPSHOT_FILE_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().startsWith(SNAPSHOT_FILE_PREFIX);
    }
  };

  // buffer size used for HDFS reads and writes
  private static final int BUFFER_SIZE = 16384;

  private CConfiguration conf;
  private FileSystem fs;
  private Configuration hConf;
  private String configuredSnapshotDir;
  private Path snapshotDir;

  @Inject
  public HDFSTransactionStateStorage(@Named("TransactionServerConfig") CConfiguration config,
                                     @Named("HBaseOVCTableHandleHConfig") Configuration hConf) {
    this.conf = config;
    this.hConf = hConf;
    configuredSnapshotDir = config.get(Constants.Transaction.Manager.CFG_TX_SNAPSHOT_DIR);
  }

  @Override
  protected void startUp() throws Exception {
    Preconditions.checkState(configuredSnapshotDir != null,
        "Snapshot directory is not configured.  Please set " + Constants.Transaction.Manager.CFG_TX_SNAPSHOT_DIR +
        " in configuration.");
    String hdfsUser = conf.get(Constants.CFG_HDFS_USER);
    if (hdfsUser == null) {
      // NOTE: we can start multiple times this storage. As hdfs uses per-jvm cache, we want to create new fs instead
      //       of getting closed one
      fs = FileSystem.newInstance(FileSystem.getDefaultUri(hConf), hConf);
    } else {
      fs = FileSystem.newInstance(FileSystem.getDefaultUri(hConf), hConf, hdfsUser);
    }
    snapshotDir = new Path(configuredSnapshotDir);
    if (!fs.exists(snapshotDir)) {
      LOG.info("Creating snapshot dir at {}", snapshotDir);
      fs.mkdirs(snapshotDir);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    fs.close();
  }

  @Override
  public void writeSnapshot(TransactionSnapshot snapshot) throws IOException {
    // TODO: instead of making an extra in-memory copy, serialize the snapshot directly to the file output stream
    SnapshotCodec codec = new SnapshotCodec();
    byte[] serialized = codec.encodeState(snapshot);
    // create a temporary file, and save the snapshot
    Path snapshotTmpFile = new Path(snapshotDir, TMP_SNAPSHOT_FILE_PREFIX + snapshot.getTimestamp());
    LOG.info("Writing snapshot to temporary file {}", snapshotTmpFile);

    FSDataOutputStream out = fs.create(snapshotTmpFile, false, BUFFER_SIZE);
    out.write(serialized);
    out.close();

    // move the temporary file into place with the correct filename
    Path finalFile = new Path(snapshotDir, SNAPSHOT_FILE_PREFIX + snapshot.getTimestamp());
    fs.rename(snapshotTmpFile, finalFile);
    LOG.info("Completed snapshot to file {}", finalFile);
  }

  @Override
  public TransactionSnapshot getLatestSnapshot() throws IOException {
    FileStatus[] snapshotFileStatuses = fs.listStatus(snapshotDir, SNAPSHOT_FILE_FILTER);
    TimestampedFilename mostRecent = null;
    for (FileStatus status : snapshotFileStatuses) {
      TimestampedFilename current = new TimestampedFilename(status.getPath());
      // check if the file is more recent
      if (mostRecent == null || current.compareTo(mostRecent) > 0) {
        mostRecent = current;
      }
    }

    if (mostRecent == null) {
      LOG.info("No snapshot files found in {}", snapshotDir);
      return null;
    }

    return readSnapshotFile(mostRecent.getPath());
  }

  private TransactionSnapshot readSnapshotFile(Path filePath) throws IOException {
    // TODO: deserialize the snapshot from the input stream instead of copying into memory twice
    FSDataInputStream in = fs.open(filePath, BUFFER_SIZE);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      byte[] chunk = new byte[BUFFER_SIZE];
      int read = 0;
      int totalRead = 0;
      while ((read = in.read(chunk)) != -1) {
        out.write(chunk, 0, read);
        totalRead += read;
      }
      LOG.info("Read {} bytes from {}", totalRead, filePath);
    } finally {
      in.close();
    }

    SnapshotCodec codec = new SnapshotCodec();
    return codec.decodeState(out.toByteArray());
  }

  @Override
  public List<TransactionLog> getLogsSince(long timestamp) throws IOException {
    FileStatus[] statuses = fs.listStatus(snapshotDir, new LogFileFilter(timestamp));
    return Lists.transform(Arrays.asList(statuses), new Function<FileStatus, TransactionLog>() {
      @Nullable
      @Override
      public TransactionLog apply(@Nullable FileStatus input) {
        return new HDFSTransactionLog(conf, fs, hConf, input.getPath());
      }
    });
  }

  @Override
  public TransactionLog createLog(long timestamp) throws IOException {
    Path newLog = new Path(snapshotDir, LOG_FILE_PREFIX + timestamp);
    return new HDFSTransactionLog(conf, fs, hConf, newLog);
  }

  @Override
  public String getLocation() {
    return snapshotDir.toString();
  }

  private static class LogFileFilter implements PathFilter {
    private long startTime;

    public LogFileFilter(long startTime) {
      this.startTime = startTime;
    }

    @Override
    public boolean accept(Path path) {
      if (path.getName().startsWith(LOG_FILE_PREFIX)) {
        String[] parts = path.getName().split("\\.");
        if (parts.length == 2) {
          try {
            long fileTime = Long.parseLong(parts[1]);
            return fileTime >= startTime;
          } catch (NumberFormatException ignored) {
            LOG.warn("Filename {} did not match the expected pattern prefix.<timestamp>", path.getName());
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
    private Path path;
    private String prefix;
    private long timestamp;

    public TimestampedFilename(Path path) {
      this.path = path;
      String[] parts = path.getName().split("\\.");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Filename " + path.getName() +
            " did not match the expected pattern prefix.timestamp");
      }
      prefix = parts[0];
      timestamp = Long.parseLong(parts[1]);
    }

    public Path getPath() {
      return path;
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
