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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.CountingInputStream;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tephra.TxConstants;
import org.apache.tephra.metrics.MetricsCollector;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.snapshot.SnapshotCodecProvider;
import org.apache.tephra.util.ConfigurationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Handles persistence of transaction snapshot and logs to a directory in HDFS.
 *
 * The directory used for file storage is configured using the {@code data.tx.snapshot.dir} configuration property.
 * Both snapshot and transaction log files are suffixed with a timestamp to allow easy ordering.  Snapshot files
 * are written with the filename "snapshot.&lt;timestamp&gt;".  Transaction log files are written with the filename
 * "txlog.&lt;timestamp&gt;".
 */
public class HDFSTransactionStateStorage extends AbstractTransactionStateStorage {
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

  private final Configuration hConf;
  private final String configuredSnapshotDir;
  private final MetricsCollector metricsCollector;
  private FileSystem fs;
  private Path snapshotDir;

  @Inject
  public HDFSTransactionStateStorage(Configuration hConf, SnapshotCodecProvider codecProvider,
                                     MetricsCollector metricsCollector) {
    super(codecProvider);
    this.hConf = hConf;
    this.configuredSnapshotDir = hConf.get(TxConstants.Manager.CFG_TX_SNAPSHOT_DIR);
    this.metricsCollector = metricsCollector;
  }

  @Override
  protected void startUp() throws Exception {
    Preconditions.checkState(configuredSnapshotDir != null,
        "Snapshot directory is not configured.  Please set " + TxConstants.Manager.CFG_TX_SNAPSHOT_DIR +
        " in configuration.");
    String hdfsUser = hConf.get(TxConstants.Manager.CFG_TX_HDFS_USER);
    if (hdfsUser == null || UserGroupInformation.isSecurityEnabled()) {
      if (hdfsUser != null && LOG.isDebugEnabled()) {
        LOG.debug("Ignoring configuration {}={}, running on secure Hadoop",
                  TxConstants.Manager.CFG_TX_HDFS_USER, hdfsUser);
      }
      // NOTE: we can start multiple times this storage. As hdfs uses per-jvm cache, we want to create new fs instead
      //       of getting closed one
      fs = FileSystem.newInstance(FileSystem.getDefaultUri(hConf), hConf);
    } else {
      fs = FileSystem.newInstance(FileSystem.getDefaultUri(hConf), hConf, hdfsUser);
    }
    snapshotDir = new Path(configuredSnapshotDir);
    LOG.info("Using snapshot dir " + snapshotDir);
  }

  @Override
  protected void shutDown() throws Exception {
    fs.close();
  }

  @Override
  public void writeSnapshot(TransactionSnapshot snapshot) throws IOException {
    // create a temporary file, and save the snapshot
    Path snapshotTmpFile = new Path(snapshotDir, TMP_SNAPSHOT_FILE_PREFIX + snapshot.getTimestamp());
    LOG.debug("Writing snapshot to temporary file {}", snapshotTmpFile);

    FSDataOutputStream out = fs.create(snapshotTmpFile, false, BUFFER_SIZE);
    // encode the snapshot and stream the serialized version to the file
    try {
      codecProvider.encode(out, snapshot);
    } finally {
      out.close();
    }

    // move the temporary file into place with the correct filename
    Path finalFile = new Path(snapshotDir, SNAPSHOT_FILE_PREFIX + snapshot.getTimestamp());
    fs.rename(snapshotTmpFile, finalFile);
    LOG.debug("Completed snapshot to file {}", finalFile);
  }

  @Override
  public TransactionSnapshot getLatestSnapshot() throws IOException {
    InputStream in = getLatestSnapshotInputStream();
    if (in == null) {
      return null;
    }
    try {
      return readSnapshotInputStream(in);
    } finally {
      in.close();
    }
  }

  @Override
  public TransactionVisibilityState getLatestTransactionVisibilityState() throws IOException {
    InputStream in = getLatestSnapshotInputStream();
    if (in == null) {
      return null;
    }
    try {
      return readTransactionVisibilityStateFromInputStream(in);
    } finally {
      in.close();
    }
  }

  private InputStream getLatestSnapshotInputStream() throws IOException {
    TimestampedFilename[] snapshots = listSnapshotFiles();
    Arrays.sort(snapshots);
    if (snapshots.length > 0) {
      // last is the most recent
      return fs.open(snapshots[snapshots.length - 1].getPath(), BUFFER_SIZE);
    }

    LOG.info("No snapshot files found in {}", snapshotDir);
    return null;
  }

  private TransactionSnapshot readSnapshotInputStream(InputStream in) throws IOException {
    CountingInputStream countingIn = new CountingInputStream(in);
    TransactionSnapshot snapshot = codecProvider.decode(countingIn);
    LOG.info("Read encoded transaction snapshot of {} bytes", countingIn.getCount());
    return snapshot;
  }

  private TransactionVisibilityState readTransactionVisibilityStateFromInputStream(InputStream in) throws IOException {
    CountingInputStream countingIn = new CountingInputStream(in);
    TransactionVisibilityState state = codecProvider.decodeTransactionVisibilityState(countingIn);
    LOG.info("Read encoded transaction snapshot of {} bytes", countingIn.getCount());
    return state;
  }

  private TransactionSnapshot readSnapshotFile(Path filePath) throws IOException {
    FSDataInputStream in = fs.open(filePath, BUFFER_SIZE);
    try {
      return readSnapshotInputStream(in);
    } finally {
      in.close();
    }
  }

  private TimestampedFilename[] listSnapshotFiles() throws IOException {
    FileStatus[] snapshotFileStatuses = fs.listStatus(snapshotDir, SNAPSHOT_FILE_FILTER);
    TimestampedFilename[] snapshotFiles = new TimestampedFilename[snapshotFileStatuses.length];
    for (int i = 0; i < snapshotFileStatuses.length; i++) {
      snapshotFiles[i] = new TimestampedFilename(snapshotFileStatuses[i].getPath());
    }
    return snapshotFiles;
  }

  @Override
  public long deleteOldSnapshots(int numberToKeep) throws IOException {
    TimestampedFilename[] snapshots = listSnapshotFiles();
    if (snapshots.length == 0) {
      return -1;
    }
    Arrays.sort(snapshots, Collections.reverseOrder());
    if (snapshots.length <= numberToKeep) {
      // nothing to remove, oldest timestamp is the last snapshot
      return snapshots[snapshots.length - 1].getTimestamp();
    }
    int toRemoveCount = snapshots.length - numberToKeep;
    TimestampedFilename[] toRemove = new TimestampedFilename[toRemoveCount];
    System.arraycopy(snapshots, numberToKeep, toRemove, 0, toRemoveCount);

    for (TimestampedFilename f : toRemove) {
      LOG.debug("Removing old snapshot file {}", f.getPath());
      fs.delete(f.getPath(), false);
    }
    long oldestTimestamp = snapshots[numberToKeep - 1].getTimestamp();
    LOG.debug("Removed {} old snapshot files prior to {}", toRemoveCount, oldestTimestamp);
    return oldestTimestamp;
  }

  @Override
  public List<String> listSnapshots() throws IOException {
    FileStatus[] files = fs.listStatus(snapshotDir, SNAPSHOT_FILE_FILTER);
    return Lists.transform(Arrays.asList(files), new Function<FileStatus, String>() {
      @Nullable
      @Override
      public String apply(@Nullable FileStatus input) {
        return input.getPath().getName();
      }
    });
  }

  @Override
  public List<TransactionLog> getLogsSince(long timestamp) throws IOException {
    FileStatus[] statuses = fs.listStatus(snapshotDir, new LogFileFilter(timestamp, Long.MAX_VALUE));
    TimestampedFilename[] timestampedFiles = new TimestampedFilename[statuses.length];
    for (int i = 0; i < statuses.length; i++) {
      timestampedFiles[i] = new TimestampedFilename(statuses[i].getPath());
    }
    return Lists.transform(Arrays.asList(timestampedFiles), new Function<TimestampedFilename, TransactionLog>() {
      @Nullable
      @Override
      public TransactionLog apply(@Nullable TimestampedFilename input) {
        return openLog(input.getPath(), input.getTimestamp());
      }
    });
  }

  @Override
  public TransactionLog createLog(long timestamp) throws IOException {
    Path newLog = new Path(snapshotDir, LOG_FILE_PREFIX + timestamp);
    return openLog(newLog, timestamp);
  }

  private TransactionLog openLog(Path path, long timestamp) {
    return new HDFSTransactionLog(fs, hConf, path, timestamp, metricsCollector);
  }

  @Override
  public void deleteLogsOlderThan(long timestamp) throws IOException {
    FileStatus[] statuses = fs.listStatus(snapshotDir, new LogFileFilter(0, timestamp));
    int removedCnt = 0;
    for (FileStatus status : statuses) {
      LOG.debug("Removing old transaction log {}", status.getPath());
      if (fs.delete(status.getPath(), false)) {
        removedCnt++;
      } else {
        LOG.error("Failed to delete transaction log file {}", status.getPath());
      }
    }
    LOG.debug("Removed {} transaction logs older than {}", removedCnt, timestamp);
  }

  @Override
  public void setupStorage() throws IOException {
    if (!fs.exists(snapshotDir)) {
      LOG.info("Creating snapshot dir at {}", snapshotDir);
      fs.mkdirs(snapshotDir);
    } else {
      Preconditions.checkState(fs.isDirectory(snapshotDir),
                               "Configured snapshot directory " + snapshotDir + " is not a directory!");
    }
  }

  @Override
  public List<String> listLogs() throws IOException {
    FileStatus[] files = fs.listStatus(snapshotDir, new LogFileFilter(0, Long.MAX_VALUE));
    return Lists.transform(Arrays.asList(files), new Function<FileStatus, String>() {
      @Nullable
      @Override
      public String apply(@Nullable FileStatus input) {
        return input.getPath().getName();
      }
    });
  }

  @Override
  public String getLocation() {
    return snapshotDir.toString();
  }

  private static class LogFileFilter implements PathFilter {
    // starting time of files to include (inclusive)
    private final long startTime;
    // ending time of files to include (exclusive)
    private final long endTime;

    public LogFileFilter(long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    @Override
    public boolean accept(Path path) {
      if (path.getName().startsWith(LOG_FILE_PREFIX)) {
        String[] parts = path.getName().split("\\.");
        if (parts.length == 2) {
          try {
            long fileTime = Long.parseLong(parts[1]);
            return fileTime >= startTime && fileTime < endTime;
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

  // TODO move this out as a separate command line tool
  private enum CLIMode { SNAPSHOT, TXLOG };
  /**
   * Reads a transaction state snapshot or transaction log from HDFS and prints the entries to stdout.
   *
   * Supports the following options:
   *    -s    read snapshot state (defaults to the latest)
   *    -l    read a transaction log
   *    [filename]  reads the given file
   * @param args
   */
  public static void main(String[] args) {
    List<String> filenames = Lists.newArrayList();
    CLIMode mode = null;
    for (String arg : args) {
      if ("-s".equals(arg)) {
        mode = CLIMode.SNAPSHOT;
      } else if ("-l".equals(arg)) {
        mode = CLIMode.TXLOG;
      } else if ("-h".equals(arg)) {
        printUsage(null);
      } else {
        filenames.add(arg);
      }
    }

    if (mode == null) {
      printUsage("ERROR: Either -s or -l is required to set mode.", 1);
    }

    Configuration config = new ConfigurationFactory().get();

    // Use the no-op metrics collector.  We are being run as a command line tool, so there are no relevant metrics
    // to report
    HDFSTransactionStateStorage storage =
      new HDFSTransactionStateStorage(config, new SnapshotCodecProvider(config), new TxMetricsCollector());
    storage.startAndWait();
    try {
      switch (mode) {
        case SNAPSHOT:
          try {
            if (filenames.isEmpty()) {
              TransactionSnapshot snapshot = storage.getLatestSnapshot();
              printSnapshot(snapshot);
            }
            for (String file : filenames) {
              Path path = new Path(file);
              TransactionSnapshot snapshot = storage.readSnapshotFile(path);
              printSnapshot(snapshot);
              System.out.println();
            }
          } catch (IOException ioe) {
            System.err.println("Error reading snapshot files: " + ioe.getMessage());
            ioe.printStackTrace();
            System.exit(1);
          }
          break;
        case TXLOG:
          if (filenames.isEmpty()) {
            printUsage("ERROR: At least one transaction log filename is required!", 1);
          }
          for (String file : filenames) {
            TimestampedFilename timestampedFilename = new TimestampedFilename(new Path(file));
            TransactionLog log = storage.openLog(timestampedFilename.getPath(), timestampedFilename.getTimestamp());
            printLog(log);
            System.out.println();
          }
          break;
      }
    } finally {
      storage.stop();
    }
  }

  private static void printUsage(String message) {
    printUsage(message, 0);
  }

  private static void printUsage(String message, int exitCode) {
    if (message != null) {
      System.out.println(message);
    }
    System.out.println("Usage: java " + HDFSTransactionStateStorage.class.getName() + " (-s|-l) file1 [file2...]");
    System.out.println();
    System.out.println("\t-s\tRead files as transaction state snapshots (will default to latest if no file given)");
    System.out.println("\t-l\tRead files as transaction logs [filename is required]");
    System.out.println("\t-h\tPrint this message");
    System.exit(exitCode);
  }

  private static void printSnapshot(TransactionSnapshot snapshot) {
    Date snapshotDate = new Date(snapshot.getTimestamp());
    System.out.println("TransactionSnapshot at " + snapshotDate.toString());
    System.out.println("\t" + snapshot.toString());
  }

  private static void printLog(TransactionLog log) {
    try {
      System.out.println("TransactionLog " + log.getName());
      TransactionLogReader reader = log.getReader();
      TransactionEdit edit;
      long seq = 0;
      while ((edit = reader.next()) != null) {
        System.out.println(String.format("    %d: %s", seq++, edit.toString()));
      }
    } catch (IOException ioe) {
      System.err.println("ERROR reading log " + log.getName() + ": " + ioe.getMessage());
      ioe.printStackTrace();
    }
  }
}
