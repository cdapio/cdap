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
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TxConstants;
import org.apache.tephra.metrics.MetricsCollector;
import org.apache.tephra.snapshot.SnapshotCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Persists transaction snapshots and write-ahead logs to files on the local filesystem.
 */
public class LocalFileTransactionStateStorage extends AbstractTransactionStateStorage {
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
  private final MetricsCollector metricsCollector;
  private File snapshotDir;

  @Inject
  public LocalFileTransactionStateStorage(Configuration conf, SnapshotCodecProvider codecProvider,
                                          MetricsCollector metricsCollector) {
    super(codecProvider);
    this.configuredSnapshotDir = conf.get(TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR);
    this.metricsCollector = metricsCollector;
  }

  @Override
  protected void startUp() throws Exception {
    Preconditions.checkState(configuredSnapshotDir != null,
        "Snapshot directory is not configured.  Please set " + TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR +
        " in configuration.");
    snapshotDir = new File(configuredSnapshotDir);
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
    // save the snapshot to a temporary file
    File snapshotTmpFile = new File(snapshotDir, TMP_SNAPSHOT_FILE_PREFIX + snapshot.getTimestamp());
    LOG.debug("Writing snapshot to temporary file {}", snapshotTmpFile);
    OutputStream out = Files.newOutputStreamSupplier(snapshotTmpFile).getOutput();
    boolean threw = true;
    try {
      codecProvider.encode(out, snapshot);
      threw = false;
    } finally {
      Closeables.close(out, threw);
    }

    // move the temporary file into place with the correct filename
    File finalFile = new File(snapshotDir, SNAPSHOT_FILE_PREFIX + snapshot.getTimestamp());
    if (!snapshotTmpFile.renameTo(finalFile)) {
      throw new IOException("Failed renaming temporary snapshot file " + snapshotTmpFile.getName() + " to " +
          finalFile.getName());
    }

    LOG.debug("Completed snapshot to file {}", finalFile);
  }

  @Override
  public TransactionSnapshot getLatestSnapshot() throws IOException {
    InputStream is = getLatestSnapshotInputStream();
    if (is == null) {
      return null;
    }
    try {
      return readSnapshotFile(is);
    } finally {
      is.close();
    }
  }

  @Override
  public TransactionVisibilityState getLatestTransactionVisibilityState() throws IOException {
    InputStream is = getLatestSnapshotInputStream();
    if (is == null) {
      return null;
    }
    try {
      return codecProvider.decodeTransactionVisibilityState(is);
    } finally {
      is.close();
    }
  }

  private InputStream getLatestSnapshotInputStream() throws IOException {
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

    return new FileInputStream(mostRecent.getFile());
  }

  private TransactionSnapshot readSnapshotFile(InputStream is) throws IOException {
    return codecProvider.decode(is);
  }

  @Override
  public long deleteOldSnapshots(int numberToKeep) throws IOException {
    File[] snapshotFiles = snapshotDir.listFiles(SNAPSHOT_FILE_FILTER);
    if (snapshotFiles.length == 0) {
      return -1;
    }
    TimestampedFilename[] snapshotFilenames = new TimestampedFilename[snapshotFiles.length];
    for (int i = 0; i < snapshotFiles.length; i++) {
      snapshotFilenames[i] = new TimestampedFilename(snapshotFiles[i]);
    }
    Arrays.sort(snapshotFilenames, Collections.reverseOrder());
    if (snapshotFilenames.length <= numberToKeep) {
      // nothing to delete, just return the oldest timestamp
      return snapshotFilenames[snapshotFilenames.length - 1].getTimestamp();
    }
    int toRemoveCount = snapshotFilenames.length - numberToKeep;
    TimestampedFilename[] toRemove = new TimestampedFilename[toRemoveCount];
    System.arraycopy(snapshotFilenames, numberToKeep, toRemove, 0, toRemoveCount);
    int removedCnt = 0;
    for (int i = 0; i < toRemove.length; i++) {
      File currentFile = toRemove[i].getFile();
      LOG.debug("Removing old snapshot file {}", currentFile.getAbsolutePath());
      if (!toRemove[i].getFile().delete()) {
        LOG.error("Failed deleting snapshot file {}", currentFile.getAbsolutePath());
      } else {
        removedCnt++;
      }
    }
    long oldestTimestamp = snapshotFilenames[numberToKeep - 1].getTimestamp();
    LOG.info("Removed {} out of {} expected snapshot files older than {}", removedCnt, toRemoveCount, oldestTimestamp);
    return oldestTimestamp;
  }

  @Override
  public List<String> listSnapshots() throws IOException {
    File[] snapshots = snapshotDir.listFiles(SNAPSHOT_FILE_FILTER);
    return Lists.transform(Arrays.asList(snapshots), new Function<File, String>() {
      @Nullable
      @Override
      public String apply(@Nullable File input) {
        return input.getName();
      }
    });
  }

  @Override
  public List<TransactionLog> getLogsSince(long timestamp) throws IOException {
    File[] logFiles = snapshotDir.listFiles(new LogFileFilter(timestamp, Long.MAX_VALUE));
    TimestampedFilename[] timestampedFiles = new TimestampedFilename[logFiles.length];
    for (int i = 0; i < logFiles.length; i++) {
      timestampedFiles[i] = new TimestampedFilename(logFiles[i]);
    }
    // logs need to be processed in ascending order
    Arrays.sort(timestampedFiles);
    return Lists.transform(Arrays.asList(timestampedFiles), new Function<TimestampedFilename, TransactionLog>() {
      @Nullable
      @Override
      public TransactionLog apply(@Nullable TimestampedFilename input) {
        return new LocalFileTransactionLog(input.getFile(), input.getTimestamp(), metricsCollector);
      }
    });
  }

  @Override
  public TransactionLog createLog(long timestamp) throws IOException {
    File newLogFile = new File(snapshotDir, LOG_FILE_PREFIX + timestamp);
    LOG.info("Creating new transaction log at {}", newLogFile.getAbsolutePath());
    return new LocalFileTransactionLog(newLogFile, timestamp, metricsCollector);
  }

  @Override
  public void deleteLogsOlderThan(long timestamp) throws IOException {
    File[] logFiles = snapshotDir.listFiles(new LogFileFilter(0, timestamp));
    int removedCnt = 0;
    for (File file : logFiles) {
      LOG.debug("Removing old transaction log {}", file.getPath());
      if (file.delete()) {
        removedCnt++;
      } else {
        LOG.warn("Failed to remove log file {}", file.getAbsolutePath());
      }
    }
    LOG.debug("Removed {} transaction logs older than {}", removedCnt, timestamp);
  }

  @Override
  public void setupStorage() throws IOException {
    // create the directory if it doesn't exist
    if (!snapshotDir.exists()) {
      if (!snapshotDir.mkdirs()) {
        throw new IOException("Failed to create directory " + configuredSnapshotDir +
                                " for transaction snapshot storage");
      }
    } else {
      Preconditions.checkState(snapshotDir.isDirectory(),
                               "Configured snapshot directory " + configuredSnapshotDir + " is not a directory!");
      Preconditions.checkState(snapshotDir.canWrite(), "Configured snapshot directory " +
        configuredSnapshotDir + " exists but is not writable!");
    }
  }

  @Override
  public List<String> listLogs() throws IOException {
    File[] logs = snapshotDir.listFiles(new LogFileFilter(0, Long.MAX_VALUE));
    return Lists.transform(Arrays.asList(logs), new Function<File, String>() {
      @Nullable
      @Override
      public String apply(@Nullable File input) {
        return input.getName();
      }
    });
  }

  private static class LogFileFilter implements FilenameFilter {
    private final long startTime;
    private final long endTime;

    public LogFileFilter(long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    @Override
    public boolean accept(File file, String s) {
      if (s.startsWith(LOG_FILE_PREFIX)) {
        String[] parts = s.split("\\.");
        if (parts.length == 2) {
          try {
            long fileTime = Long.parseLong(parts[1]);
            return fileTime >= startTime && fileTime < endTime;
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
