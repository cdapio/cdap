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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.tephra.TxConstants;
import org.apache.tephra.metrics.MetricsCollector;
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
  private final Configuration hConf;
  private final Path logPath;

  /**
   * Creates a new HDFS-backed write-ahead log for storing transaction state.
   * @param fs Open FileSystem instance for opening log files in HDFS.
   * @param hConf HDFS cluster configuration.
   * @param logPath Path to the log file.
   */
  public HDFSTransactionLog(final FileSystem fs, final Configuration hConf,
                            final Path logPath, long timestamp, MetricsCollector metricsCollector) {
    super(timestamp, metricsCollector);
    this.fs = fs;
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

    TransactionLogReader reader = null;
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
        reader = new HDFSTransactionLogReaderSupplier(fileReader).get();
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

  @VisibleForTesting
  static final class LogWriter implements TransactionLogWriter {
    private final SequenceFile.Writer internalWriter;
    public LogWriter(FileSystem fs, Configuration hConf, Path logPath) throws IOException {
      // TODO: retry a few times to ride over transient failures?
      SequenceFile.Metadata metadata = new SequenceFile.Metadata();
      metadata.set(new Text(TxConstants.TransactionLog.VERSION_KEY),
                   new Text(Byte.toString(TxConstants.TransactionLog.CURRENT_VERSION)));

      this.internalWriter = SequenceFile.createWriter(fs, hConf, logPath, LongWritable.class, TransactionEdit.class,
                                                      SequenceFile.CompressionType.NONE, null, null, metadata);
      LOG.debug("Created a new TransactionLog writer for " + logPath);
    }

    @Override
    public void append(Entry entry) throws IOException {
      internalWriter.append(entry.getKey(), entry.getEdit());
    }

    @Override
    public void commitMarker(int count) throws IOException {
      CommitMarkerCodec.writeMarker(internalWriter, count);
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
}
