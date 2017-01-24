/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.logging.framework;

import co.cask.cdap.logging.write.FileMetaDataManager;
import com.google.common.io.Closeables;
import org.apache.avro.Schema;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Class including logic for getting log file to write to. Used by {@link CDAPLogAppender}
 */
class LogFileManager implements Flushable {
  private static final Logger LOG = LoggerFactory.getLogger(LogFileManager.class);

  private final long maxLifetimeMillis;
  private final Map<LogPathIdentifier, LogFileOutputStream> outputStreamMap;
  private final Location logsDirectoryLocation;
  private final FileMetaDataManager fileMetaDataManager;
  private final int syncIntervalBytes;
  private final Schema schema;

  LogFileManager(long maxFileLifetimeMs, int syncIntervalBytes, Schema schema,
                 FileMetaDataManager fileMetaDataManager,
                 LocationFactory locationFactory) {
    this.maxLifetimeMillis = maxFileLifetimeMs;
    this.syncIntervalBytes = syncIntervalBytes;
    this.schema = schema;
    this.fileMetaDataManager = fileMetaDataManager;
    this.logsDirectoryLocation = locationFactory.create("logs");
    this.outputStreamMap = new HashMap<>();
  }

  /**
   * Get log file output stream for the give logging context and timestamp - return an already open file,
   * or create and return a new file or rotate a file based on time and return the corresponding stream.
   * @param logPathIdentifier
   * @param timestamp
   * @return LogFileOutputStream
   * @throws IOException
   */
  public LogFileOutputStream getLogFileOutputStream(LogPathIdentifier logPathIdentifier,
                                                    long timestamp) throws IOException {
    LogFileOutputStream logFileOutputStream = outputStreamMap.get(logPathIdentifier);
    if (logFileOutputStream == null) {
      logFileOutputStream = createOutputStream(logPathIdentifier, timestamp);
    } else {
      // rotate the file if needed (time has passed)
      logFileOutputStream = rotateOutputStream(logFileOutputStream, logPathIdentifier, timestamp);
    }
    return logFileOutputStream;
  }

  private LogFileOutputStream createOutputStream(final LogPathIdentifier identifier,
                                                 long timestamp) throws IOException {
    Location location = getLocation(identifier);
    try {
      int seqId = getSequenceId(location);
      fileMetaDataManager.writeMetaData(identifier, timestamp, seqId, location);
    } catch (Throwable e) {
      throw new IOException(e);
    }
    LOG.info("Creating Avro file {}", location);
    // if the create output stream step fails, we will have metadata entry but not actual file,
    // this should be handled and cleaned by Logcleanup thread
    LogFileOutputStream logFileOutputStream = new LogFileOutputStream(location, schema, syncIntervalBytes,
                                                                      new Closeable() {
                                                                        @Override
                                                                        public void close() throws IOException {
                                                                          outputStreamMap.remove(identifier);
                                                                        }
                                                                      });

    outputStreamMap.put(identifier, logFileOutputStream);
    return logFileOutputStream;
  }

  private LogFileOutputStream rotateOutputStream(LogFileOutputStream logFileOutputStream,
                                                 LogPathIdentifier identifier, long timestamp) throws IOException {
    long currentTs = System.currentTimeMillis();
    long timeSinceFileCreate = currentTs - logFileOutputStream.getCreateTime();
    if (timeSinceFileCreate > maxLifetimeMillis) {
      logFileOutputStream.close();
      return createOutputStream(identifier, timestamp);
    }
    return logFileOutputStream;
  }

  /**
   * closes all the open output streams in the map
   */
  public void close() {
    // close all the files in outputStreamMap
    // clear the map
    Collection<LogFileOutputStream> streams = new ArrayList<>(outputStreamMap.values());
    outputStreamMap.clear();

    for (LogFileOutputStream stream : streams) {
      Closeables.closeQuietly(stream);
    }
  }

  /**
   * flushes the contents of all the open log files
   * @throws IOException
   */
  @Override
  public void flush() throws IOException {
    // perform flush on all the files in the outputStreamMap
    for (LogFileOutputStream file : outputStreamMap.values()) {
      file.flush();
    }
  }

  void ensureDirectoryCheck(Location location) throws IOException {
    if (!location.exists()) {
      location.mkdirs();
    } else {
      if (!location.isDirectory()) {
        throw new IOException(
          String.format("File Exists at the logging location %s, Expected to be a directory", location));
      }
    }
  }

  private Location getLocation(LogPathIdentifier logPathIdentifier) throws IOException {
    ensureDirectoryCheck(logsDirectoryLocation);

    String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    Location contextLocation =
      logsDirectoryLocation.append(logPathIdentifier.getNamespaceId())
        .append(date)
        .append(logPathIdentifier.getPathId1())
        .append(logPathIdentifier.getPathId2());
    ensureDirectoryCheck(contextLocation);

    int sequenceId = contextLocation.list().size();
    String fileName = String.format("%s.avro", sequenceId);
    return contextLocation.append(fileName);
  }


  /**
   * file name is of the format <seq-id-integer>.avro, we return the sequence id from the location name.
   * @param location
   * @return sequenceId
   */
  private int getSequenceId(Location location) {
    String[] fileName = location.getName().split("\\.");
    return Integer.valueOf(fileName[0]);
  }
}
