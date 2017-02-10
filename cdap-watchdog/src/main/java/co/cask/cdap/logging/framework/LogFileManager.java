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

import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.io.Syncable;
import co.cask.cdap.logging.meta.FileMetaDataWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Uninterruptibles;
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
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Class including logic for getting log file to write to. Used by {@link CDAPLogAppender}
 */
final class LogFileManager implements Flushable, Syncable {
  private static final Logger LOG = LoggerFactory.getLogger(LogFileManager.class);

  private final String dirPermissions;
  private final String filePermissions;
  private final int syncIntervalBytes;
  private final long maxLifetimeMillis;
  private final long maxFileSizeInBytes;
  private final Map<LogPathIdentifier, LogFileOutputStream> outputStreamMap;
  private final Location logsDirectoryLocation;
  private final FileMetaDataWriter fileMetaDataWriter;
  private final Schema schema;

  LogFileManager(String dirPermissions, String filePermissions,
                 long maxFileLifetimeMs, long maxFileSizeInBytes, int syncIntervalBytes,
                 Schema schema, FileMetaDataWriter fileMetaDataWriter, LocationFactory locationFactory) {
    this.dirPermissions = dirPermissions;
    this.filePermissions = filePermissions;
    this.maxLifetimeMillis = maxFileLifetimeMs;
    this.maxFileSizeInBytes = maxFileSizeInBytes;
    this.syncIntervalBytes = syncIntervalBytes;
    this.schema = schema;
    this.fileMetaDataWriter = fileMetaDataWriter;
    this.logsDirectoryLocation = locationFactory.create("logs");
    this.outputStreamMap = new HashMap<>();
  }

  /**
   * Get log file output stream for the give logging context and timestamp - return an already open file,
   * or create and return a new file or rotate a file based on time and return the corresponding stream.
   * @param logPathIdentifier identify logging context
   * @param eventTimestamp timestamp from logging event
   * @return LogFileOutputStream output stream to the log file
   * @throws IOException if there is exception while getting location or while writing meta data
   */
  LogFileOutputStream getLogFileOutputStream(LogPathIdentifier logPathIdentifier,
                                             long eventTimestamp) throws IOException {
    LogFileOutputStream logFileOutputStream = outputStreamMap.get(logPathIdentifier);
    if (logFileOutputStream == null) {
      logFileOutputStream = createOutputStream(logPathIdentifier, eventTimestamp);
    } else {
      // rotate the file if needed (time has passed)
      logFileOutputStream = rotateOutputStream(logFileOutputStream, logPathIdentifier, eventTimestamp);
    }
    return logFileOutputStream;
  }

  private LogFileOutputStream createOutputStream(final LogPathIdentifier identifier,
                                                 long timestamp) throws IOException {
    TimeStampLocation location = createLocation(identifier);
    LogFileOutputStream logFileOutputStream = new LogFileOutputStream(
      location.getLocation(), schema, syncIntervalBytes, location.getTimeStamp(), new Closeable() {
      @Override
      public void close() throws IOException {
        outputStreamMap.remove(identifier);
      }
    });
    logFileOutputStream.flush();
    LOG.info("Created Avro file at {}", location);

    // we write meta data after creating output stream, as we want to avoid having meta data for zero-length avro file.
    // LogFileOutputStream creation writes the schema to the avro file. if meta data write fails,
    // we then close output stream and delete the file
    try {
      fileMetaDataWriter.writeMetaData(identifier, timestamp, location.getTimeStamp(), location.getLocation());
    } catch (Throwable e) {
      // delete created file as there was exception while writing meta data
      Closeables.closeQuietly(logFileOutputStream);
      Locations.deleteQuietly(location.getLocation());
      throw new IOException(e);
    }

    outputStreamMap.put(identifier, logFileOutputStream);
    return logFileOutputStream;
  }


  private TimeStampLocation createLocation(LogPathIdentifier logPathIdentifier) throws IOException {
    // if createNew fails, we retry after sleeping for a milli second as we use current timestamp for fileName.
    // this retry should succeed on any potential conflicts, though the likelihood of conflict is very small.
    TimeStampLocation location = getLocation(logPathIdentifier);
    // NOTE : we are creating a file before writing meta data, so if there are simultaneous write to same folder from
    // multiple instance of log appender, they wouldn't overwrite the meta data and would fail early,
    // so we can retry with different timestamp for file creation.
    // however there is a possibility for the log.saver could crash after file was created
    // but before meta data was written, log clean up should handle this scenario.
    // log cleanup shouldn't rely on metadata table for cleaning up old files.
    while (!location.getLocation().createNew(filePermissions)) {
      Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.MILLISECONDS);
      location = getLocation(logPathIdentifier);
    }
    LOG.trace("Created new file at Location {}", location);
    return location;
  }

  private LogFileOutputStream rotateOutputStream(LogFileOutputStream logFileOutputStream,
                                                 LogPathIdentifier identifier, long timestamp) throws IOException {
    long currentTs = System.currentTimeMillis();
    long timeSinceFileCreate = currentTs - logFileOutputStream.getCreateTime();
    if (timeSinceFileCreate > maxLifetimeMillis || logFileOutputStream.getSize() > maxFileSizeInBytes) {
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
   * Flushes the contents of all the open log files
   * @throws IOException if flush failed on any of the underlying stream.
   */
  @Override
  public void flush() throws IOException {
    // perform flush on all the files in the outputStreamMap
    long currentTs = System.currentTimeMillis();
    Iterator<LogFileOutputStream> itor = outputStreamMap.values().iterator();
    while (itor.hasNext()) {
      LogFileOutputStream stream = itor.next();
      stream.flush();
      long timeSinceFileCreated = currentTs - stream.getCreateTime();
      if (timeSinceFileCreated > maxLifetimeMillis) {
        // Remove it from the map first.
        // This also make sure even if the close failed, the stream won't stay in the map
        itor.remove();
        stream.close();
      }
    }
  }

  @Override
  public void sync() throws IOException {
    // Perform sync on all files
    for (LogFileOutputStream file : outputStreamMap.values()) {
      file.sync();
    }
  }

  private void ensureDirectoryCheck(Location location) throws IOException {
    if (!location.isDirectory() && !location.mkdirs(dirPermissions) && !location.isDirectory()) {
      throw new IOException(
        String.format("File Exists at the logging location %s, Expected to be a directory", location));
    }
  }

  // only used by tests
  @VisibleForTesting
  @Nullable
  LogFileOutputStream getActiveOutputStream(LogPathIdentifier logPathIdentifier) {
    return outputStreamMap.get(logPathIdentifier);
  }

  private TimeStampLocation getLocation(LogPathIdentifier logPathIdentifier) throws IOException {
    ensureDirectoryCheck(logsDirectoryLocation);
    long currentTime = System.currentTimeMillis();
    String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date(currentTime));
    Location contextLocation =
      logsDirectoryLocation.append(logPathIdentifier.getNamespaceId())
        .append(date)
        .append(logPathIdentifier.getPathId1())
        .append(logPathIdentifier.getPathId2());
    ensureDirectoryCheck(contextLocation);

    String fileName = String.format("%s.avro", currentTime);
    return new TimeStampLocation(contextLocation.append(fileName), currentTime);
  }

  private class TimeStampLocation {
    private final Location location;
    private final long timeStamp;

    private TimeStampLocation(final Location location, final long timeStamp) {
      this.location = location;
      this.timeStamp = timeStamp;
    }

    private Location getLocation() {
      return location;
    }

    private long getTimeStamp() {
      return timeStamp;
    }

    @Override
    public String toString() {
      return "TimeStampLocation{" +
        "location=" + location +
        ", timeStamp=" + timeStamp +
        '}';
    }
  }
}
