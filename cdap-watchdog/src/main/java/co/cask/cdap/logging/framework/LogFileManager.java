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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.logging.write.FileMetaDataManager;
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
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
  private final String permissions;

  LogFileManager(long maxFileLifetimeMs, int syncIntervalBytes, Schema schema,
                 FileMetaDataManager fileMetaDataManager,
                 LocationFactory locationFactory, CConfiguration cConfiguration) {
    this.maxLifetimeMillis = maxFileLifetimeMs;
    this.syncIntervalBytes = syncIntervalBytes;
    this.schema = schema;
    this.fileMetaDataManager = fileMetaDataManager;
    this.logsDirectoryLocation = locationFactory.create("logs");
    this.outputStreamMap = new HashMap<>();
    // if permission is null, we use rw for owner(cdap) and no permissions for group and others
    this.permissions = cConfiguration.get(Constants.LogSaver.PERMISSION, "600");
  }

  /**
   * Get log file output stream for the give logging context and timestamp - return an already open file,
   * or create and return a new file or rotate a file based on time and return the corresponding stream.
   * @param logPathIdentifier identify logging context
   * @param eventTimestamp timestamp from logging event
   * @return LogFileOutputStream output stream to the log file
   * @throws IOException if there is exception while getting location or while writing meta data
   */
  public LogFileOutputStream getLogFileOutputStream(LogPathIdentifier logPathIdentifier,
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
    try {
      fileMetaDataManager.writeMetaData(identifier, timestamp, location.getTimeStamp(), location.getLocation());
    } catch (Throwable e) {
      // delete created file as there was exception while writing meta data
      Locations.deleteQuietly(location.getLocation());
      throw new IOException(e);
    }

    LOG.info("Created Avro file at {}", location);
    LogFileOutputStream logFileOutputStream = new LogFileOutputStream(
      location.getLocation(), schema, syncIntervalBytes, location.getTimeStamp(), new Closeable() {
      @Override
      public void close() throws IOException {
        outputStreamMap.remove(identifier);
      }
    });

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
    while (!location.getLocation().createNew(permissions)) {
      Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.MILLISECONDS);
      location = getLocation(logPathIdentifier);
    }
    LOG.trace("created new file at Location {}", location);
    return location;
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

  private void ensureDirectoryCheck(Location location) throws IOException {
    if (!location.isDirectory() && !location.mkdirs(permissions) && !location.isDirectory()) {
      throw new IOException(
        String.format("File Exists at the logging location %s, Expected to be a directory", location));
    }
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
  }
}
