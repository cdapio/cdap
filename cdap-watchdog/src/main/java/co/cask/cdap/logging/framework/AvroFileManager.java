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

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.logging.write.FileMetaDataManager;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Class including logic for getting avro files to write to. Used by {@link CDAPLogAppender}
 */
class AvroFileManager {
  private static final Logger LOG = LoggerFactory.getLogger(CDAPLogAppender.class);

  private final long maxLifetimeMillis;
  private final Map<LogPathIdentifier, LogFileOutputStream> fileMap;
  private final Location logsDirectoryLocation;
  private final FileMetaDataManager fileMetaDataManager;
  private final int syncIntervalBytes;
  private final Schema schema;

  AvroFileManager(long maxFileLifetimeMs, int syncIntervalBytes, Schema schema,
                  FileMetaDataManager fileMetaDataManager,
                  LocationFactory locationFactory) {
    this.maxLifetimeMillis = maxFileLifetimeMs;
    this.syncIntervalBytes = syncIntervalBytes;
    this.schema = schema;
    this.fileMetaDataManager = fileMetaDataManager;
    this.logsDirectoryLocation = locationFactory.create("logs");
    this.fileMap = new HashMap<>();
  }

  /**
   * Get an avro file for the give logging context and timestamp - return an already open file,
   * or create and return a new file or rotate a file based on time and return.
   * @param logPathIdentifier
   * @param timestamp
   * @return LogFileOutputStream
   * @throws IOException
   */
  public LogFileOutputStream getAvroFile(LogPathIdentifier logPathIdentifier, long timestamp) throws IOException {
    LogFileOutputStream logFileOutputStream = fileMap.get(logPathIdentifier);
    // If the file is not open then set reference to null so that a new one gets created
    if (logFileOutputStream == null) {
      logFileOutputStream = createAvroFile(logPathIdentifier, timestamp);
    }
    // rotate the file if needed (time has passed)
    logFileOutputStream = rotateFile(logFileOutputStream, logPathIdentifier, timestamp);
    return logFileOutputStream;
  }

  private LogFileOutputStream createAvroFile(LogPathIdentifier identifier, long timestamp) throws IOException {
    Location location = getLocation(identifier, timestamp);
    LOG.info("Creating Avro file {}", location);
    LogFileOutputStream logFileOutputStream = new LogFileOutputStream(location);
    try {
      fileMetaDataManager.writeMetaData(identifier, timestamp, location);
    } catch (Throwable e) {
      closeAndDelete(logFileOutputStream);
      throw new IOException(e);
    }

    fileMap.put(identifier, logFileOutputStream);
    return logFileOutputStream;
  }

  private LogFileOutputStream rotateFile(LogFileOutputStream logFileOutputStream,
                                         LogPathIdentifier identifier, long timestamp) throws IOException {
    long currentTs = System.currentTimeMillis();
    long timeSinceFileCreate = currentTs - logFileOutputStream.getCreateTime();
    if (timeSinceFileCreate > maxLifetimeMillis) {
      logFileOutputStream.close();
      return createAvroFile(identifier, timestamp);
    }
    return logFileOutputStream;
  }

  /**
   * closes all the open avro files in the map
   */
  public void close() {
    // close all the files in fileMap
    // clear the map
    for (LogFileOutputStream file : fileMap.values()) {
      try {
        file.flush();
      } catch (Exception e) {
        LOG.debug("Exception while flushing contents to file {}", file.getLocation(), e);
      } finally {
        Closeables.closeQuietly(file);
      }
    }
    fileMap.clear();
  }

  private void closeAndDelete(LogFileOutputStream logFileOutputStream) {
    try {
      try {
        logFileOutputStream.close();
      } finally {
        if (logFileOutputStream.getLocation().exists()) {
          logFileOutputStream.getLocation().delete();
        }
      }
    } catch (IOException e) {
      LOG.error("Error while closing and deleting file {}", logFileOutputStream.getLocation(), e);
    }
  }

  /**
   * flushes the contents of all the open avro files
   * @throws IOException
   */
  public void flush() throws IOException {
    // perform flush on all the files in the fileMap
    for (LogFileOutputStream file : fileMap.values()) {
      try {
        file.flush();
      } catch (Exception e) {
        LOG.debug("Exception while flushing contents to file {}", file.getLocation());
        Closeables.closeQuietly(file);
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Represents an Avro file.
   *
   * Since there is no way to check the state of the underlying file on an exception,
   * all methods of this class assume that the file state is bad on any exception and close the file.
   */
  class LogFileOutputStream implements Closeable {
    private final Location location;
    private FSDataOutputStream outputStream;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private long createTime;

    LogFileOutputStream(Location location) throws IOException {
      this.location = location;
      try {
        this.outputStream = new FSDataOutputStream(location.getOutputStream(), null);
        this.dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema));
        this.dataFileWriter.create(schema, this.outputStream);
        this.dataFileWriter.setSyncInterval(syncIntervalBytes);
        this.createTime = System.currentTimeMillis();
      } catch (Exception e) {
        Closeables.closeQuietly(dataFileWriter);
        Closeables.closeQuietly(outputStream);
        throw new IOException("Exception while creating file " + location, e);
      }
    }

    public Location getLocation() {
      return location;
    }

    public void append(ILoggingEvent event) throws IOException {
      try {
        dataFileWriter.append(co.cask.cdap.logging.serialize.LoggingEvent.encode(schema, event));
      } catch (Exception e) {
        throw new IOException("Exception while appending to file " + location, e);
      }
    }

    private long getCreateTime() {
      return createTime;
    }

    public void flush() throws IOException {
      try {
        dataFileWriter.flush();
        outputStream.hsync();
      } catch (Exception e) {
        throw new IOException("Exception while flushing file " + location, e);
      }
    }

    public void sync() throws IOException {
      try {
        dataFileWriter.flush();
        outputStream.hsync();
      } catch (Exception e) {
        throw new IOException("Exception while syncing file " + location, e);
      }
    }

    @Override
    public void close() throws IOException {
      LOG.trace("Closing file {}", location);

      if (dataFileWriter != null) {
        dataFileWriter.close();
      }
    }
  }

  private Location getLocation(LogPathIdentifier logPathIdentifier, long timestamp) throws IOException {
    if (!logsDirectoryLocation.exists()) {
      try {
        logsDirectoryLocation.mkdirs();
      } catch (IOException e) {
        LOG.error("Unable to create logging base directory at {} ", logsDirectoryLocation, e);
        throw e;
      }
    }

    String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    Location namespaceLocation = logsDirectoryLocation.append(logPathIdentifier.getNamespaceId()).append(date);

    if (!namespaceLocation.exists()) {
      namespaceLocation.mkdirs();
    }
    String fileName = String.format("%s:%s.avro", logPathIdentifier.getLogFilePrefix(), timestamp);
    return namespaceLocation.append(fileName);
  }
}
