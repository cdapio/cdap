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
public class AvroFileManager {
  private static final Logger LOG = LoggerFactory.getLogger(CDAPLogAppender.class);

  private final long maxLifetimeMillis;
  private final Map<LogPathIdentifier, AvroFile> fileMap;
  private final LoggingLocationFactory loggingLocationFactory;
  private final FileMetaDataManager fileMetaDataManager;
  private final int syncIntervalBytes;
  private final Schema schema;

  public AvroFileManager(long maxFileLifetimeMs, int syncIntervalBytes, Schema schema,
                         FileMetaDataManager fileMetaDataManager,
                         LocationFactory locationFactory) {
    this.maxLifetimeMillis = maxFileLifetimeMs;
    this.syncIntervalBytes = syncIntervalBytes;
    this.schema = schema;
    this.fileMetaDataManager = fileMetaDataManager;
    this.loggingLocationFactory = new LoggingLocationFactory(locationFactory);
    this.fileMap = new HashMap<>();
  }

  /**
   * Get an avro file for the give logging context and timestamp - return an already open file,
   * or create and return a new file or rotate a file based on time and return.
   * @param logPathIdentifier
   * @param timestamp
   * @return
   * @throws IOException
   */
  public AvroFile getAvroFile(LogPathIdentifier logPathIdentifier, long timestamp) throws IOException {
    AvroFileManager.AvroFile avroFile = fileMap.get(logPathIdentifier);
    // If the file is not open then set reference to null so that a new one gets created
    if (avroFile != null && !avroFile.isOpen()) {
      avroFile = null;
    }

    if (avroFile == null) {
      avroFile = createAvroFile(logPathIdentifier, timestamp);
    }
    // rotate the file if needed (time has passed)
    avroFile = rotateFile(avroFile, logPathIdentifier, timestamp);
    return avroFile;
  }

  private AvroFile createAvroFile(LogPathIdentifier identifier, long timestamp) throws IOException {
    Location location = loggingLocationFactory.getLocation(identifier, timestamp);
    LOG.info("Creating Avro file {}", location);
    AvroFile avroFile = new AvroFile(location);
    try {
      avroFile.open();
    } catch (IOException e) {
      closeAndDelete(avroFile);
      throw e;
    }
    try {
      fileMetaDataManager.writeMetaData(identifier, timestamp, location);
    } catch (Throwable e) {
      closeAndDelete(avroFile);
      throw new IOException(e);
    }

    fileMap.put(identifier, avroFile);
    return avroFile;
  }

  private AvroFile rotateFile(AvroFile avroFile, LogPathIdentifier identifier, long timestamp) throws IOException {
    long currentTs = System.currentTimeMillis();
    long timeSinceFileCreate = currentTs - avroFile.getCreateTime();
    if (timeSinceFileCreate > maxLifetimeMillis) {
      avroFile.close();
    }
    if (!avroFile.isOpen()) {
      LOG.info("Rotating a closed file {}", avroFile.getLocation());
      return createAvroFile(identifier, timestamp);
    }
    return avroFile;
  }

  /**
   * closes all the open avro files in the map
   */
  public void close() {
    // close all the files in fileMap
    // clear the map
    for (AvroFile file : fileMap.values()) {
      try {
        file.flush();
        file.close();
      } catch (Exception e) {
        LOG.debug("Exception while flushing and closing contents to file {}", file.getLocation(), e);
      }
    }
    fileMap.clear();
  }

  private void closeAndDelete(AvroFile avroFile) {
    try {
      try {
        avroFile.close();
      } finally {
        if (avroFile.getLocation().exists()) {
          avroFile.getLocation().delete();
        }
      }
    } catch (IOException e) {
      LOG.error("Error while closing and deleting file {}", avroFile.getLocation(), e);
    }
  }

  /**
   * flushes the contents of all the open avro files
   * @throws IOException
   */
  public void flush() throws IOException {
    // perform flush on all the files in the fileMap
    for (AvroFile file : fileMap.values()) {
      try {
        file.flush();
      } catch (Exception e) {
        LOG.debug("Exception while flushing contents to file {}", file.getLocation());
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
  public class AvroFile implements Closeable {
    private final Location location;
    private FSDataOutputStream outputStream;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private long createTime;
    private boolean isOpen = false;

    public AvroFile(Location location) {
      this.location = location;
    }

    /**
     * Opens the underlying file for writing.
     * If open throws an exception then underlying file may still need to be deleted.
     *
     * @throws IOException
     */
    void open() throws IOException {
      try {
        this.outputStream = new FSDataOutputStream(location.getOutputStream(), null);
        this.dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema));
        this.dataFileWriter.create(schema, this.outputStream);
        this.dataFileWriter.setSyncInterval(syncIntervalBytes);
        this.createTime = System.currentTimeMillis();
        // Sync the file as soon as it is created, otherwise a zero length Avro file can get created on OOM
        sync();
      } catch (Exception e) {
        close();
        throw new IOException("Exception while creating file " + location, e);
      }
      this.isOpen = true;
    }

    public boolean isOpen() {
      return isOpen;
    }

    public Location getLocation() {
      return location;
    }

    public void append(ILoggingEvent event) throws IOException {
      try {
        dataFileWriter.append(co.cask.cdap.logging.serialize.LoggingEvent.encode(schema, event));
      } catch (Exception e) {
        close();
        throw new IOException("Exception while appending to file " + location, e);
      }
    }

    public long getPos() throws IOException {
      try {
        return outputStream.getPos();
      } catch (Exception e) {
        close();
        throw new IOException("Exception while getting position of file " + location, e);
      }
    }

    public long getCreateTime() {
      return createTime;
    }

    public void flush() throws IOException {
      try {
        dataFileWriter.flush();
        outputStream.hsync();
      } catch (Exception e) {
        close();
        throw new IOException("Exception while flushing file " + location, e);
      }
    }

    public void sync() throws IOException {
      try {
        dataFileWriter.flush();
        outputStream.hsync();
      } catch (Exception e) {
        close();
        throw new IOException("Exception while syncing file " + location, e);
      }
    }

    @Override
    public void close() throws IOException {
      if (!isOpen) {
        return;
      }

      LOG.trace("Closing file {}", location);
      isOpen = false;

      try {
        if (dataFileWriter != null) {
          dataFileWriter.close();
        }
      } finally {
        if (outputStream != null) {
          outputStream.close();
        }
      }
    }
  }

  class LoggingLocationFactory {
    Location logsLocation;

    LoggingLocationFactory(LocationFactory locationFactory) {
      this.logsLocation = locationFactory.create("logs");

    }

    private Location getLocation(LogPathIdentifier logPathIdentifier, long timestamp) throws IOException {
      // create "<hdfs-namepsace>/logs" if the dir doesn't exist already
      if (!logsLocation.exists()) {
        logsLocation.mkdirs();
      }
      String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
      Location namespaceLocation = logsLocation.append(logPathIdentifier.getNamespaceId()).append(date);

      if (!namespaceLocation.exists()) {
        namespaceLocation.mkdirs();
      }
      String fileName = String.format("%s:%s.avro", logPathIdentifier.getLogFilePrefix(), timestamp);
      return namespaceLocation.append(fileName);
    }
  }
}
