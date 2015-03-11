/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.logging.write;

import co.cask.cdap.common.logging.LoggingContext;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Helper class that manages writing of KafkaLogEvent to Avro files. The events are written into appropriate files
 * based on the LoggingContext of the event. The files are also rotated based on size. This class is not thread-safe.
 */
public final class AvroFileWriter implements Closeable, Flushable {
  private static final Logger LOG = LoggerFactory.getLogger(AvroFileWriter.class);

  private final FileMetaDataManager fileMetaDataManager;
  private final Location rootDir;
  private final String logBaseDir;
  private final Schema schema;
  private final int syncIntervalBytes;
  private final Map<String, AvroFile> fileMap;
  private final long maxFileSize;
  private final long inactiveIntervalMs;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Constructs an AvroFileWriter object.
   * @param fileMetaDataManager used to store file meta data.
   * @param rootDir the CDAP root dir on the filesystem
   * @param logBaseDir the basedirectory for logs as defined in configuration
   * @param schema schema of the Avro data to be written.
   * @param maxFileSize Avro files greater than maxFileSize will get rotated.
   * @param syncIntervalBytes the approximate number of uncompressed bytes to write in each block.
   * @param inactiveIntervalMs files that have no data written for more than inactiveIntervalMs will be closed.
   */
  public AvroFileWriter(FileMetaDataManager fileMetaDataManager, Location rootDir, String logBaseDir, Schema schema,
                        long maxFileSize, int syncIntervalBytes, long inactiveIntervalMs) {
    this.fileMetaDataManager = fileMetaDataManager;
    this.rootDir = rootDir;
    this.logBaseDir = logBaseDir;
    this.schema = schema;
    this.syncIntervalBytes = syncIntervalBytes;
    this.fileMap = Maps.newHashMap();
    this.maxFileSize = maxFileSize;
    this.inactiveIntervalMs = inactiveIntervalMs;
  }

  /**
   * Appends a log event to an appropriate Avro file based on LoggingContext. If the log event does not contain
   * LoggingContext then the event will be dropped.
   * @param events Log event
   * @throws IOException
   */
  public void append(List<? extends LogWriteEvent> events) throws Exception {
    if (events.isEmpty()) {
      LOG.debug("Empty append list.");
      return;
    }

    LogWriteEvent event = events.get(0);
    LoggingContext loggingContext = event.getLoggingContext();

    if (LOG.isTraceEnabled()) {
      LOG.trace("Appending {} messages for logging context {}", events.size(),
                loggingContext.getLogPathFragment(logBaseDir));
    }

    long timestamp = event.getLogEvent().getTimeStamp();
    AvroFile avroFile = getAvroFile(loggingContext, timestamp);
    avroFile = rotateFile(avroFile, loggingContext, timestamp);

    for (LogWriteEvent e : events) {
      avroFile.append(e);
    }
    avroFile.flush();
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    // First checkpoint state
    try {
      flush();
    } catch (Exception e) {
      LOG.error("Caught exception while checkpointing", e);
    }

    // Close all files
    LOG.info("Closing all files");
    for (Map.Entry<String, AvroFile> entry : fileMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (Throwable e) {
        LOG.error(String.format("Caught exception while closing file %s", entry.getValue().getLocation().toURI()), e);
      }
    }
    fileMap.clear();
  }

  @Override
  public void flush() throws IOException {
    long currentTs = System.currentTimeMillis();

    for (Iterator<Map.Entry<String, AvroFile>> it = fileMap.entrySet().iterator(); it.hasNext();) {
      AvroFile avroFile = it.next().getValue();
      avroFile.sync();

      // Close inactive files
      if (currentTs - avroFile.getLastModifiedTs() > inactiveIntervalMs) {
        avroFile.close();
        it.remove();
      }
    }
  }

  private AvroFile getAvroFile(LoggingContext loggingContext, long timestamp) throws Exception {
    AvroFile avroFile = fileMap.get(loggingContext.getLogPathFragment(logBaseDir));
    if (avroFile == null) {
      avroFile = createAvroFile(loggingContext, timestamp);
    }
    return avroFile;
  }

  private AvroFile createAvroFile(LoggingContext loggingContext, long timestamp) throws Exception {
    long currentTs = System.currentTimeMillis();
    Location location = createLocation(loggingContext.getLogPathFragment(logBaseDir), currentTs);
    LOG.info(String.format("Creating Avro file %s", location.toURI()));
    AvroFile avroFile = new AvroFile(location);
    try {
      avroFile.open();
    } catch (IOException e) {
      closeAndDelete(avroFile);
      throw e;
    }
    try {
      fileMetaDataManager.writeMetaData(loggingContext, timestamp, location);
    } catch (Throwable e) {
      closeAndDelete(avroFile);
      throw new IOException(e);
    }
    fileMap.put(loggingContext.getLogPathFragment(logBaseDir), avroFile);
    return avroFile;
  }

  private Location createLocation(String pathFragment, long timestamp) throws IOException {
    String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    String fileName = String.format("%s.avro", timestamp);
    return rootDir.append(pathFragment).append(date).append(fileName);
  }

  private AvroFile rotateFile(AvroFile avroFile, LoggingContext loggingContext, long timestamp) throws Exception {
    if (avroFile.getPos() > maxFileSize) {
      LOG.info(String.format("Rotating file %s", avroFile.getLocation().toURI()));
      flush();
      avroFile.close();
      return createAvroFile(loggingContext, timestamp);
    }
    return avroFile;
  }

  private void closeAndDelete(AvroFile avroFile) {
    try {
      avroFile.close();
      if (avroFile.getLocation().exists()) {
        avroFile.getLocation().delete();
      }
    } catch (IOException e) {
      LOG.error("Error while closing and deleting file {}", avroFile.getLocation(), e);
    }
  }

  /**
   * Represents an Avro file.
   */
  public class AvroFile implements Closeable {
    private final Location location;
    private FSDataOutputStream outputStream;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private long lastModifiedTs;
    private boolean isOpen = false;

    public AvroFile(Location location) {
      this.location = location;
    }

    /**
     * Opens the underlying file for writing. If open throws an exception, then @{link #close()} needs to be called to
     * free resources.
     * @throws IOException
     */
    void open() throws IOException {
      this.outputStream = new FSDataOutputStream(location.getOutputStream(), null);
      this.dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema));
      this.dataFileWriter.create(schema, this.outputStream);
      this.dataFileWriter.setSyncInterval(syncIntervalBytes);
      this.lastModifiedTs = System.currentTimeMillis();
      this.isOpen = true;
    }

    public Location getLocation() {
      return location;
    }

    public void append(LogWriteEvent event) throws IOException {
      dataFileWriter.append(event.getGenericRecord());
      lastModifiedTs = System.currentTimeMillis();
    }

    public long getPos() throws IOException {
      return outputStream.getPos();
    }

    public long getLastModifiedTs() {
      return lastModifiedTs;
    }

    public void flush() throws IOException {
      dataFileWriter.flush();
      outputStream.hflush();
    }

    public void sync() throws IOException {
      dataFileWriter.flush();
      outputStream.hsync();
    }

    @Override
    public void close() throws IOException {
      if (!isOpen) {
        return;
      }

      try {
        if (dataFileWriter != null) {
          dataFileWriter.close();
        }
      } finally {
        if (outputStream != null) {
          outputStream.close();
        }
      }

      isOpen = false;
    }
  }
}
