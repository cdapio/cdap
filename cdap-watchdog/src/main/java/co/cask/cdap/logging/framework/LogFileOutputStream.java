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
import co.cask.cdap.common.io.ByteBuffers;
import co.cask.cdap.common.io.Syncable;
import co.cask.cdap.logging.serialize.LoggingEvent;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import com.google.common.io.Closeables;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Represents output stream for a log file.
 *
 * Since there is no way to check the state of the underlying file on an exception,
 * all methods of this class assume that the file state is bad on any exception and close the file.
 */

class LogFileOutputStream implements Closeable, Flushable, Syncable {
  private static final Logger LOG = LoggerFactory.getLogger(LogFileOutputStream.class);

  private final Location location;
  private final long createTime;
  private final Closeable closeable;
  private final LoggingEventSerializer serializer;

  private OutputStream outputStream;
  private DataFileWriter<GenericRecord> dataFileWriter;
  private long fileSize;

  LogFileOutputStream(Location location, String filePermissions,
                      int syncIntervalBytes, long createTime, Closeable closeable) throws IOException {
    this.location = location;
    this.closeable = closeable;
    this.serializer = new LoggingEventSerializer();

    Schema schema = serializer.getAvroSchema();
    try {
      this.outputStream = location.getOutputStream(filePermissions);
      this.dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema));
      this.dataFileWriter.create(schema, outputStream);
      this.dataFileWriter.setSyncInterval(syncIntervalBytes);
      this.createTime = createTime;
      this.fileSize = 0;
    } catch (IOException e) {
      Closeables.closeQuietly(outputStream);
      Closeables.closeQuietly(dataFileWriter);
      throw e;
    }
  }

  Location getLocation() {
    return location;
  }

  void append(ILoggingEvent event) throws IOException {
    // If the event is already a LoggingEvent, we don't need to re-encode.
    if (event instanceof LoggingEvent) {
      ByteBuffer encoded = ((LoggingEvent) event).getEncoded();
      if (encoded != null) {
        // TODO: CDAP-8735 Remove the copying after upgraded to Avro 1.7.4+
        dataFileWriter.appendEncoded(ByteBuffers.copy(encoded));
      } else {
        dataFileWriter.append(((LoggingEvent) event).getRecord());
      }
      return;
    }
    dataFileWriter.append(serializer.toGenericRecord(event));
  }

  /**
   * get create time of the file
   * @return create time
   */
  long getCreateTime() {
    return createTime;
  }

  /**
   * get the number of bytes written to output stream
   * @return file size
   */
  long getSize() {
    return fileSize;
  }

  @Override
  public void flush() throws IOException {
    fileSize = dataFileWriter.sync();
  }

  @Override
  public void sync() throws IOException {
    flush();
    if (outputStream instanceof org.apache.hadoop.fs.Syncable) {
      ((org.apache.hadoop.fs.Syncable) outputStream).hsync();
    } else {
      outputStream.flush();
    }
  }

  @Override
  public void close() throws IOException {
    LOG.trace("Closing file {}", location);
    try {
      dataFileWriter.close();
    } finally {
      closeable.close();
    }
  }
}
