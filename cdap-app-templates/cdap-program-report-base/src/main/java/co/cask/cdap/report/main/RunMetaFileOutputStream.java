/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report.main;

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

/**
 * Represents output stream for the run meta avro file.
 */

class RunMetaFileOutputStream implements Closeable, Flushable {
  private static final Logger LOG = LoggerFactory.getLogger(RunMetaFileOutputStream.class);

  private final Location location;
  private final long createTime;
  private final Closeable closeable;

  private OutputStream outputStream;
  private DataFileWriter<GenericRecord> dataFileWriter;
  private long fileSize;

  RunMetaFileOutputStream(Location location, String filePermissions,
                          int syncIntervalBytes, long createTime, Closeable closeable) throws IOException {
    this.location = location;
    this.closeable = closeable;
    Schema schema = ProgramRunInfoSerializer.SCHEMA;
    try {
      this.outputStream =
        filePermissions.isEmpty() ? location.getOutputStream() : location.getOutputStream(filePermissions);
      this.dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema));
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

  void append(ProgramRunInfo programRunInfo) throws IOException {
    dataFileWriter.append(ProgramRunInfoSerializer.createRecord(programRunInfo));
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
