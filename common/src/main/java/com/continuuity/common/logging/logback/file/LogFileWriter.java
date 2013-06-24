/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.common.logging.logback.file;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.logging.logback.serialize.LoggingEvent;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Writes a logging event to file with rotation. File name will be the current timestamp. This class is not
 * thread-safe.
 */
public class LogFileWriter implements Closeable {
  private final FileSystem fileSystem;
  private final Path logBaseDir;
  private final Schema schema;
  private final int syncIntervalBytes;
  private final long fileRotateIntervalMs;

  private FSDataOutputStream outputStream;
  private DataFileWriter<GenericRecord> dataFileWriter;
  private long currentTimeInterval = -1;

  public LogFileWriter(FileSystem fileSystem, Path logBaseDir, Schema schema, int syncIntervalBytes,
                       long fileRotateIntervalMs) {
    this.fileSystem = fileSystem;
    this.logBaseDir = logBaseDir;
    this.schema = schema;
    this.syncIntervalBytes = syncIntervalBytes;
    this.fileRotateIntervalMs = fileRotateIntervalMs;
  }

  public void append(ILoggingEvent event) throws IOException {
    rotate(event.getTimeStamp());
    GenericRecord datum = LoggingEvent.encode(schema, event);
    dataFileWriter.append(datum);
    dataFileWriter.flush();
    outputStream.hflush();
  }

  @Override
  public void close() throws IOException {
    if (dataFileWriter != null) {
      dataFileWriter.close();
      dataFileWriter = null;
    }
    if (outputStream != null) {
      outputStream.close();
      outputStream = null;
    }
  }

  private void rotate(long ts) throws IOException {
    long timeInterval = getMinuteInterval(ts);
    if (currentTimeInterval != timeInterval && timeInterval % fileRotateIntervalMs == 0 || dataFileWriter == null) {
      close();
      create(timeInterval);
      currentTimeInterval = timeInterval;
    }
  }

  private void create(long timeInterval) throws IOException {
    Path file = new Path(logBaseDir, String.format("%d.avro", timeInterval));
    this.outputStream = fileSystem.create(file, false);
    this.dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema));
    this.dataFileWriter.create(schema, this.outputStream);
    this.dataFileWriter.setSyncInterval(syncIntervalBytes);
  }

  private static long getMinuteInterval(long ts) {
    long minutes =  TimeUnit.MINUTES.convert(ts, TimeUnit.MILLISECONDS);
    return TimeUnit.MILLISECONDS.convert(minutes, TimeUnit.MINUTES);
  }
}

