/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.appender.file;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.logging.serialize.LoggingEvent;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

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
  private final long retentionDurationMs;

  private FSDataOutputStream outputStream;
  private DataFileWriter<GenericRecord> dataFileWriter;
  private long currentTimeInterval = -1;

  private static final String FILE_SUFFIX = "avro";

  public LogFileWriter(FileSystem fileSystem, Path logBaseDir, Schema schema, int syncIntervalBytes,
                       long fileRotateIntervalMs, long retentionDurationMs) {
    this.fileSystem = fileSystem;
    this.logBaseDir = logBaseDir;
    this.schema = schema;
    this.syncIntervalBytes = syncIntervalBytes;
    this.fileRotateIntervalMs = fileRotateIntervalMs;
    this.retentionDurationMs = retentionDurationMs;
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
    try {
      if (dataFileWriter != null) {
        dataFileWriter.close();
        dataFileWriter = null;
      }
    } finally {
      if (outputStream != null) {
        outputStream.close();
        outputStream = null;
      }
    }
  }

  private void rotate(long ts) throws IOException {
    long timeInterval = getMinuteInterval(ts);
    if ((currentTimeInterval != timeInterval && timeInterval % fileRotateIntervalMs == 0) || dataFileWriter == null) {
      close();
      create(timeInterval);
      currentTimeInterval = timeInterval;

      cleanUp();
    }
  }

  private void create(long timeInterval) throws IOException {
    Path file = new Path(logBaseDir, String.format("%d.%s", timeInterval, FILE_SUFFIX));
    this.outputStream = fileSystem.create(file, false);
    this.dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema));
    this.dataFileWriter.create(schema, this.outputStream);
    this.dataFileWriter.setSyncInterval(syncIntervalBytes);
  }

  private void cleanUp() throws IOException {
    long retentionTs = System.currentTimeMillis() - retentionDurationMs;
    RemoteIterator<LocatedFileStatus> filesIt = fileSystem.listFiles(logBaseDir, false);
    while (filesIt.hasNext()) {
      LocatedFileStatus status = filesIt.next();
      if (status.getModificationTime() < retentionTs && status.getPath().getName().endsWith(FILE_SUFFIX)) {
        fileSystem.delete(status.getPath(), false);
      }
    }
  }

  private static long getMinuteInterval(long ts) {
    long minutes =  TimeUnit.MINUTES.convert(ts, TimeUnit.MILLISECONDS);
    return TimeUnit.MILLISECONDS.convert(minutes, TimeUnit.MINUTES);
  }
}

