/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.appender.file;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.logging.serialize.LoggingEvent;
import com.continuuity.weave.filesystem.Location;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Writes a logging event to file with rotation. File name will be the current timestamp. This class is not
 * thread-safe.
 */
public class LogFileWriter implements Closeable {
  private final Location logBaseDir;
  private final Schema schema;
  private final int syncIntervalBytes;
  private final long fileRotateIntervalMs;
  private final long retentionDurationMs;

  private OutputStream outputStream;
  private DataFileWriter<GenericRecord> dataFileWriter;
  private long currentFileTs = -1;

  private static final String FILE_SUFFIX = "avro";

  public LogFileWriter(Location logBaseDir, Schema schema, int syncIntervalBytes,
                       long fileRotateIntervalMs, long retentionDurationMs) {
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
    outputStream.flush();
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

  /**
   * Rotates the current log file file when current log file is open for more than fileRotateIntervalMs.
   * @param ts Timestamp to use for creating the new log file.
   * @throws IOException
   */
  void rotate(long ts) throws IOException {
    if ((currentFileTs != ts && ts - currentFileTs > fileRotateIntervalMs) || dataFileWriter == null) {
      close();
      create(ts);
      currentFileTs = ts;

      cleanUp();
    }
  }

  private void create(long timeInterval) throws IOException {
    Location file = logBaseDir.append(String.format("%d.%s", timeInterval, FILE_SUFFIX));
    this.outputStream = file.getOutputStream();
    this.dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema));
    this.dataFileWriter.create(schema, this.outputStream);
    this.dataFileWriter.setSyncInterval(syncIntervalBytes);
  }

  private void cleanUp() throws IOException {
    long retentionTs = System.currentTimeMillis() - retentionDurationMs;
    File baseDir = new File(logBaseDir.toURI());
    File [] files = baseDir.listFiles();
    if (files == null || files.length == 0) {
      return;
    }

    for (File f : files) {
      String fileName = f.getName();
      String currentFilePrefix = String.valueOf(currentFileTs);
      if (f.lastModified() < retentionTs && fileName.endsWith(FILE_SUFFIX)
        && !fileName.startsWith(currentFilePrefix)) {
        //noinspection ResultOfMethodCallIgnored
        f.delete();
      }
    }
  }
}

