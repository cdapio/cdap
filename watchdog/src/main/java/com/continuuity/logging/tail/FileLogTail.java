/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.tail;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingConfiguration;
import com.continuuity.common.logging.logback.serialize.LogSchema;
import com.continuuity.common.logging.logback.serialize.LoggingEvent;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.filter.LogFilterGenerator;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Tails log events from a file.
 */
public class FileLogTail implements LogTail, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FileLogTail.class);

  private final Configuration hConf;
  private final Path logBaseDir;

  private final Schema schema;
  private final FileContext fileContext;

  @Inject
  public FileLogTail(@Assisted CConfiguration cConf, @Assisted Configuration hConf) {
    this.hConf = hConf;
    String baseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);
    Preconditions.checkNotNull(baseDir, "Log base dir cannot be null");
    this.logBaseDir = new Path(baseDir);

    try {
      schema = new LogSchema().getAvroSchema();
      fileContext = FileContext.getFileContext(hConf);
    } catch (IOException e) {
      LOG.error("Cannot get LogSchema", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void tailFlowLog(String accountId, String applicationId, String flowId, long fromTimeMs, int maxEvents,
                          Callback callback) {
    Filter logFilter = LogFilterGenerator.createTailFilter(accountId, applicationId, flowId);

    try {
      int count = 0;
      for (Path path : getTailFiles(fromTimeMs)) {
        count += tailLog(path, logFilter, fromTimeMs, maxEvents, callback);
        if (count >= maxEvents) {
          return;
        }
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() throws IOException {
  }

  private Iterable<Path> getTailFiles(long fromTimeMs) throws IOException {
    Map<Long, Path> sortedFiles = Maps.newTreeMap();
    RemoteIterator<FileStatus> filesIt = fileContext.listStatus(logBaseDir);

    while (filesIt.hasNext()) {
      FileStatus status = filesIt.next();
      Path path = status.getPath();
      try {
        long interval = extractInterval(path);
        sortedFiles.put(interval, path);
      } catch (NumberFormatException e) {
        LOG.warn(String.format("Not able to parse interval from log file name %s", path.toUri()));
      }
    }

    long prevInterval = -1;
    Path prevPath = null;
    List<Path> tailFiles = Lists.newArrayListWithExpectedSize(sortedFiles.size());
    for (Map.Entry<Long, Path> entry : sortedFiles.entrySet()){
      if (entry.getKey() >= fromTimeMs && prevInterval != -1) {
        tailFiles.add(prevPath);
      }
      prevInterval = entry.getKey();
      prevPath = entry.getValue();
    }
    if (prevInterval != -1) {
      tailFiles.add(prevPath);
    }
    return tailFiles;
  }

  private static long extractInterval(Path path) {
    // File name format is timestamp.avro
    String fileName = path.getName();
    int endIndex = fileName.indexOf('.');
    return Long.parseLong(fileName.substring(0, endIndex));
  }

  private int tailLog(Path file, Filter logFilter, long fromTimeMs, int maxEvents, Callback callback) {
    int count = 0;
    FSDataInputStream inputStream = null;
    DataFileReader<GenericRecord> dataFileReader = null;
    try {
      FileContext fileContext = FileContext.getFileContext(hConf);
      // Check if file exists
      FileStatus fileStatus = fileContext.getFileStatus(file);
      inputStream = fileContext.open(file);
      dataFileReader = new DataFileReader<GenericRecord>(new AvroFSInput(inputStream, fileStatus.getLen()),
                                                         new GenericDatumReader<GenericRecord>(schema));

      ILoggingEvent loggingEvent;
      GenericRecord datum;
      if (dataFileReader.hasNext()) {
        datum = dataFileReader.next();
        loggingEvent = LoggingEvent.decode(datum);
        long prevSyncPos = -1;
        // Seek to time fromTimeMs
        while (loggingEvent.getTimeStamp() < fromTimeMs && dataFileReader.hasNext()) {
          // Seek to the next sync point
          long curPos = dataFileReader.tell();
          dataFileReader.sync(curPos);
          if (dataFileReader.pastSync(curPos)) {
            prevSyncPos = dataFileReader.tell();
          }
          if (dataFileReader.hasNext()) {
            loggingEvent = LoggingEvent.decode(dataFileReader.next(datum));
          }
        }

        // We're now likely past the record with fromTimeMs, rewind to the previous sync point
        if (prevSyncPos != -1) {
          dataFileReader.seek(prevSyncPos);
        }

        // Start reading events from file
        while (dataFileReader.hasNext()) {
          loggingEvent = LoggingEvent.decode(dataFileReader.next(datum));
          if (loggingEvent.getTimeStamp() >= fromTimeMs && logFilter.match(loggingEvent)) {
            ++count;
            callback.handle(loggingEvent);
            if (count >= maxEvents) {
              break;
            }
          }
        }
      }
      return count;
    } catch (Exception e) {
      LOG.error("Got exception while tailing log", e);
      return count;
    } finally {
      try {
        if (dataFileReader != null) {
          dataFileReader.close();
        }
        if (inputStream != null) {
          inputStream.close();
        }
      } catch (IOException e) {
        LOG.error("Got exception while closing", e);
      }
    }
  }
}
