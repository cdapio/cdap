/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.tail;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.logback.serialize.LogSchema;
import com.continuuity.common.logging.logback.serialize.LoggingEvent;
import com.continuuity.logging.LoggingContextHelper;
import com.continuuity.logging.filter.Filter;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tails log events from a file.
 */
public class FileLogTail implements LogTail {
  private static final Logger LOG = LoggerFactory.getLogger(FileLogTail.class);

  private final Configuration hConf;
  private final Path logBaseDir;

  private final Schema schema;

  @Inject
  public FileLogTail(@Assisted CConfiguration cConf, @Assisted Configuration hConf) {
    this.hConf = hConf;
    String baseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);
    Preconditions.checkNotNull(baseDir, "Log base dir cannot be null");
    this.logBaseDir = new Path(baseDir);

    try {
      schema = new LogSchema().getAvroSchema();
    } catch (IOException e) {
      LOG.error("Cannot get LogSchema", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Result getLogNext(LoggingContext loggingContext, String positionHintString, int maxEvents) {
    Filter logFilter = LoggingContextHelper.createFilter(loggingContext);
    PositionHint positionHint = new PositionHint(positionHintString);
    try {
      TreeMap<Long, FileStatus> sortedFiles = getTailFiles(null);
      if (sortedFiles.isEmpty()) {
        return new Result(ImmutableList.<ILoggingEvent>of(), positionHintString, positionHint.isValid());
      }

      if (!positionHint.isValid()) {
        return getLogPrev(loggingContext, positionHintString, maxEvents);
      }

      long fromTimeMs = positionHint.getNextTimestamp();
      long prevInterval = -1;
      Path prevPath = null;
      List<Path> tailFiles = Lists.newArrayListWithExpectedSize(sortedFiles.size());
      for (Map.Entry<Long, FileStatus> entry : sortedFiles.entrySet()){
        if (entry.getKey() >= fromTimeMs) {
          tailFiles.add(prevPath);
        }
        prevInterval = entry.getKey();
        prevPath = entry.getValue().getPath();
      }

      if (prevInterval != -1) {
        tailFiles.add(prevPath);
      }

      List<ILoggingEvent> loggingEvents = Lists.newLinkedList();
      for (Path file : tailFiles) {
        Collection<ILoggingEvent> events = readLogNext(file, logFilter, fromTimeMs, maxEvents - loggingEvents.size());
        loggingEvents.addAll(events);
        if (events.size() >= maxEvents) {
          break;
        }
      }
      if (loggingEvents.isEmpty()) {
        return new Result(ImmutableList.<ILoggingEvent>of(), positionHintString, positionHint.isValid());
      } else {
        return new Result(loggingEvents,
                          PositionHint.genPositionHint(loggingEvents.get(0).getTimeStamp() - 1,
                            loggingEvents.get(loggingEvents.size() - 1).getTimeStamp() + 1), positionHint.isValid());
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private Collection<ILoggingEvent> readLogNext(Path file, Filter logFilter, long fromTimeMs, int maxEvents) {
    int count = 0;
    FSDataInputStream inputStream = null;
    DataFileReader<GenericRecord> dataFileReader = null;
    List<ILoggingEvent> loggingEvents = Lists.newArrayListWithExpectedSize(maxEvents);
    try {
      FileContext fileContext = FileContext.getFileContext(hConf);
      FileStatus fileStatus = fileContext.getFileStatus(file);
      inputStream = fileContext.open(file);
      dataFileReader = new DataFileReader<GenericRecord>(new AvroFSInput(inputStream, fileStatus.getLen()),
                                                         new GenericDatumReader<GenericRecord>(schema));

      ILoggingEvent loggingEvent;
      GenericRecord datum;
      if (dataFileReader.hasNext()) {
        datum = dataFileReader.next();
        loggingEvent = LoggingEvent.decode(datum);
        long prevPrevSyncPos = 0;
        long prevSyncPos;
        // Seek to time fromTimeMs
        while (loggingEvent.getTimeStamp() < fromTimeMs && dataFileReader.hasNext()) {
          // Seek to the next sync point
          long curPos = dataFileReader.tell();
          prevSyncPos = dataFileReader.previousSync();
          prevPrevSyncPos = prevSyncPos;
          dataFileReader.sync(curPos);
          if (dataFileReader.hasNext()) {
            loggingEvent = LoggingEvent.decode(dataFileReader.next(datum));
          }
        }

        // We're now likely past the record with fromTimeMs, rewind to the previous sync point
        dataFileReader.sync(prevPrevSyncPos);

        // Start reading events from file
        long prevTimestamp = -1;
        while (dataFileReader.hasNext()) {
          loggingEvent = LoggingEvent.decode(dataFileReader.next(datum));
          if (loggingEvent.getTimeStamp() >= fromTimeMs && logFilter.match(loggingEvent)) {
            ++count;
            if (count > maxEvents && loggingEvent.getTimeStamp() != prevTimestamp) {
              break;
            }
            loggingEvents.add(loggingEvent);
          }
          prevTimestamp = loggingEvent.getTimeStamp();
        }
      }
      return loggingEvents;
    } catch (Exception e) {
      LOG.error(String.format("Got exception while reading log file %s", file.toUri()), e);
      throw Throwables.propagate(e);
    } finally {
      try {
        if (dataFileReader != null) {
          dataFileReader.close();
        }
        if (inputStream != null) {
          inputStream.close();
        }
      } catch (IOException e) {
        LOG.error(String.format("Got exception while closing log file %s", file.toUri()), e);
      }
    }
  }

  @Override
  public Result getLogPrev(LoggingContext loggingContext, String positionHintString, int maxEvents) {
    Filter logFilter = LoggingContextHelper.createFilter(loggingContext);
    PositionHint positionHint = new PositionHint(positionHintString);
    try {
      TreeMap<Long, FileStatus> sortedFiles = getTailFiles(Collections.<Long>reverseOrder());
      if (sortedFiles.isEmpty()) {
        return new Result(ImmutableList.<ILoggingEvent>of(), positionHintString, positionHint.isValid());
      }

      long fromTimeMs = positionHint.isValid() ? positionHint.getPrevTimestamp() :
        sortedFiles.firstEntry().getValue().getModificationTime();

      List<Path> tailFiles = Lists.newArrayListWithExpectedSize(sortedFiles.size());
      for (Map.Entry<Long, FileStatus> entry : sortedFiles.entrySet()){
        if (entry.getKey() <= fromTimeMs) {
          tailFiles.add(entry.getValue().getPath());
        }
      }

      List<ILoggingEvent> loggingEvents = Lists.newLinkedList();
      for (Path file : tailFiles) {
        Collection<ILoggingEvent> events = readLogPrev(file, logFilter, fromTimeMs, maxEvents - loggingEvents.size());
        loggingEvents.addAll(0, events);
        if (events.size() >= maxEvents) {
          break;
        }
      }
      if (loggingEvents.isEmpty()) {
        return new Result(ImmutableList.<ILoggingEvent>of(), positionHintString, positionHint.isValid());
      } else {
        return new Result(loggingEvents,
                          PositionHint.genPositionHint(loggingEvents.get(0).getTimeStamp() - 1,
                            loggingEvents.get(loggingEvents.size() - 1).getTimeStamp() + 1), positionHint.isValid());
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private Collection<ILoggingEvent> readLogPrev(Path file, Filter logFilter, long fromTimeMs, final int maxEvents) {
    FSDataInputStream inputStream = null;
    DataFileReader<GenericRecord> dataFileReader = null;

    LinkedHashMap<Long, ILoggingEvent> evictingQueue =
      new LinkedHashMap<Long, ILoggingEvent>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, ILoggingEvent> event) {
          if (size() > maxEvents) {
            long timestamp = event.getKey();
            for (Iterator<Map.Entry<Long, ILoggingEvent>> eit = entrySet().iterator(); eit.hasNext();) {
              Map.Entry<Long, ILoggingEvent> entry = eit.next();
              if (entry.getKey() == timestamp) {
                eit.remove();
              } else {
                break;
              }
            }
          }
          return false;
        }
      };

    try {
      FileContext fileContext = FileContext.getFileContext(hConf);
      FileStatus fileStatus = fileContext.getFileStatus(file);
      inputStream = fileContext.open(file);
      dataFileReader = new DataFileReader<GenericRecord>(new AvroFSInput(inputStream, fileStatus.getLen()),
                                                         new GenericDatumReader<GenericRecord>(schema));

      GenericRecord datum = new GenericData.Record(schema);
      long id = 0;
      while (dataFileReader.hasNext()) {
        ILoggingEvent loggingEvent = LoggingEvent.decode(dataFileReader.next(datum));
        if (loggingEvent.getTimeStamp() <= fromTimeMs && logFilter.match(loggingEvent)) {
          evictingQueue.put(id++, loggingEvent);
        }
      }
      return evictingQueue.values();
    } catch (Exception e) {
      LOG.error(String.format("Got exception while reading log file %s", file.toUri()), e);
      throw Throwables.propagate(e);
    } finally {
      try {
        if (dataFileReader != null) {
          dataFileReader.close();
        }
        if (inputStream != null) {
          inputStream.close();
        }
      } catch (IOException e) {
        LOG.error(String.format("Got exception while closing log file %s", file.toUri()), e);
      }
    }
  }

  private TreeMap<Long, FileStatus> getTailFiles(Comparator<Long> comparator) throws IOException {
    TreeMap<Long, FileStatus> sortedFiles = Maps.newTreeMap(comparator);
    FileContext fileContext = FileContext.getFileContext(hConf);
    RemoteIterator<FileStatus> filesIt = fileContext.listStatus(logBaseDir);

    while (filesIt.hasNext()) {
      FileStatus status = filesIt.next();
      Path path = status.getPath();
      try {
        long interval = extractInterval(path.getName());
        sortedFiles.put(interval, status);
      } catch (NumberFormatException e) {
        LOG.warn(String.format("Not able to parse interval from log file name %s", path.toUri()));
      }
    }

    return sortedFiles;
  }

  private static long extractInterval(String fileName) {
    // File name format is timestamp.avro
    if (fileName == null || fileName.isEmpty()) {
      return -1;
    }
    int endIndex = fileName.indexOf('.');
    return Long.parseLong(fileName.substring(0, endIndex));
  }


  static class PositionHint {
    private static final String POS_HINT_PREFIX = "FILE";
    private static final int POS_HINT_NUM_FILEDS = 3;

    private long prevTimestamp = -1;
    private long nextTimestamp = -1;

    private static final Splitter SPLITTER = Splitter.on(':').limit(POS_HINT_NUM_FILEDS);

    public PositionHint(String positionHint) {
      if (positionHint == null || positionHint.isEmpty()) {
        return;
      }

      List<String> splits = ImmutableList.copyOf(SPLITTER.split(positionHint));
      if (splits.size() != POS_HINT_NUM_FILEDS || !splits.get(0).equals(POS_HINT_PREFIX)) {
        return;
      }

      try {
        prevTimestamp = Long.parseLong(splits.get(1));
        nextTimestamp = Long.parseLong(splits.get(2));
      } catch (NumberFormatException e) {
        // Cannot parse position hint
        prevTimestamp = -1;
        nextTimestamp = -1;
      }
    }

    public long getPrevTimestamp() {
      return prevTimestamp;
    }

    public long getNextTimestamp() {
      return nextTimestamp;
    }

    public boolean isValid() {
      return prevTimestamp > -1 && nextTimestamp > -1;
    }

    public static String genPositionHint(long prevTimestamp, long nextTimestamp) {
      if (prevTimestamp > -1) {
        return String.format("%s:%d:%d", POS_HINT_PREFIX, prevTimestamp, nextTimestamp);
      }
      return "";
    }
  }
}
