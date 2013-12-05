package com.continuuity.logging.read;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.serialize.LoggingEvent;
import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Reads log events from an Avro file.
 */
public class AvroFileLogReader {
  private static final Logger LOG = LoggerFactory.getLogger(AvroFileLogReader.class);
  private static final long DEFAULT_SKIP_LEN = 50 * 1024;

  private final Schema schema;

  public AvroFileLogReader(Schema schema) {
    this.schema = schema;
  }

  public void readLog(Location file, Filter logFilter, long fromTimeMs, long toTimeMs,
                      int maxEvents, Callback callback) {
    FSDataInputStream inputStream = null;
    DataFileReader<GenericRecord> dataFileReader = null;
    try {
      inputStream = new FSDataInputStream(file.getInputStream());
      dataFileReader = new DataFileReader<GenericRecord>(new AvroFSInput(inputStream, file.length()),
                                                         new GenericDatumReader<GenericRecord>(schema));

      ILoggingEvent loggingEvent;
      GenericRecord datum;
      if (dataFileReader.hasNext()) {
        datum = dataFileReader.next();
        loggingEvent = LoggingEvent.decode(datum);
        long prevPrevSyncPos = 0;
        long prevSyncPos = 0;
        // Seek to time fromTimeMs
        while (loggingEvent.getTimeStamp() < fromTimeMs && dataFileReader.hasNext()) {
          // Seek to the next sync point
          long curPos = dataFileReader.tell();
          prevPrevSyncPos = prevSyncPos;
          prevSyncPos = dataFileReader.previousSync();
          dataFileReader.sync(curPos);
          if (dataFileReader.hasNext()) {
            loggingEvent = LoggingEvent.decode(dataFileReader.next(datum));
          }
        }

        // We're now likely past the record with fromTimeMs, rewind to the previous sync point
        dataFileReader.sync(prevPrevSyncPos);

        // Start reading events from file
        int count = 0;
        long prevTimestamp = -1;
        while (dataFileReader.hasNext()) {
          loggingEvent = LoggingEvent.decode(dataFileReader.next(datum));
          if (loggingEvent.getTimeStamp() >= fromTimeMs && logFilter.match(loggingEvent)) {
            ++count;
            if ((count > maxEvents || loggingEvent.getTimeStamp() >= toTimeMs)
              && loggingEvent.getTimeStamp() != prevTimestamp) {
              break;
            }
            callback.handle(new LogEvent(loggingEvent, loggingEvent.getTimeStamp()));
          }
          prevTimestamp = loggingEvent.getTimeStamp();
        }
      }
    } catch (Exception e) {
      LOG.error(String.format("Got exception while reading log file %s", file.toURI()), e);
      throw Throwables.propagate(e);
    } finally {
      try {
        try {
          if (dataFileReader != null) {
            dataFileReader.close();
          }
        } finally {
          if (inputStream != null) {
            inputStream.close();
          }
        }
      } catch (IOException e) {
        LOG.error(String.format("Got exception while closing log file %s", file.toURI()), e);
      }
    }
  }

  public Collection<LogEvent> readLogPrev(Location file, Filter logFilter, long fromTimeMs, final int maxEvents) {
    FSDataInputStream inputStream = null;
    DataFileReader<GenericRecord> dataFileReader = null;

    try {
      inputStream = new FSDataInputStream(file.getInputStream());
      dataFileReader = new DataFileReader<GenericRecord>(new AvroFSInput(inputStream, file.length()),
                                                         new GenericDatumReader<GenericRecord>(schema));

      if (!dataFileReader.hasNext()) {
        return ImmutableList.of();
      }

      GenericRecord datum;
      List<List<LogEvent>> logSegments = Lists.newArrayList();
      int count = 0;

      // Calculate skipLen based on fileLength
      long skipLen = file.length() / 10;
      if (skipLen > DEFAULT_SKIP_LEN) {
        skipLen = DEFAULT_SKIP_LEN;
      } else if (skipLen <= 0) {
        skipLen = DEFAULT_SKIP_LEN;
      }

      List<LogEvent> logSegment = Lists.newArrayList();
      long boundaryTimeMs = Long.MAX_VALUE;

      long seekPos = file.length();
      while (seekPos > 0) {
        seekPos = seekPos < skipLen ? 0 : seekPos - skipLen;
        dataFileReader.sync(seekPos);

        logSegment = logSegment.isEmpty() ? logSegment : Lists.<LogEvent>newArrayList();
        long segmentStartTimeMs = Long.MAX_VALUE;
        while (dataFileReader.hasNext()) {
          datum = dataFileReader.next();

          ILoggingEvent loggingEvent = LoggingEvent.decode(datum);

          if (segmentStartTimeMs == Long.MAX_VALUE) {
            segmentStartTimeMs = loggingEvent.getTimeStamp();
          }

          // Stop when reached fromTimeMs, or at the end of current segment.
          if (loggingEvent.getTimeStamp() > fromTimeMs || loggingEvent.getTimeStamp() >= boundaryTimeMs) {
            break;
          }

          if (logFilter.match(loggingEvent)) {
            ++count;
            logSegment.add(new LogEvent(loggingEvent, loggingEvent.getTimeStamp()));
          }
        }

        boundaryTimeMs = segmentStartTimeMs;

        if (!logSegment.isEmpty()) {
          logSegments.add(logSegment);
        }

        if (count > maxEvents) {
          break;
        }
      }

      int skip = count >= maxEvents ? count - maxEvents : 0;
      return Lists.newArrayList(Iterables.skip(Iterables.concat(Lists.reverse(logSegments)), skip));
    } catch (Exception e) {
      LOG.error(String.format("Got exception while reading log file %s", file.toURI()), e);
      throw Throwables.propagate(e);
    } finally {
      try {
        try {
          if (dataFileReader != null) {
            dataFileReader.close();
          }
        } finally {
          if (inputStream != null) {
            inputStream.close();
          }
        }
      } catch (IOException e) {
        LOG.error(String.format("Got exception while closing log file %s", file.toURI()), e);
      }
    }
  }
}
