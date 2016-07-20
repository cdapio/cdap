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

package co.cask.cdap.logging.read;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.io.SeekableInputStream;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.serialize.LoggingEvent;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Reads log events from an Avro file.
 */
public class AvroFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(AvroFileReader.class);
  private static final long DEFAULT_SKIP_LEN = 10 * 1024 * 1024;

  private final Schema schema;

  public AvroFileReader(Schema schema) {
    this.schema = schema;
  }

  public void readLog(Location file, Filter logFilter, long fromTimeMs, long toTimeMs,
                      int maxEvents, Callback callback) {
    try {
      DataFileReader<GenericRecord> dataFileReader = createReader(file);
      try {
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
            LOG.trace("Syncing to pos {}", curPos);
            dataFileReader.sync(curPos);
            if (dataFileReader.hasNext()) {
              loggingEvent = LoggingEvent.decode(dataFileReader.next(datum));
            }
          }

          // We're now likely past the record with fromTimeMs, rewind to the previous sync point
          dataFileReader.sync(prevPrevSyncPos);
          LOG.trace("Final sync pos {}", prevPrevSyncPos);

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
              callback.handle(new LogEvent(loggingEvent,
                                           new LogOffset(LogOffset.INVALID_KAFKA_OFFSET, loggingEvent.getTimeStamp())));
            }
            prevTimestamp = loggingEvent.getTimeStamp();
          }
        }
      } finally {
        try {
          dataFileReader.close();
        } catch (IOException e) {
          LOG.error("Got exception while closing log file {}", file, e);
        }
      }
    } catch (Exception e) {
      LOG.error("Got exception while reading log file {}", file, e);
      throw Throwables.propagate(e);
    }
  }

  public Collection<LogEvent> readLogPrev(Location file, Filter logFilter, long fromTimeMs, final int maxEvents) {
    try {
      DataFileReader<GenericRecord> dataFileReader = createReader(file);

      try {
        if (!dataFileReader.hasNext()) {
          return ImmutableList.of();
        }

        List<List<LogEvent>> logSegments = Lists.newArrayList();
        List<LogEvent> logSegment;
        int count = 0;

        // Calculate skipLen based on fileLength
        long length = file.length();
        LOG.trace("Got file length {}", length);
        long skipLen = length / 10;
        if (skipLen > DEFAULT_SKIP_LEN || skipLen <= 0) {
          skipLen = DEFAULT_SKIP_LEN;
        }

        // For open file, endPosition sync marker is unknown so start from file length and read till the actual eof
        dataFileReader.sync(length);
        long finalSync = dataFileReader.previousSync();
        logSegment = readToEndSyncPosition(dataFileReader, logFilter, fromTimeMs, -1);

        if (!logSegment.isEmpty()) {
          logSegments.add(logSegment);
          count = count + logSegment.size();
        }

        LOG.trace("Read logevents {} from position {}", count, finalSync);

        long startPosition = finalSync;
        long endPosition = startPosition;
        long currentSync;

        while (startPosition > 0 && count < maxEvents) {
          // Skip to sync position less than current sync position
          startPosition = skipToPosition(dataFileReader, startPosition, endPosition, skipLen);
          currentSync = dataFileReader.previousSync();
          logSegment = readToEndSyncPosition(dataFileReader, logFilter, fromTimeMs, endPosition);

          if (!logSegment.isEmpty()) {
            logSegments.add(logSegment);
            count = count + logSegment.size();
          }
          LOG.trace("Read logevents {} from position {} to endPosition {}", count, currentSync, endPosition);

          endPosition = currentSync;
        }

        int skip = count >= maxEvents ? count - maxEvents : 0;
        return Lists.newArrayList(Iterables.skip(Iterables.concat(Lists.reverse(logSegments)), skip));
      } finally {
        try {
          dataFileReader.close();
        } catch (IOException e) {
          LOG.error("Got exception while closing log file {}", file, e);
        }
      }
    } catch (Exception e) {
      LOG.error("Got exception while reading log file {}", file, e);
      throw Throwables.propagate(e);
    }
  }

  /**
   *  Read current block in Avro file from current block sync marker to next block sync marker
   */
  private List<LogEvent> readToEndSyncPosition(DataFileReader<GenericRecord> dataFileReader, Filter logFilter,
                                               long fromTimeMs, long endSyncPosition) throws IOException {

    List<LogEvent> logSegment = new ArrayList<>();
    GenericRecord datum = null;
    long currentSyncPosition = dataFileReader.previousSync();
    // Read till the end if endSyncPosition is not known (in case of open file)
    // or read until endSyncPosition has reached
    while (dataFileReader.hasNext() && (endSyncPosition == -1 || (currentSyncPosition < endSyncPosition))) {
      datum = dataFileReader.next(datum);
      ILoggingEvent loggingEvent = LoggingEvent.decode(datum);

      // Stop when reached fromTimeMs
      if (loggingEvent.getTimeStamp() > fromTimeMs) {
        break;
      }

      if (logFilter.match(loggingEvent)) {
        logSegment.add(new LogEvent(loggingEvent,
                                    new LogOffset(LogOffset.INVALID_KAFKA_OFFSET, loggingEvent.getTimeStamp())));
      }
      currentSyncPosition = dataFileReader.previousSync();
    }

    return logSegment;
  }

  /**
   * Starting from currentSyncPosition, move backwards by skipLen number of positions in each iteration to
   * find out a sync position less than currentSyncPosition
   */
  private long skipToPosition(DataFileReader<GenericRecord> dataFileReader,
                             long startPosition, long endSyncPosition, long skipLen) throws IOException {
    long currentSync = endSyncPosition;
    while (startPosition > 0 && currentSync == endSyncPosition) {
      startPosition = startPosition < skipLen ? 0 : startPosition - skipLen;
      dataFileReader.sync(startPosition);
      currentSync = dataFileReader.previousSync();
      LOG.trace("Got position {} after skipping {} positions from currentSync {}", startPosition, skipLen, currentSync);
    }
    return startPosition;
  }

  private DataFileReader<GenericRecord> createReader(Location location) throws IOException {
    return new DataFileReader<>(new LocationSeekableInput(location),
                                new GenericDatumReader<GenericRecord>(schema));

  }

  /**
   * An implementation of Avro SeekableInput over Location.
   */
  private static final class LocationSeekableInput implements SeekableInput {

    private final SeekableInputStream is;
    private final long len;

    LocationSeekableInput(Location location) throws IOException {
      this.is = Locations.newInputSupplier(location).getInput();
      this.len = location.length();
    }

    @Override
    public void seek(long p) throws IOException {
      is.seek(p);
    }

    @Override
    public long tell() throws IOException {
      return is.getPos();
    }

    @Override
    public long length() throws IOException {
      return len;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return is.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
      is.close();
    }
  }
}
