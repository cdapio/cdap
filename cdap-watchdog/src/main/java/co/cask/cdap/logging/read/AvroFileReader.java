/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.io.SeekableInputStream;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.serialize.LoggingEvent;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.impersonation.Impersonator;
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
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

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

  private final class LogEventIterator implements CloseableIterator<LogEvent> {

    private final Location file;

    private final Filter logFilter;
    private final long fromTimeMs;
    private final long toTimeMs;
    private final long maxEvents;

    private DataFileReader<GenericRecord> dataFileReader;

    private ILoggingEvent loggingEvent;
    private GenericRecord datum;

    private int count = 0;
    private long prevTimestamp = -1;

    private LogEvent next;

    LogEventIterator(Location file, Filter logFilter, long fromTimeMs, long toTimeMs, long maxEvents,
                     NamespaceId namespaceId, Impersonator impersonator) {
      this.file = file;

      this.logFilter = logFilter;
      this.fromTimeMs = fromTimeMs;
      this.toTimeMs = toTimeMs;
      this.maxEvents = maxEvents;


      try {
        dataFileReader = createReader(file, namespaceId, impersonator);
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
        }

        // populate the first element
        computeNext();

      } catch (Exception e) {
        // we want to ignore invalid or missing log files
        LOG.error("Got exception while reading log file {}", file, e);
      }
    }

    // will compute the next LogEvent and set the field 'next', unless its already set
    private void computeNext() {
      try {
        // read events from file
        while (next == null && dataFileReader.hasNext()) {
          loggingEvent = LoggingEvent.decode(dataFileReader.next(datum));
          if (loggingEvent.getTimeStamp() >= fromTimeMs && logFilter.match(loggingEvent)) {
            ++count;
            if ((count > maxEvents || loggingEvent.getTimeStamp() >= toTimeMs)
              && loggingEvent.getTimeStamp() != prevTimestamp) {
              break;
            }
            next = new LogEvent(loggingEvent,
                                new LogOffset(LogOffset.INVALID_KAFKA_OFFSET, loggingEvent.getTimeStamp()));
          }
          prevTimestamp = loggingEvent.getTimeStamp();
        }
      } catch (Exception e) {
        // We want to ignore invalid or missing log files.
        // If the 'next' variable wasn't set by this method call, then the 'hasNext' method
        // will return false, and no more events will be read from this file.
        LOG.error("Got exception while reading log file {}", file, e);
      }
    }

    @Override
    public void close() {
      try {
        dataFileReader.close();
      } catch (IOException e) {
        LOG.error("Got exception while closing log file {}", file, e);
      }
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public LogEvent next() {
      if (this.next == null) {
        throw new NoSuchElementException();
      }
      LogEvent toReturn = this.next;
      this.next = null;
      computeNext();
      return toReturn;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove not supported");
    }
  }

  public CloseableIterator<LogEvent> readLog(Location file, Filter logFilter, long fromTimeMs, long toTimeMs,
                                             int maxEvents, NamespaceId namespaceId, Impersonator impersonator) {
    return new LogEventIterator(file, logFilter, fromTimeMs, toTimeMs, maxEvents, namespaceId, impersonator);
  }


  public void readLog(Location file, Filter logFilter, long fromTimeMs, long toTimeMs,
                      int maxEvents, Callback callback, NamespaceId namespaceId, Impersonator impersonator)
    throws IOException {
    try (CloseableIterator<LogEvent> logEventIter =
           readLog(file, logFilter, fromTimeMs, toTimeMs, maxEvents, namespaceId, impersonator)) {
      while (logEventIter.hasNext()) {
        callback.handle(logEventIter.next());
      }
    }
  }

  @SuppressWarnings("WeakerAccess")
  public Collection<LogEvent> readLogPrev(Location file, Filter logFilter, long fromTimeMs, final int maxEvents,
                                          NamespaceId namespaceId, Impersonator impersonator) throws IOException {
    DataFileReader<GenericRecord> dataFileReader = createReader(file, namespaceId, impersonator);

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

      // For open file, endPosition sync marker is unknown so start from file length and read up to the actual EOF
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
  }

  /**
   *  Read current block in Avro file from current block sync marker to next block sync marker
   */
  private List<LogEvent> readToEndSyncPosition(DataFileReader<GenericRecord> dataFileReader, Filter logFilter,
                                               long fromTimeMs, long endSyncPosition) throws IOException {

    List<LogEvent> logSegment = new ArrayList<>();
    GenericRecord datum = null;
    long currentSyncPosition = dataFileReader.previousSync();
    // Read up to the end if endSyncPosition is not known (in case of an open file)
    // or read until endSyncPosition has been reached
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

  private DataFileReader<GenericRecord> createReader(Location location, NamespaceId namespaceId,
                                                     Impersonator impersonator) throws IOException {
    return new DataFileReader<>(new LocationSeekableInput(location, namespaceId, impersonator),
                                new GenericDatumReader<GenericRecord>(schema));
  }

  /**
   * An implementation of Avro SeekableInput over Location.
   */
  private static final class LocationSeekableInput implements SeekableInput {

    private final SeekableInputStream is;
    private final long len;

    LocationSeekableInput(final Location location,
                          NamespaceId namespaceId, Impersonator impersonator) throws IOException {
      try {
        this.is = impersonator.doAs(namespaceId, new Callable<SeekableInputStream>() {
          @Override
          public SeekableInputStream call() throws Exception {
            return Locations.newInputSupplier(location).getInput();
          }
        });
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        // should not happen
        throw Throwables.propagate(e);
      }

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
