package com.continuuity.logging.read;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.serialize.LoggingEvent;
import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Throwables;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reads log events from an Avro file.
 */
public class AvroFileLogReader {
  private static final Logger LOG = LoggerFactory.getLogger(AvroFileLogReader.class);

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

  public Collection<ILoggingEvent> readLogPrev(Location file, Filter logFilter, long fromTimeMs, final int maxEvents) {
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
      inputStream = new FSDataInputStream(file.getInputStream());
      dataFileReader = new DataFileReader<GenericRecord>(new AvroFSInput(inputStream, file.length()),
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
