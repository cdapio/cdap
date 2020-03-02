/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.gateway.handlers;

import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.internal.app.store.RunRecordMeta;
import io.cdap.cdap.logging.LoggingConfiguration;
import io.cdap.cdap.logging.filter.Filter;
import io.cdap.cdap.logging.filter.FilterParser;
import io.cdap.cdap.logging.read.Callback;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.logging.read.LogOffset;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.logging.read.ReadRange;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Abstract Class that contains commonly used methods for log Http Requests.
 */
public abstract class AbstractLogHttpHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractLogHttpHandler.class);

  private final String logPattern;

  protected AbstractLogHttpHandler(CConfiguration cConfig) {
    this.logPattern = cConfig.get(LoggingConfiguration.LOG_PATTERN, LoggingConfiguration.DEFAULT_LOG_PATTERN);
  }

  protected void doGetLogs(LogReader logReader, HttpResponder responder, LoggingContext loggingContext,
                           long fromTimeSecsParam, long toTimeSecsParam, boolean escape, String filterStr,
                           @Nullable RunRecordMeta runRecord, String format, List<String> fieldsToSuppress) {

    try {
      TimeRange timeRange = parseTime(fromTimeSecsParam, toTimeSecsParam, responder);
      if (timeRange == null) {
        return;
      }

      Filter filter = FilterParser.parse(filterStr);

      ReadRange readRange = new ReadRange(timeRange.getFromMillis(), timeRange.getToMillis(),
                                          LogOffset.INVALID_KAFKA_OFFSET);
      readRange = adjustReadRange(readRange, runRecord, fromTimeSecsParam != -1);
      try {
        // the iterator is closed by the BodyProducer passed to the HttpResponder
        CloseableIterator<LogEvent> logIter = logReader.getLog(loggingContext, readRange.getFromMillis(),
                                                               readRange.getToMillis(), filter);
        AbstractChunkedLogProducer logsProducer = getFullLogsProducer(format, logIter, fieldsToSuppress, escape);
        responder.sendContent(HttpResponseStatus.OK, logsProducer, logsProducer.getResponseHeaders());
      } catch (Exception ex) {
        LOG.debug("Exception while reading logs for logging context {}", loggingContext, ex);
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }
  }

  protected void doPrev(LogReader logReader, HttpResponder responder, LoggingContext loggingContext,
                        int maxEvents, String fromOffsetStr, boolean escape, String filterStr,
                        @Nullable RunRecordMeta runRecord, String format, List<String> fieldsToSuppress) {
    try {
      Filter filter = FilterParser.parse(filterStr);

      Callback logCallback = getNextOrPrevLogsCallback(format, responder, fieldsToSuppress, escape);
      LogOffset logOffset = FormattedTextLogEvent.parseLogOffset(fromOffsetStr);
      ReadRange readRange = ReadRange.createToRange(logOffset);
      readRange = adjustReadRange(readRange, runRecord, true);
      try {
        logReader.getLogPrev(loggingContext, readRange, maxEvents, filter, logCallback);
      } catch (Exception ex) {
        LOG.debug("Exception while reading logs for logging context {}", loggingContext, ex);
      } finally {
        logCallback.close();
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }
  }

  protected void doNext(LogReader logReader, HttpResponder responder, LoggingContext loggingContext, int maxEvents,
                        String fromOffsetStr, boolean escape, String filterStr, @Nullable RunRecordMeta runRecord,
                        String format, List<String> fieldsToSuppress) {
    try {
      Filter filter = FilterParser.parse(filterStr);
      Callback logCallback = getNextOrPrevLogsCallback(format, responder, fieldsToSuppress, escape);
      LogOffset logOffset = FormattedTextLogEvent.parseLogOffset(fromOffsetStr);
      ReadRange readRange = ReadRange.createFromRange(logOffset);
      readRange = adjustReadRange(readRange, runRecord, true);
      try {
        logReader.getLogNext(loggingContext, readRange, maxEvents, filter, logCallback);
      } catch (Exception ex) {
        LOG.debug("Exception while reading logs for logging context {}", loggingContext, ex);
      } finally {
        logCallback.close();
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }
  }

  private Callback getNextOrPrevLogsCallback(String format, HttpResponder responder,
                                             List<String> suppress, boolean escape) {
    LogFormatType formatType = getFormatType(format);
    switch (formatType) {
      case JSON:
        return new LogDataOffsetCallback(responder, suppress);
      default:
        return new TextOffsetCallback(responder, logPattern, escape);
    }
  }

  private static final class TimeRange {
    private final long fromMillis;
    private final long toMillis;

    private TimeRange(long fromMillis, long toMillis) {
      this.fromMillis = fromMillis;
      this.toMillis = toMillis;
    }

    long getFromMillis() {
      return fromMillis;
    }

    long getToMillis() {
      return toMillis;
    }
  }

  private static TimeRange parseTime(long fromTimeSecsParam, long toTimeSecsParam, HttpResponder responder) {
    long currentTimeMillis = System.currentTimeMillis();
    long fromMillis = fromTimeSecsParam < 0 ?
      currentTimeMillis - TimeUnit.HOURS.toMillis(1) : TimeUnit.SECONDS.toMillis(fromTimeSecsParam);
    long toMillis = toTimeSecsParam < 0 ? currentTimeMillis : TimeUnit.SECONDS.toMillis(toTimeSecsParam);

    if (toMillis <= fromMillis) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid time range. " +
        "'stop' should be greater than 'start'.");
      return null;
    }

    return new TimeRange(fromMillis, toMillis);
  }

  private enum LogFormatType {
    TEXT,
    JSON
  }

  private AbstractChunkedLogProducer getFullLogsProducer(String format, CloseableIterator<LogEvent> logEventIter,
                                                         List<String> suppress, boolean escape) {
    LogFormatType formatType = getFormatType(format);
    switch (formatType) {
      case JSON:
        return new LogDataOffsetProducer(logEventIter, suppress);
      default:
        return new TextChunkedLogProducer(logEventIter, logPattern, escape);
    }
  }

  /**
   * If readRange is outside runRecord's range, then the readRange is adjusted to fall within runRecords range.
   */
  private ReadRange adjustReadRange(ReadRange readRange, @Nullable RunRecordMeta runRecord,
                                    boolean fromTimeSpecified) {
    if (runRecord == null) {
      return readRange;
    }

    long fromTimeMillis = readRange.getFromMillis();
    long toTimeMillis = readRange.getToMillis();

    long runStartMillis = TimeUnit.SECONDS.toMillis(runRecord.getStartTs());

    if (!fromTimeSpecified) {
      // If from time is not specified explicitly, use the run records start time as from time
      fromTimeMillis = runStartMillis;
    }


    if (fromTimeMillis < runStartMillis) {
      // If from time is specified but is smaller than run records start time, reset it to
      // run record start time. This is to optimize so that we do not look into extra files.
      fromTimeMillis = runStartMillis;
    }

    if (runRecord.getCluster().getEnd() != null) {
      // Add a buffer to stop time due to CDAP-3100
      long runStopMillis = TimeUnit.SECONDS.toMillis(runRecord.getCluster().getEnd() + 1);
      if (toTimeMillis > runStopMillis) {
        toTimeMillis = runStopMillis;
      }
    }

    ReadRange adjusted = new ReadRange(fromTimeMillis, toTimeMillis, readRange.getKafkaOffset());
    LOG.trace("Original read range: {}. Adjusted read range: {}", readRange, adjusted);
    return adjusted;
  }

  private static LogFormatType getFormatType(String format) {
    return LogFormatType.valueOf(format.toUpperCase());
  }
}
