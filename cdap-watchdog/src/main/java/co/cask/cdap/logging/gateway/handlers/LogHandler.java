/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.logging.gateway.handlers;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.filter.FilterParser;
import co.cask.cdap.logging.gateway.handlers.store.ProgramStore;
import co.cask.cdap.logging.read.Callback;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.cdap.logging.read.LogOffset;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.logging.read.ReadRange;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * v3 {@link HttpHandler} to handle /logs requests
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class LogHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LogHandler.class);

  private final LogReader logReader;
  private final ProgramStore programStore;
  private final String logPattern;

  @Inject
  public LogHandler(LogReader logReader, CConfiguration cConfig, ProgramStore programStore) {
    this.logReader = logReader;
    this.programStore = programStore;
    this.logPattern = cConfig.get(LoggingConfiguration.LOG_PATTERN, LoggingConfiguration.DEFAULT_LOG_PATTERN);
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/logs")
  public void getLogs(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                      @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                      @PathParam("program-id") String programId,
                      @QueryParam("start") @DefaultValue("-1") long fromTimeSecsParam,
                      @QueryParam("stop") @DefaultValue("-1") long toTimeSecsParam,
                      @QueryParam("escape") @DefaultValue("true") boolean escape,
                      @QueryParam("filter") @DefaultValue("") String filterStr) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContext(namespaceId, appId, programId,
                                             ProgramType.valueOfCategoryName(programType));
    doGetLogs(responder, loggingContext, fromTimeSecsParam, toTimeSecsParam, escape, filterStr, null, "text",
              ImmutableList.<String>of());
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/runs/{run-id}/logs")
  public void getRunIdLogs(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                           @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                           @PathParam("program-id") String programId, @PathParam("run-id") String runId,
                           @QueryParam("start") @DefaultValue("-1") long fromTimeSecsParam,
                           @QueryParam("stop") @DefaultValue("-1") long toTimeSecsParam,
                           @QueryParam("escape") @DefaultValue("true") boolean escape,
                           @QueryParam("filter") @DefaultValue("") String filterStr,
                           @QueryParam("format") @DefaultValue("text") String format,
                           @QueryParam("suppress") List<String> suppress) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    RunRecordMeta runRecord = programStore.getRun(new ProgramId(namespaceId, appId, type, programId), runId);
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(namespaceId, appId, programId, type,
                                                                                    runId, runRecord.getSystemArgs());

    doGetLogs(responder, loggingContext, fromTimeSecsParam, toTimeSecsParam, escape, filterStr, runRecord, format,
              suppress);
  }

  private void doGetLogs(HttpResponder responder, LoggingContext loggingContext,
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
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/logs/next")
  public void next(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                   @PathParam("program-id") String programId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                   @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                   @QueryParam("escape") @DefaultValue("true") boolean escape,
                   @QueryParam("filter") @DefaultValue("") String filterStr) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContext(namespaceId, appId,
                                             programId, ProgramType.valueOfCategoryName(programType));
    doNext(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, null, "text",
           ImmutableList.<String>of());
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/runs/{run-id}/logs/next")
  public void runIdNext(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                        @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                        @PathParam("program-id") String programId, @PathParam("run-id") String runId,
                        @QueryParam("max") @DefaultValue("50") int maxEvents,
                        @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                        @QueryParam("escape") @DefaultValue("true") boolean escape,
                        @QueryParam("filter") @DefaultValue("") String filterStr,
                        @QueryParam("format") @DefaultValue("text") String format,
                        @QueryParam("suppress") List<String> suppress) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    RunRecordMeta runRecord = programStore.getRun(new ProgramId(namespaceId, appId, type, programId), runId);
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(namespaceId, appId, programId, type,
                                                                                    runId, runRecord.getSystemArgs());

    doNext(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, runRecord, format, suppress);
  }

  private void doNext(HttpResponder responder, LoggingContext loggingContext, int maxEvents,
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

    if (runRecord.getStopTs() != null) {
      // Add a buffer to stop time due to CDAP-3100
      long runStopMillis = TimeUnit.SECONDS.toMillis(runRecord.getStopTs() + 1);
      if (toTimeMillis > runStopMillis) {
        toTimeMillis = runStopMillis;
      }
    }

    ReadRange adjusted = new ReadRange(fromTimeMillis, toTimeMillis, readRange.getKafkaOffset());
    LOG.trace("Original read range: {}. Adjusted read range: {}", readRange, adjusted);
    return adjusted;
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/logs/prev")
  public void prev(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                   @PathParam("program-id") String programId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                   @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                   @QueryParam("escape") @DefaultValue("true") boolean escape,
                   @QueryParam("filter") @DefaultValue("") String filterStr) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContext(namespaceId, appId, programId,
                                             ProgramType.valueOfCategoryName(programType));
    doPrev(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, null, "text",
           ImmutableList.<String>of());
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/runs/{run-id}/logs/prev")
  public void runIdPrev(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                        @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                        @PathParam("program-id") String programId, @PathParam("run-id") String runId,
                        @QueryParam("max") @DefaultValue("50") int maxEvents,
                        @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                        @QueryParam("escape") @DefaultValue("true") boolean escape,
                        @QueryParam("filter") @DefaultValue("") String filterStr,
                        @QueryParam("format") @DefaultValue("text") String format,
                        @QueryParam("suppress") List<String> suppress) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    RunRecordMeta runRecord = programStore.getRun(new ProgramId(namespaceId, appId, type, programId), runId);
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(namespaceId, appId, programId, type,
                                                                                    runId, runRecord.getSystemArgs());

    doPrev(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, runRecord, format, suppress);
  }

  private void doPrev(HttpResponder responder, LoggingContext loggingContext, int maxEvents, String fromOffsetStr,
                      boolean escape, String filterStr, @Nullable RunRecordMeta runRecord, String format,
                      List<String> fieldsToSuppress) {
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

  @GET
  @Path("/system/{component-id}/{service-id}/logs")
  public void sysList(HttpRequest request, HttpResponder responder, @PathParam("component-id") String componentId,
                      @PathParam("service-id") String serviceId,
                      @QueryParam("start") @DefaultValue("-1") long fromTimeSecsParam,
                      @QueryParam("stop") @DefaultValue("-1") long toTimeSecsParam,
                      @QueryParam("escape") @DefaultValue("true") boolean escape,
                      @QueryParam("filter") @DefaultValue("") String filterStr,
                      @QueryParam("format") @DefaultValue("text") String format,
                      @QueryParam("suppress") List<String> suppress) {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(Id.Namespace.SYSTEM.getId(), componentId,
                                                                           serviceId);
    doGetLogs(responder, loggingContext, fromTimeSecsParam, toTimeSecsParam, escape, filterStr, null, format, suppress);
  }

  @GET
  @Path("/system/{component-id}/{service-id}/logs/next")
  public void sysNext(HttpRequest request, HttpResponder responder, @PathParam("component-id") String componentId,
                      @PathParam("service-id") String serviceId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                      @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                      @QueryParam("escape") @DefaultValue("true") boolean escape,
                      @QueryParam("filter") @DefaultValue("") String filterStr,
                      @QueryParam("format") @DefaultValue("text") String format,
                      @QueryParam("suppress") List<String> suppress) {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(Id.Namespace.SYSTEM.getId(), componentId,
                                                                           serviceId);
    doNext(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, null, format, suppress);
  }

  @GET
  @Path("/system/{component-id}/{service-id}/logs/prev")
  public void sysPrev(HttpRequest request, HttpResponder responder, @PathParam("component-id") String componentId,
                      @PathParam("service-id") String serviceId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                      @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                      @QueryParam("escape") @DefaultValue("true") boolean escape,
                      @QueryParam("filter") @DefaultValue("") String filterStr,
                      @QueryParam("format") @DefaultValue("text") String format,
                      @QueryParam("suppress") List<String> suppress) {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(Id.Namespace.SYSTEM.getId(), componentId,
                                                                           serviceId);
    doPrev(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, null, format, suppress);
  }

  private static final class TimeRange {
    private final long fromMillis;
    private final long toMillis;

    private TimeRange(long fromMillis, long toMillis) {
      this.fromMillis = fromMillis;
      this.toMillis = toMillis;
    }

    public long getFromMillis() {
      return fromMillis;
    }

    public long getToMillis() {
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

  private Callback getNextOrPrevLogsCallback(String format, HttpResponder responder, List<String> suppress,
                                             boolean escape) {
    LogFormatType formatType = getFormatType(format);
    switch (formatType) {
      case JSON:
        return new LogDataOffsetCallback(responder, suppress);
      default:
        return new TextOffsetCallback(responder, logPattern, escape);
    }
  }

  private static LogFormatType getFormatType(String format) {
    return LogFormatType.valueOf(format.toUpperCase());
  }
}
