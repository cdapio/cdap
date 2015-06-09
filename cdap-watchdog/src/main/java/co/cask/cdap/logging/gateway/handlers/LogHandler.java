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

package co.cask.cdap.logging.gateway.handlers;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.filter.FilterParser;
import co.cask.cdap.logging.gateway.handlers.store.ProgramStore;
import co.cask.cdap.logging.read.LogOffset;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.logging.read.ReadRange;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                      @QueryParam("filter") @DefaultValue("") String filterStr,
                      @QueryParam("adapterid") String adapterId) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContext(namespaceId, appId, programId,
                                             ProgramType.valueOfCategoryName(programType), adapterId);
    doGetLogs(responder, loggingContext, fromTimeSecsParam, toTimeSecsParam, escape, filterStr, null);
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
                           @QueryParam("adapterid") String adapterId) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContextWithRunId(namespaceId, appId, programId,
                                                      ProgramType.valueOfCategoryName(programType),
                                                      runId, adapterId);
    RunRecord runRecord = programStore.getRun(
      Id.Program.from(namespaceId, appId, ProgramType.valueOfCategoryName(programType), programId), runId);
    doGetLogs(responder, loggingContext, fromTimeSecsParam, toTimeSecsParam, escape, filterStr, runRecord);
  }

  private void doGetLogs(HttpResponder responder, LoggingContext loggingContext,
                         long fromTimeSecsParam, long toTimeSecsParam, boolean escape, String filterStr,
                         @Nullable RunRecord runRecord) {
    try {
      TimeRange timeRange = parseTime(fromTimeSecsParam, toTimeSecsParam, responder);
      if (timeRange == null) {
        return;
      }

      Filter filter = FilterParser.parse(filterStr);

      ReadRange readRange = new ReadRange(timeRange.getFromMillis(), timeRange.getToMillis(),
                                          LogOffset.INVALID_KAFKA_OFFSET);
      readRange = adjustReadRange(readRange, runRecord);

      ChunkedLogReaderCallback logCallback = new ChunkedLogReaderCallback(responder, logPattern, escape);
      logReader.getLog(loggingContext, readRange.getFromMillis(), readRange.getToMillis(), filter, logCallback);
      logCallback.close();
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/logs/next")
  public void next(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                   @PathParam("program-id") String programId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                   @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                   @QueryParam("escape") @DefaultValue("true") boolean escape,
                   @QueryParam("filter") @DefaultValue("") String filterStr,
                   @QueryParam("adapterid") String adapterId) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContext(namespaceId, appId,
                                             programId, ProgramType.valueOfCategoryName(programType),
                                             adapterId);
    doNext(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, null);
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
                        @QueryParam("adapterid") String adapterId) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContextWithRunId(namespaceId, appId, programId,
                                                      ProgramType.valueOfCategoryName(programType),
                                                      runId, adapterId);
    RunRecord runRecord = programStore.getRun(
      Id.Program.from(namespaceId, appId, ProgramType.valueOfCategoryName(programType), programId), runId);
    doNext(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, runRecord);
  }

  private void doNext(HttpResponder responder, LoggingContext loggingContext, int maxEvents,
                      String fromOffsetStr, boolean escape, String filterStr, @Nullable RunRecord runRecord) {
    try {
      Filter filter = FilterParser.parse(filterStr);

      LogReaderCallback logCallback = new LogReaderCallback(responder, logPattern, escape);

      LogOffset logOffset = FormattedLogEvent.parseLogOffset(fromOffsetStr);
      ReadRange readRange = ReadRange.createFromRange(logOffset);
      readRange = adjustReadRange(readRange, runRecord);
      logReader.getLogNext(loggingContext, readRange, maxEvents, filter, logCallback);
      logCallback.close();
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * If readRange is outside runRecord's range, then the readRange is adjusted to fall within runRecords range.
   */
  private ReadRange adjustReadRange(ReadRange readRange, @Nullable RunRecord runRecord) {
    if (runRecord == null) {
      return readRange;
    }

    long fromTimeMillis = readRange.getFromMillis();
    long toTimeMillis = readRange.getToMillis();

    long runStartMillis = TimeUnit.SECONDS.toMillis(runRecord.getStartTs());
    if (fromTimeMillis < runStartMillis) {
      fromTimeMillis = runStartMillis;
    }
    if (runRecord.getStopTs() != null) {
      long runStopMillis = TimeUnit.SECONDS.toMillis(runRecord.getStopTs());
      if (toTimeMillis > runStopMillis) {
        toTimeMillis = runStopMillis;
      }
    }
    return new ReadRange(fromTimeMillis, toTimeMillis, readRange.getKafkaOffset());
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/logs/prev")
  public void prev(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                   @PathParam("program-id") String programId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                   @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                   @QueryParam("escape") @DefaultValue("true") boolean escape,
                   @QueryParam("filter") @DefaultValue("") String filterStr,
                   @QueryParam("adapterid") String adapterId) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContext(namespaceId, appId, programId,
                                             ProgramType.valueOfCategoryName(programType), adapterId);
    doPrev(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, null);
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
                        @QueryParam("adapterid") String adapterId) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContextWithRunId(namespaceId, appId, programId,
                                                      ProgramType.valueOfCategoryName(programType),
                                                      runId, adapterId);
    RunRecord runRecord = programStore.getRun(
      Id.Program.from(namespaceId, appId, ProgramType.valueOfCategoryName(programType), programId), runId);
    doPrev(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, runRecord);
  }

  private void doPrev(HttpResponder responder, LoggingContext loggingContext, int maxEvents, String fromOffsetStr,
                      boolean escape, String filterStr, @Nullable RunRecord runRecord) {
    try {
      Filter filter = FilterParser.parse(filterStr);

      LogReaderCallback logCallback = new LogReaderCallback(responder, logPattern, escape);
      LogOffset logOffset = FormattedLogEvent.parseLogOffset(fromOffsetStr);
      ReadRange readRange = ReadRange.createToRange(logOffset);
      readRange = adjustReadRange(readRange, runRecord);
      logReader.getLogPrev(loggingContext, readRange,
                           maxEvents, filter, logCallback);
      logCallback.close();
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/system/{component-id}/{service-id}/logs")
  public void sysList(HttpRequest request, HttpResponder responder, @PathParam("component-id") String componentId,
                      @PathParam("service-id") String serviceId,
                      @QueryParam("start") @DefaultValue("-1") long fromTimeSecsParam,
                      @QueryParam("stop") @DefaultValue("-1") long toTimeSecsParam,
                      @QueryParam("escape") @DefaultValue("true") boolean escape,
                      @QueryParam("filter") @DefaultValue("") String filterStr) {
    try {
      TimeRange timeRange = parseTime(fromTimeSecsParam, toTimeSecsParam, responder);
      if (timeRange == null) {
        return;
      }

      Filter filter = FilterParser.parse(filterStr);
      LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(Constants.SYSTEM_NAMESPACE, componentId,
                                                                             serviceId);
      ChunkedLogReaderCallback logCallback = new ChunkedLogReaderCallback(responder, logPattern, escape);
      logReader.getLog(loggingContext, timeRange.getFromMillis(), timeRange.getToMillis(), filter, logCallback);
      logCallback.close();
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/system/{component-id}/{service-id}/logs/next")
  public void sysNext(HttpRequest request, HttpResponder responder, @PathParam("component-id") String componentId,
                      @PathParam("service-id") String serviceId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                      @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                      @QueryParam("escape") @DefaultValue("true") boolean escape,
                      @QueryParam("filter") @DefaultValue("") String filterStr) {
    try {
      Filter filter = FilterParser.parse(filterStr);

      LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(Constants.SYSTEM_NAMESPACE, componentId,
                                                                             serviceId);
      LogReaderCallback logCallback = new LogReaderCallback(responder, logPattern, escape);
      LogOffset logOffset = FormattedLogEvent.parseLogOffset(fromOffsetStr);
      ReadRange readRange = ReadRange.createFromRange(logOffset);
      logReader.getLogNext(loggingContext, readRange,
                           maxEvents, filter, logCallback);
      logCallback.close();
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/system/{component-id}/{service-id}/logs/prev")
  public void sysPrev(HttpRequest request, HttpResponder responder, @PathParam("component-id") String componentId,
                      @PathParam("service-id") String serviceId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                      @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                      @QueryParam("escape") @DefaultValue("true") boolean escape,
                      @QueryParam("filter") @DefaultValue("") String filterStr) {
    try {
      Filter filter = FilterParser.parse(filterStr);

      LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(Constants.SYSTEM_NAMESPACE, componentId,
                                                                             serviceId);
      LogReaderCallback logCallback = new LogReaderCallback(responder, logPattern, escape);
      LogOffset logOffset = FormattedLogEvent.parseLogOffset(fromOffsetStr);
      ReadRange readRange = ReadRange.createToRange(logOffset);
      logReader.getLogPrev(loggingContext, readRange,
                           maxEvents, filter, logCallback);
      logCallback.close();
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private static final class TimeRange {
    private final long fromMillis;
    private final long toMillis;

    public TimeRange(long fromMillis, long toMillis) {
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
    long fromMillis = fromTimeSecsParam < 0 ?
      System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1) : TimeUnit.SECONDS.toMillis(fromTimeSecsParam);
    long toMillis = toTimeSecsParam < 0 ? System.currentTimeMillis() : TimeUnit.SECONDS.toMillis(toTimeSecsParam);

    if (toMillis <= fromMillis) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid time range. " +
        "'stop' should be greater than 'start'.");
      return null;
    }

    return new TimeRange(fromMillis, toMillis);
  }
}
