I/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.filter.FilterParser;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * v3 {@link HttpHandler} for logging
 */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class LogHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LogHandler.class);

  private final LogReader logReader;
  private final String logPattern;

  @Inject
  public LogHandler(Authenticator authenticator, LogReader logReader, CConfiguration cConfig) {
    super(authenticator);
    this.logReader = logReader;
    this.logPattern = cConfig.get(LoggingConfiguration.LOG_PATTERN, LoggingConfiguration.DEFAULT_LOG_PATTERN);
  }

  @GET
  @Path("/apps/{app-id}/{program-type}/{program-id}/logs")
  public void list(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                   @PathParam("program-id") String programId) {
    try {
      // Parse start, stop, filter and escape
      LogRequestArguments logArgs = decodeLogArgs(request);
      long fromTimeMs = logArgs.getFromTimeMs();
      long toTimeMs = logArgs.getToTimeMs();
      boolean escape = logArgs.getEscape();
      Filter filter = FilterParser.parse(logArgs.getFilter());

      if (fromTimeMs < 0 || toTimeMs < 0 || toTimeMs <= fromTimeMs) {
        responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
        return;
      }

      LoggingContext loggingContext =
        LoggingContextHelper.getLoggingContext(namespaceId, appId, programId,
                                               getProgramType(ProgramType.valueOfCategoryName(programType)));
      ChunkedLogReaderCallback logCallback = new ChunkedLogReaderCallback(responder, logPattern, escape);
      logReader.getLog(loggingContext, fromTimeMs, toTimeMs, filter, logCallback);
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
  @Path("/apps/{app-id}/{program-type}/{program-id}/logs/next")
  public void next(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                   @PathParam("program-id") String programId) {

    try {
      //Parse filter, max, offset and escape
      LogRequestArguments logArgs = decodeLogArgs(request);
      int maxEvents = logArgs.getMaxEvents();
      long fromOffset = logArgs.getFromOffset();
      boolean escape = logArgs.getEscape();
      Filter filter = FilterParser.parse(logArgs.getFilter());

      LoggingContext loggingContext =
        LoggingContextHelper.getLoggingContext(namespaceId, appId,
                                               programId, getProgramType(ProgramType.valueOfCategoryName(programType)));
      LogReaderCallback logCallback = new LogReaderCallback(responder, logPattern, escape);

      logReader.getLogNext(loggingContext, fromOffset, maxEvents, filter, logCallback);
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
  @Path("/apps/{app-id}/{program-type}/{program-id}/logs/prev")
  public void prev(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                   @PathParam("program-id") String programId) {
    try {
      //Parse filter, max, offset and escape
      LogRequestArguments logArgs = decodeLogArgs(request);
      int maxEvents = logArgs.getMaxEvents();
      long fromOffset = logArgs.getFromOffset();
      boolean escape = logArgs.getEscape();
      Filter filter = FilterParser.parse(logArgs.getFilter());

      LoggingContext loggingContext =
        LoggingContextHelper.getLoggingContext(namespaceId, appId, programId,
                                               getProgramType(ProgramType.valueOfCategoryName(programType)));
      LogReaderCallback logCallback = new LogReaderCallback(responder, logPattern, escape);
      logReader.getLogPrev(loggingContext, fromOffset, maxEvents, filter, logCallback);
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
   * protected until we move over the /system APIs to v3
   */
  protected class LogRequestArguments {
    private long fromTimeMs;
    private long toTimeMs;
    private String filter;
    private int maxEvents;
    private long fromOffset;
    private boolean escape;

    LogRequestArguments(long fromTimeMs, long toTimeMs, String filter, int maxEvents, long fromOffset, boolean escape) {
      this.fromTimeMs = fromTimeMs;
      this.toTimeMs = toTimeMs;
      this.filter = filter;
      this.maxEvents = maxEvents;
      this.fromOffset = fromOffset;
      this.escape = escape;
    }

    public long getFromTimeMs() {
      return fromTimeMs;
    }

    public long getToTimeMs() {
      return toTimeMs;
    }

    public String getFilter() {
      return filter;
    }

    public int getMaxEvents() {
      return maxEvents;
    }

    public long getFromOffset() {
      return fromOffset;
    }

    public boolean getEscape() {
      return escape;
    }
  }

  /**
   * protected until we support v2 APIs
   */
  protected LogRequestArguments decodeLogArgs(HttpRequest request) {
    Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();
    long fromTimeMs = parseTimestamp(queryParams.get("start"));
    long toTimeMs = parseTimestamp(queryParams.get("stop"));

    String filterStr = "";
    if (queryParams.get("filter") != null && !queryParams.get("filter").isEmpty()) {
      filterStr = queryParams.get("filter").get(0);
    }

    int maxEvents = queryParams.get("max") != null && !queryParams.get("max").isEmpty() ?
      Integer.parseInt(queryParams.get("max").get(0)) : 50;

    long fromOffset = queryParams.get("fromOffset") != null && !queryParams.get("fromOffset").isEmpty() ?
      Long.parseLong(queryParams.get("fromOffset").get(0)) : -1;

    boolean escape = queryParams.get("escape") == null || queryParams.get("escape").isEmpty() ||
      Boolean.parseBoolean(queryParams.get("escape").get(0));

    return new LogRequestArguments(fromTimeMs, toTimeMs, filterStr, maxEvents, fromOffset, escape);
  }

  private static long parseTimestamp(List<String> parameter) {
    if (parameter == null || parameter.isEmpty()) {
      return -1;
    }
    try {
      return TimeUnit.MILLISECONDS.convert(Long.parseLong(parameter.get(0)), TimeUnit.SECONDS);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private ProgramType getProgramType(ProgramType programType) {
    Preconditions.checkNotNull(programType, "ProgramType cannot be null");

    switch (programType) {
      case FLOW:
        return ProgramType.FLOW;
      case PROCEDURE:
        return ProgramType.PROCEDURE;
      case MAPREDUCE:
        return ProgramType.MAPREDUCE;
      case SPARK:
        return ProgramType.SPARK;
      case SERVICE:
        return ProgramType.SERVICE;
      default:
        throw new IllegalArgumentException(String.format("Illegal program type %s", programType));
    }
  }
}
