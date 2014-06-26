package com.continuuity.logging.gateway.handlers;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.AuthenticatedHttpHandler;
import com.continuuity.http.HandlerContext;
import com.continuuity.http.HttpResponder;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.context.LoggingContextHelper;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.filter.FilterParser;
import com.continuuity.logging.read.LogReader;
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
 * Handler to serve log requests.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class LogHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LogHandler.class);

  private final LogReader logReader;
  private final String logPattern;

  private enum EntityType {
    flows, procedures, mapreduce, services
  }

  @Inject
  public LogHandler(Authenticator authenticator, LogReader logReader, CConfiguration cConfig) {
    super(authenticator);
    this.logReader = logReader;
    this.logPattern = cConfig.get(LoggingConfiguration.LOG_PATTERN, LoggingConfiguration.DEFAULT_LOG_PATTERN);
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting LogHandler.");
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping LogHandler.");
  }

  @GET
  @Path("/system/{component-id}/{service-id}/logs")
  public void sysList(HttpRequest request, HttpResponder responder,
                      @PathParam("component-id") String componentId, @PathParam("service-id") String serviceId) {
    try {
      LogRequestArguments logArgs = decodeLogArgs(request);
      long fromTimeMs = logArgs.getFromTimeMs();
      long toTimeMs = logArgs.getToTimeMs();
      boolean escape = logArgs.getEscape();
      Filter filter = FilterParser.parse(logArgs.getFilter());

      if (fromTimeMs < 0 || toTimeMs < 0 || toTimeMs <= fromTimeMs) {
        responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
        return;
      }

      LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(Constants.Logging.SYSTEM_NAME, componentId,
                                                                             serviceId);
      ChunkedLogReaderCallback logCallback = new ChunkedLogReaderCallback(responder, logPattern, escape);
      logReader.getLog(loggingContext, fromTimeMs, toTimeMs, filter, logCallback);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/apps/{app-id}/{entity-type}/{entity-id}/logs")
  public void list(HttpRequest request, HttpResponder responder,
                   @PathParam("app-id") String appId, @PathParam("entity-type") String entityType,
                   @PathParam("entity-id") String entityId) {
    try {
      String accountId = getAuthenticatedAccountId(request);

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
        LoggingContextHelper.getLoggingContext(accountId, appId,
                                               entityId, getEntityType(EntityType.valueOf(entityType)));
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
  @Path("/system/{component-id}/{service-id}/logs/next")
  public void sysNext(HttpRequest request, HttpResponder responder,
                      @PathParam("component-id") String componentId, @PathParam("service-id") String serviceId) {
    try {
      LogRequestArguments logArgs = decodeLogArgs(request);
      int maxEvents = logArgs.getMaxEvents();
      long fromOffset = logArgs.getFromOffset();
      boolean escape = logArgs.getEscape();
      Filter filter = FilterParser.parse(logArgs.getFilter());

      LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(Constants.Logging.SYSTEM_NAME, componentId,
                                                                             serviceId);
      LogReaderCallback logCallback = new LogReaderCallback(responder, logPattern, escape);
      logReader.getLogNext(loggingContext, fromOffset, maxEvents, filter, logCallback);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/apps/{app-id}/{entity-type}/{entity-id}/logs/next")
  public void next(HttpRequest request, HttpResponder responder,
                   @PathParam("app-id") String appId, @PathParam("entity-type") String entityType,
                   @PathParam("entity-id") String entityId) {

    try {
      String accountId = getAuthenticatedAccountId(request);

      //Parse filter, max, offset and escape
      LogRequestArguments logArgs = decodeLogArgs(request);
      int maxEvents = logArgs.getMaxEvents();
      long fromOffset = logArgs.getFromOffset();
      boolean escape = logArgs.getEscape();
      Filter filter = FilterParser.parse(logArgs.getFilter());

      LoggingContext loggingContext =
        LoggingContextHelper.getLoggingContext(accountId, appId,
                                               entityId, getEntityType(EntityType.valueOf(entityType)));
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
  @Path("/system/{component-id}/{service-id}/logs/prev")
  public void sysPrev(HttpRequest request, HttpResponder responder,
                      @PathParam("component-id") String componentId, @PathParam("service-id") String serviceId) {
    try {
      LogRequestArguments logArgs = decodeLogArgs(request);
      int maxEvents = logArgs.getMaxEvents();
      long fromOffset = logArgs.getFromOffset();
      boolean escape = logArgs.getEscape();
      Filter filter = FilterParser.parse(logArgs.getFilter());

      LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(Constants.Logging.SYSTEM_NAME, componentId,
                                                                             serviceId);
      LogReaderCallback logCallback = new LogReaderCallback(responder, logPattern, escape);
      logReader.getLogPrev(loggingContext, fromOffset, maxEvents, filter, logCallback);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/apps/{app-id}/{entity-type}/{entity-id}/logs/prev")
  public void prev(HttpRequest request, HttpResponder responder,
                   @PathParam("app-id") String appId, @PathParam("entity-type") String entityType,
                   @PathParam("entity-id") String entityId) {
    try {
      String accountId = getAuthenticatedAccountId(request);

      //Parse filter, max, offset and escape
      LogRequestArguments logArgs = decodeLogArgs(request);
      int maxEvents = logArgs.getMaxEvents();
      long fromOffset = logArgs.getFromOffset();
      boolean escape = logArgs.getEscape();
      Filter filter = FilterParser.parse(logArgs.getFilter());

      LoggingContext loggingContext =
        LoggingContextHelper.getLoggingContext(accountId, appId,
                                               entityId, getEntityType(EntityType.valueOf(entityType)));
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

  private class LogRequestArguments {
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

  private LogRequestArguments decodeLogArgs(HttpRequest request) {
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

  private LoggingContextHelper.EntityType getEntityType(EntityType entityType) {
    if (entityType == null) {
      throw new IllegalArgumentException("Null program type");
    }

    switch (entityType) {
      case flows:
        return LoggingContextHelper.EntityType.FLOW;
      case procedures:
        return LoggingContextHelper.EntityType.PROCEDURE;
      case mapreduce:
        return LoggingContextHelper.EntityType.MAP_REDUCE;
      case services:
        return LoggingContextHelper.EntityType.SERVICE;
      default:
        throw new IllegalArgumentException(String.format("Illegal program type %s", entityType));
    }
  }
}
