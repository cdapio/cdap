package com.continuuity.gateway.v2.handlers.v2.log;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.v2.handlers.v2.AuthenticatedHttpHandler;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.context.LoggingContextHelper;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.filter.FilterParser;
import com.continuuity.logging.read.LogReader;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Handler to serve log requests.
 */
@Path("/v2")
public class LogHandler extends AuthenticatedHttpHandler {
  private final LogReader logReader;
  private final String logPattern;

  private enum EntityType {
    flow, procedure, mapreduce
  }

  @Inject
  public LogHandler(GatewayAuthenticator authenticator, LogReader logReader, CConfiguration cConfig) {
    super(authenticator);
    this.logReader = logReader;
    this.logPattern = cConfig.get(LoggingConfiguration.LOG_PATTERN, LoggingConfiguration.DEFAULT_LOG_PATTERN);
  }

  @GET
  @Path("/apps/{appId}/{entityType}/{entityId}/logs")
  public void list(HttpRequest request, HttpResponder responder,
                   @PathParam("appId") String appId, @PathParam("entityType") String entityType,
                   @PathParam("entityId") String entityId) {

    long fromTimeMs;
    long toTimeMs;
    Filter filter;
    LoggingContext loggingContext;
    ChunkedLogReaderCallback logCallback;
    try {
      String accountId = getAuthenticatedAccountId(request);

      // Parse fromTime, toTime and filter
      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();
      fromTimeMs = parseTimestamp(queryParams.get("fromTime"));
      toTimeMs = parseTimestamp(queryParams.get("toTime"));

      if (fromTimeMs < 0 || toTimeMs < 0 || toTimeMs <= fromTimeMs) {
        responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
        return;
      }

      String filterStr = "";
      if (queryParams.get("filter") != null && !queryParams.get("filter").isEmpty()) {
        filterStr = queryParams.get("filter").get(0);
      }
      filter = FilterParser.parse(filterStr);

      loggingContext =
        LoggingContextHelper.getLoggingContext(accountId, appId,
                                               entityId, getEntityType(EntityType.valueOf(entityType)));
      logCallback = new ChunkedLogReaderCallback(responder, logPattern);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
      return;
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }  catch (Throwable e) {
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      return;
    }

      logReader.getLog(loggingContext, fromTimeMs, toTimeMs, filter,
                       logCallback);
  }

  @GET
  @Path("/apps/{appId}/{entityType}/{entityId}/logs/next")
  public void next(HttpRequest request, HttpResponder responder,
                   @PathParam("appId") String appId, @PathParam("entityType") String entityType,
                   @PathParam("entityId") String entityId) {

    long fromOffset;
    int maxEvents;
    Filter filter;
    LoggingContext loggingContext;
    LogReaderCallback logCallback;
    try {
      String accountId = getAuthenticatedAccountId(request);

      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();
      String filterStr = "";
      if (queryParams.get("filter") != null && !queryParams.get("filter").isEmpty()) {
        filterStr = queryParams.get("filter").get(0);
      }
      filter = FilterParser.parse(filterStr);

      maxEvents = queryParams.get("max") != null && !queryParams.get("max").isEmpty() ?
        Integer.parseInt(queryParams.get("max").get(0)) : 50;

      fromOffset = queryParams.get("fromOffset") != null && !queryParams.get("fromOffset").isEmpty() ?
        Long.parseLong(queryParams.get("fromOffset").get(0)) : -1;

      loggingContext =
        LoggingContextHelper.getLoggingContext(accountId, appId,
                                               entityId, getEntityType(EntityType.valueOf(entityType)));
      logCallback = new LogReaderCallback(responder, logPattern);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
      return;
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    } catch (Throwable e) {
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      return;
    }

    logReader.getLogNext(loggingContext, fromOffset, maxEvents, filter, logCallback);
  }

  @GET
  @Path("/apps/{appId}/{entityType}/{entityId}/logs/prev")
  public void prev(HttpRequest request, HttpResponder responder,
                   @PathParam("appId") String appId, @PathParam("entityType") String entityType,
                   @PathParam("entityId") String entityId) {

    long fromOffset;
    int maxEvents;
    Filter filter;
    LoggingContext loggingContext;
    LogReaderCallback logCallback;
    try {
      String accountId = getAuthenticatedAccountId(request);

      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();
      String filterStr = "";
      if (queryParams.get("filter") != null && !queryParams.get("filter").isEmpty()) {
        filterStr = queryParams.get("filter").get(0);
      }
      filter = FilterParser.parse(filterStr);

      maxEvents = queryParams.get("max") != null && !queryParams.get("max").isEmpty() ?
        Integer.parseInt(queryParams.get("max").get(0)) : 50;

      fromOffset = queryParams.get("fromOffset") != null && !queryParams.get("fromOffset").isEmpty() ?
        Long.parseLong(queryParams.get("fromOffset").get(0)) : -1;

      loggingContext =
        LoggingContextHelper.getLoggingContext(accountId, appId,
                                               entityId, getEntityType(EntityType.valueOf(entityType)));
      logCallback = new LogReaderCallback(responder, logPattern);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
      return;
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    } catch (Throwable e) {
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      return;
    }

    logReader.getLogPrev(loggingContext, fromOffset, maxEvents, filter, logCallback);
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
      throw new IllegalArgumentException("Null entity type");
    }

    switch (entityType) {
      case flow:
        return LoggingContextHelper.EntityType.FLOW;
      case procedure:
        return LoggingContextHelper.EntityType.PROCEDURE;
      case mapreduce:
        return LoggingContextHelper.EntityType.MAP_REDUCE;
      default:
        throw new IllegalArgumentException(String.format("Illegal entity type %s", entityType));
    }
  }

}
