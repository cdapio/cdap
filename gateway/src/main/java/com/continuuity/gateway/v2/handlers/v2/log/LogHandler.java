package com.continuuity.gateway.v2.handlers.v2.log;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.HandlerContext;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Handler to serve log requests.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class LogHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LogHandler.class);

  private final LogReader logReader;
  private final String logPattern;

  private enum EntityType {
    flows, procedures, mapreduce
  }

  @Inject
  public LogHandler(GatewayAuthenticator authenticator, LogReader logReader, CConfiguration cConfig) {
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
  @Path("/apps/{app-id}/{entity-type}/{entity-id}/logs")
  public void list(HttpRequest request, HttpResponder responder,
                   @PathParam("app-id") String appId, @PathParam("entity-type") String entityType,
                   @PathParam("entity-id") String entityId) {

    try {
      String accountId = getAuthenticatedAccountId(request);

      // Parse start, stop, filter and escape
      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();
      long fromTimeMs = parseTimestamp(queryParams.get("start"));
      long toTimeMs = parseTimestamp(queryParams.get("stop"));

      if (fromTimeMs < 0 || toTimeMs < 0 || toTimeMs <= fromTimeMs) {
        responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
        return;
      }

      String filterStr = "";
      if (queryParams.get("filter") != null && !queryParams.get("filter").isEmpty()) {
        filterStr = queryParams.get("filter").get(0);
      }
      Filter filter = FilterParser.parse(filterStr);

      boolean escape = queryParams.get("escape") == null || queryParams.get("escape").isEmpty() ||
        Boolean.parseBoolean(queryParams.get("escape").get(0));

      LoggingContext loggingContext =
        LoggingContextHelper.getLoggingContext(accountId, appId,
                                               entityId, getEntityType(EntityType.valueOf(entityType)));
      ChunkedLogReaderCallback logCallback = new ChunkedLogReaderCallback(responder, logPattern, escape);

      logReader.getLog(loggingContext, fromTimeMs, toTimeMs, filter,
                       logCallback);
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
  @Path("/apps/{app-id}/{entity-type}/{entity-id}/logs/next")
  public void next(HttpRequest request, HttpResponder responder,
                   @PathParam("app-id") String appId, @PathParam("entity-type") String entityType,
                   @PathParam("entity-id") String entityId) {

    try {
      String accountId = getAuthenticatedAccountId(request);

      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();
      String filterStr = "";
      if (queryParams.get("filter") != null && !queryParams.get("filter").isEmpty()) {
        filterStr = queryParams.get("filter").get(0);
      }
      Filter filter = FilterParser.parse(filterStr);

      int maxEvents = queryParams.get("max") != null && !queryParams.get("max").isEmpty() ?
        Integer.parseInt(queryParams.get("max").get(0)) : 50;

      long fromOffset = queryParams.get("fromOffset") != null && !queryParams.get("fromOffset").isEmpty() ?
        Long.parseLong(queryParams.get("fromOffset").get(0)) : -1;

      boolean escape = queryParams.get("escape") == null || queryParams.get("escape").isEmpty() ||
        Boolean.parseBoolean(queryParams.get("escape").get(0));

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
  @Path("/apps/{app-id}/{entity-type}/{entity-id}/logs/prev")
  public void prev(HttpRequest request, HttpResponder responder,
                   @PathParam("app-id") String appId, @PathParam("entity-type") String entityType,
                   @PathParam("entity-id") String entityId) {

    try {
      String accountId = getAuthenticatedAccountId(request);

      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();
      String filterStr = "";
      if (queryParams.get("filter") != null && !queryParams.get("filter").isEmpty()) {
        filterStr = queryParams.get("filter").get(0);
      }
      Filter filter = FilterParser.parse(filterStr);

      int maxEvents = queryParams.get("max") != null && !queryParams.get("max").isEmpty() ?
        Integer.parseInt(queryParams.get("max").get(0)) : 50;

      long fromOffset = queryParams.get("fromOffset") != null && !queryParams.get("fromOffset").isEmpty() ?
        Long.parseLong(queryParams.get("fromOffset").get(0)) : -1;

      boolean escape = queryParams.get("escape") == null || queryParams.get("escape").isEmpty() ||
        Boolean.parseBoolean(queryParams.get("escape").get(0));

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
      default:
        throw new IllegalArgumentException(String.format("Illegal program type %s", entityType));
    }
  }

}
