/*
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
import co.cask.http.HandlerContext;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * v2 {@link HttpHandler} to handle /logs requests.
 */
@Path(Constants.Gateway.API_VERSION_2)
public class LogHandlerV2 extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LogHandlerV2.class);

  private final LogReader logReader;
  private final String logPattern;

  /**
   * v3 Handler
   */
  private final LogHandler logHandler;

  @Inject
  public LogHandlerV2(Authenticator authenticator, LogReader logReader, CConfiguration cConfig,
                      LogHandler logHandler) {
    super(authenticator);
    this.logReader = logReader;
    this.logPattern = cConfig.get(LoggingConfiguration.LOG_PATTERN, LoggingConfiguration.DEFAULT_LOG_PATTERN);
    this.logHandler = logHandler;
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
  public void sysList(HttpRequest request, HttpResponder responder, @PathParam("component-id") String componentId,
                      @PathParam("service-id") String serviceId,
                      @QueryParam("start") @DefaultValue("-1") long fromTimeMsParam,
                      @QueryParam("stop") @DefaultValue("-1") long toTimeMsParam,
                      @QueryParam("escape") @DefaultValue("true") boolean escape,
                      @QueryParam("filter") @DefaultValue("") String filterStr) {
    try {
      Filter filter = FilterParser.parse(filterStr);
      long fromTimeMs = TimeUnit.MILLISECONDS.convert(fromTimeMsParam, TimeUnit.SECONDS);
      long toTimeMs = TimeUnit.MILLISECONDS.convert(toTimeMsParam, TimeUnit.SECONDS);

      if (fromTimeMs < 0 || toTimeMs < 0 || toTimeMs <= fromTimeMs) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid time range. 'start' and 'stop' should be " +
          "greater than zero and stop should be greater than start.");
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
                   @PathParam("entity-id") String entityId, @QueryParam("start") long fromTimeMs,
                   @QueryParam("stop") long toTimeMs, @QueryParam("escape") @DefaultValue("true") boolean escape,
                   @QueryParam("filter") @DefaultValue("") String filterStr) {
    logHandler.list(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId, entityType, entityId,
                    fromTimeMs, toTimeMs, escape, filterStr);
  }

  @GET
  @Path("/system/{component-id}/{service-id}/logs/next")
  public void sysNext(HttpRequest request, HttpResponder responder, @PathParam("component-id") String componentId,
                      @PathParam("service-id") String serviceId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                      @QueryParam("fromOffset") @DefaultValue("-1") long fromOffset,
                      @QueryParam("escape") @DefaultValue("true") boolean escape,
                      @QueryParam("filter") @DefaultValue("") String filterStr) {
    try {
      Filter filter = FilterParser.parse(filterStr);

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
                   @PathParam("entity-id") String entityId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                   @QueryParam("fromOffset") @DefaultValue("-1") long fromOffset,
                   @QueryParam("escape") @DefaultValue("true") boolean escape,
                   @QueryParam("filter") @DefaultValue("") String filterStr) {
    logHandler.next(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId, entityType, entityId,
                    maxEvents, fromOffset, escape, filterStr);
  }

  @GET
  @Path("/system/{component-id}/{service-id}/logs/prev")
  public void sysPrev(HttpRequest request, HttpResponder responder, @PathParam("component-id") String componentId,
                      @PathParam("service-id") String serviceId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                      @QueryParam("fromOffset") @DefaultValue("-1") long fromOffset,
                      @QueryParam("escape") @DefaultValue("true") boolean escape,
                      @QueryParam("filter") @DefaultValue("") String filterStr) {
    try {
      Filter filter = FilterParser.parse(filterStr);

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
                   @PathParam("entity-id") String entityId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                   @QueryParam("fromOffset") @DefaultValue("-1") long fromOffset,
                   @QueryParam("escape") @DefaultValue("true") boolean escape,
                   @QueryParam("filter") @DefaultValue("") String filterStr) {
    logHandler.prev(rewriteRequest(request), responder, Constants.DEFAULT_NAMESPACE, appId, entityType, entityId,
                    maxEvents, fromOffset, escape, filterStr);
  }

  /**
   * Updates the request URI to its v3 URI before delegating the call to the corresponding v3 handler.
   * Note: This piece of code is duplicated from AbstractAppFabricHttpHandler, but its ok since this temporary, till we
   * support v2 APIs
   *
   * @param request the original {@link HttpRequest}
   * @return {@link HttpRequest} with modified URI
   */
  public HttpRequest rewriteRequest(HttpRequest request) {
    String originalUri = request.getUri();
    request.setUri(originalUri.replaceFirst(Constants.Gateway.API_VERSION_2, Constants.Gateway.API_VERSION_3 +
      "/namespaces/" + Constants.DEFAULT_NAMESPACE));
    return request;
  }
}
