/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.common;

import co.cask.cdap.common.http.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.http.ExceptionHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common logic to handle exceptions in handler methods.
 */
public class HttpExceptionHandler extends ExceptionHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HttpExceptionHandler.class);

  @Override
  public void handle(Throwable t, HttpRequest request, HttpResponder responder) {
    if (Iterables.size(Iterables.filter(Throwables.getCausalChain(t), ServiceUnavailableException.class)) > 0) {
      responder.sendString(HttpResponseStatus.SERVICE_UNAVAILABLE, t.getMessage());
    } else if (t instanceof BadRequestException) {
      logWithTrace(request, t);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, t.getMessage());
    } else if (t instanceof ConflictException) {
      logWithTrace(request, t);
      responder.sendString(HttpResponseStatus.CONFLICT, t.getMessage());
    } else if (t instanceof NotFoundException) {
      logWithTrace(request, t);
      responder.sendString(HttpResponseStatus.NOT_FOUND, t.getMessage());
    } else if (t instanceof NotImplementedException) {
      logWithTrace(request, t);
      responder.sendString(HttpResponseStatus.NOT_IMPLEMENTED, t.getMessage());
    } else if (t instanceof MethodNotAllowedException) {
      logWithTrace(request, t);
      responder.sendStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
    } else if (t instanceof UnauthenticatedException) {
      logWithTrace(request, t);
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } else if (t instanceof UnauthorizedException) {
      logWithTrace(request, t);
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
    } else if (t instanceof FeatureDisabledException) {
      logWithTrace(request, t);
      responder.sendString(HttpResponseStatus.NOT_IMPLEMENTED, t.getMessage());
    } else {
      LOG.error("Unexpected error: request={} {} user={}:", request.getMethod().getName(), request.getUri(),
                SecurityRequestContext.getUserId().or("<null>"), t);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, Throwables.getRootCause(t).getMessage());
    }
  }

  private void logWithTrace(HttpRequest request, Throwable t) {
    LOG.trace("Error in handling request={} {} for user={}:", request.getMethod().getName(), request.getUri(),
              SecurityRequestContext.getUserId().or("<null>"), t);
  }
}
