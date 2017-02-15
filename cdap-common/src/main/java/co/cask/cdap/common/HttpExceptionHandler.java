/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.common.HttpErrorStatusProvider;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.http.ExceptionHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
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
    // Check if the exception is caused by Service being unavailable: this will happen during master startup
    for (Throwable cause : Throwables.getCausalChain(t)) {
      if (cause instanceof ServiceUnavailableException) {
        responder.sendString(HttpResponseStatus.SERVICE_UNAVAILABLE, cause.getMessage());
        return;
      }
    }

    // If the exception provides http status, response with it
    if (t instanceof HttpErrorStatusProvider) {
      logWithTrace(request, t);
      responder.sendString(HttpResponseStatus.valueOf(((HttpErrorStatusProvider) t).getStatusCode()),
                           t.getMessage());
      return;
    }

    // For some known exception naming convention, response with 4xx
    if (t.getClass().getName().endsWith("NotFoundException")) {
      logWithTrace(request, t);
      responder.sendString(HttpResponseStatus.NOT_FOUND, t.getMessage());
      return;
    }
    if (t.getClass().getName().endsWith("AlreadyExistsException")) {
      logWithTrace(request, t);
      responder.sendString(HttpResponseStatus.CONFLICT, t.getMessage());
      return;
    }

    // If it is not some known exception type, response with 500.
    LOG.error("Unexpected error: request={} {} user={}:", request.getMethod().getName(), request.getUri(),
              Objects.firstNonNull(SecurityRequestContext.getUserId(), "<null>"), t);
    responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, Throwables.getRootCause(t).getMessage());
  }

  private void logWithTrace(HttpRequest request, Throwable t) {
    LOG.trace("Error in handling request={} {} for user={}:", request.getMethod().getName(), request.getUri(),
              Objects.firstNonNull(SecurityRequestContext.getUserId(), "<null>"), t);
  }
}
