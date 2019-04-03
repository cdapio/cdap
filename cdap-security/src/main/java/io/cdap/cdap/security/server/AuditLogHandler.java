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

package co.cask.cdap.security.server;

import co.cask.cdap.common.logging.AuditLogEntry;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Handler for audit logging for the {@link ExternalAuthenticationServer}.
 */
public class AuditLogHandler extends DefaultHandler {

  private final Logger logger;

  public AuditLogHandler(Logger logger) {
    this.logger = logger;
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request,
                     HttpServletResponse response) throws IOException, ServletException {
    String requestLine = request.getMethod() + " " + request.getRequestURI() + " " + request.getProtocol();
    AuditLogEntry logEntry = new AuditLogEntry(requestLine, request.getRemoteUser(),
                                               InetAddress.getByName(request.getRemoteAddr()).getHostAddress(),
                                               Collections.<String, String>emptyMap());
    logEntry.setResponse(response.getStatus(), ((Response) response).getContentCount());
    logger.trace(logEntry.toString());
  }
}
