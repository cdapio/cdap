/*
 * Copyright 2014 Cask, Inc.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Filter to write audit logs.
 */
public class AuditLogFilter implements Filter {

  private static final Logger AUDIT_LOG = LoggerFactory.getLogger("http-access");

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {

  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,
                       FilterChain filterChain) throws IOException, ServletException {
    HttpServletRequest request = (HttpServletRequest) servletRequest;
    HttpServletResponse response = (HttpServletResponse) servletResponse;

    AuditLogEntry logEntry = new AuditLogEntry();
    logEntry.setUserName(request.getRemoteUser());
    logEntry.setClientIP(InetAddress.getByName(request.getRemoteAddr()));
    logEntry.setRequestLine(request.getMethod(), request.getRequestURI(), request.getProtocol());
    logEntry.setResponseCode(response.getStatus());
    if (response.getHeader("Content-Length") != null) {
      logEntry.setResponseContentLength(Long.parseLong(response.getHeader("Content-Length")));
    }
    AUDIT_LOG.trace(logEntry.toString());
  }

  @Override
  public void destroy() {

  }
}
