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

package co.cask.cdap.common.logging;

import co.cask.cdap.common.conf.Constants;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Represents an entry in an audit log.
 */
public final class AuditLogEntry {
  private static final DateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");

  /** Each audit log field will default to "-" if the field is missing or not supported. */
  private static final String DEFAULT_VALUE = "-";

  private final String requestLine;
  private final Map<String, String> headers;
  private final Date date;
  private final String clientIP;

  private String userName;
  private Integer responseCode;
  private Long responseContentLength;
  private StringBuilder requestBody;
  private StringBuilder responseBody;

  public AuditLogEntry(HttpRequest request, @Nullable String clientIP) {
    this(request, clientIP, Collections.<String>emptySet());
  }

  public AuditLogEntry(HttpRequest request, @Nullable String clientIP, Set<String> includeHeaders) {
    this(request.method() + " " + request.uri() + " " + request.protocolVersion(),
         request.headers().contains(Constants.Security.Headers.USER_ID)
           ? request.headers().get(Constants.Security.Headers.USER_ID)
           : null,
         clientIP,
         extractHeaders(request, includeHeaders));
  }

  public AuditLogEntry(String requestLine, @Nullable String userName,
                       @Nullable String clientIP, Map<String, String> headers) {
    this.date = new Date();
    this.requestLine = requestLine;
    this.userName = userName;
    this.clientIP = clientIP;
    this.headers = headers.isEmpty() ? null : Collections.unmodifiableMap(new TreeMap<>(headers));
  }

  public String toString() {
    return String.format("%s - %s [%s] \"%s\" %s %s %s %s %s",
                         toString(clientIP),
                         toString(userName),
                         DEFAULT_DATE_FORMAT.format(date),
                         toString(requestLine),
                         toString(headers),
                         toString(requestBody),
                         toString(responseCode),
                         toString(responseContentLength),
                         toString(responseBody));
  }

  public void setUserName(@Nullable String userName) {
    this.userName = userName;
  }

  public void appendRequestBody(String requestBody) {
    if (this.requestBody == null) {
      this.requestBody = new StringBuilder();
    }
    this.requestBody.append(requestBody);
  }

  public void setResponse(HttpResponse response) {
    responseCode = response.status().code();
    responseContentLength = HttpUtil.getContentLength(response, -1L);
    if (responseContentLength < 0) {
      responseContentLength = null;
    }
  }

  public void setResponse(int code, long length) {
    responseCode = code;
    responseContentLength = length >= 0 ? length : null;
  }

  public void appendResponseBody(String appendBody) {
    if (this.responseBody == null) {
      this.responseBody = new StringBuilder();
    }
    this.responseBody.append(appendBody);
  }

  private String toString(Object value) {
    return value == null ? DEFAULT_VALUE : value.toString();
  }

  private static Map<String, String> extractHeaders(HttpRequest request, Set<String> headerNames) {
    Map<String, String> headers = new TreeMap<>();
    for (String header : headerNames) {
      if (request.headers().contains(header)) {
        headers.put(header, request.headers().get(header));
      }
    }
    return headers;
  }
}
