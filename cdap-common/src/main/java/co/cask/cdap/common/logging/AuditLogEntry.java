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

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpVersion;

import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Represents an entry in an audit log.
 */
public final class AuditLogEntry {
  private static final DateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");

  /** Each audit log field will default to "-" if the field is missing or not supported. */
  private static final String DEFAULT_VALUE = "-";

  /** Indicates whether this entry has already been logged. */
  private boolean logged;

  private InetAddress clientIP;
  private String userName;
  private Date date;
  private String requestLine;
  private String requestBody;
  private Integer responseCode;
  private Long responseContentLength;
  private String userIdentity;

  public AuditLogEntry() {
    this.date = new Date();
  }

  public String toString() {
    return String.format("%s %s %s [%s] \"%s\" %s %s %s",
                         clientIP != null ? clientIP.getHostAddress() : DEFAULT_VALUE,
                         fieldOrDefault(userIdentity),
                         fieldOrDefault(userName),
                         DEFAULT_DATE_FORMAT.format(date),
                         fieldOrDefault(requestLine),
                         fieldOrDefault(requestBody),
                         fieldOrDefault(responseCode),
                         fieldOrDefault(responseContentLength));
  }

  public String getUserIdentity() {
    return userIdentity;
  }

  public void setUserIdentity(String userIdentity) {
    this.userIdentity = userIdentity;
  }

  public boolean isLogged() {
    return logged;
  }

  public void setLogged(boolean logged) {
    this.logged = logged;
  }

  public InetAddress getClientIP() {
    return clientIP;
  }

  public void setClientIP(InetAddress clientIP) {
    this.clientIP = clientIP;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public Date getDate() {
    return date;
  }

  public void setDate(Date date) {
    this.date = date;
  }

  public String getRequestLine() {
    return requestLine;
  }

  public void setRequestLine(HttpMethod method, String uri, HttpVersion protocolVersion) {
    this.requestLine = method + " " + uri + " " + protocolVersion;
  }

  public void setRequestLine(String method, String uri, String protocolVersion) {
    this.requestLine = method + " " + uri + " " + protocolVersion;
  }

  public void setRequestBody(String requestBody) {
    this.requestBody = requestBody;
  }

  public String getRequestBody() {
    return requestBody;
  }

  public Integer getResponseCode() {
    return responseCode;
  }

  public void setResponseCode(Integer responseCode) {
    this.responseCode = responseCode;
  }

  public Long getResponseContentLength() {
    return responseContentLength;
  }

  public void setResponseContentLength(Long responseContentLength) {
    this.responseContentLength = responseContentLength;
  }

  private String fieldOrDefault(Object field) {
    return field == null ? DEFAULT_VALUE : field.toString();
  }
}
