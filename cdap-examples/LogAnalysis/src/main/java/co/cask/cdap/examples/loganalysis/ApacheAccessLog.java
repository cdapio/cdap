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
 *
 * This class is taken from on the DataBricks "Log Analysis with Spark" reference application.
 * The original file may be found here:
 * http://databricks.gitbooks.io/databricks-spark-reference-applications/content/logs_analyzer/
 * chapter1/java8/src/main/java/com/databricks/apps/logs/ApacheAccessLog.java
 *
 * Attribution-NonCommercial 3.0 Unported:
 * http://databricks.gitbooks.io/databricks-spark-reference-applications/content/LICENSE
 *
 */

package co.cask.cdap.examples.loganalysis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class represents an Apache access log line.
 * See http://httpd.apache.org/docs/2.2/logs.html for more details.
 */
public class ApacheAccessLog implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ApacheAccessLog.class);

  private String ipAddress;
  private String clientIdentd;
  private String userID;
  private String dateTimeString;
  private String method;
  private String endpoint;
  private String protocol;
  private int responseCode;
  private long contentSize;

  private ApacheAccessLog(String ipAddress, String clientIdentd, String userID,
                          String dateTime, String method, String endpoint,
                          String protocol, String responseCode,
                          String contentSize) {
    this.ipAddress = ipAddress;
    this.clientIdentd = clientIdentd;
    this.userID = userID;
    this.dateTimeString = dateTime;
    this.method = method;
    this.endpoint = endpoint;
    this.protocol = protocol;
    this.responseCode = Integer.parseInt(responseCode);
    this.contentSize = Long.parseLong(contentSize);
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public String getClientIdentd() {
    return clientIdentd;
  }

  public String getUserID() {
    return userID;
  }

  public String getDateTimeString() {
    return dateTimeString;
  }

  public String getMethod() {
    return method;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getProtocol() {
    return protocol;
  }

  public int getResponseCode() {
    return responseCode;
  }

  public long getContentSize() {
    return contentSize;
  }

  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
  }

  public void setClientIdentd(String clientIdentd) {
    this.clientIdentd = clientIdentd;
  }

  public void setUserID(String userID) {
    this.userID = userID;
  }

  public void setDateTimeString(String dateTimeString) {
    this.dateTimeString = dateTimeString;
  }

  public void setMethod(String method) {
    this.method = method;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  public void setResponseCode(int responseCode) {
    this.responseCode = responseCode;
  }

  public void setContentSize(long contentSize) {
    this.contentSize = contentSize;
  }

  // Example Apache log line:
  //   127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
  private static final String LOG_ENTRY_PATTERN =
    // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
    "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
  private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

  public static ApacheAccessLog parseFromLogLine(String logline) {
    Matcher m = PATTERN.matcher(logline);
    if (!m.find()) {
      LOG.error("Cannot parse logline" + logline);
      throw new RuntimeException("Error parsing logline");
    }

    return new ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
                               m.group(5), m.group(6), m.group(7), m.group(8), m.group(9));
  }

  @Override public String toString() {
    return String.format("%s %s %s [%s] \"%s %s %s\" %s %s",
                         ipAddress, clientIdentd, userID, dateTimeString, method, endpoint,
                         protocol, responseCode, contentSize);
  }
}
