/*
 * Copyright © 2017 Cask Data, Inc.
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

import com.google.common.collect.ImmutableSet;
import io.netty.handler.codec.http.HttpMethod;

import java.util.Objects;
import java.util.Set;

/**
 * Audit log content which indicates what additional info is needed for the
 * {@link co.cask.cdap.common.logging.AuditLogEntry}
 */
public class AuditLogConfig {

  private final HttpMethod httpMethod;
  private final boolean logRequestBody;
  private final boolean logResponseBody;
  private final Set<String> headerNames;

  public AuditLogConfig(HttpMethod httpMethod, boolean logRequestBody, boolean logResponseBody,
                        Iterable<String> headerNames) {
    this.httpMethod = httpMethod;
    this.logRequestBody = logRequestBody;
    this.logResponseBody = logResponseBody;
    this.headerNames = ImmutableSet.copyOf(headerNames);
  }

  public HttpMethod getHttpMethod() {
    return httpMethod;
  }

  public boolean isLogRequestBody() {
    return logRequestBody;
  }

  public boolean isLogResponseBody() {
    return logResponseBody;
  }

  public Set<String> getHeaderNames() {
    return headerNames;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AuditLogConfig other = (AuditLogConfig) o;
    return Objects.equals(httpMethod, other.getHttpMethod()) &&
      logRequestBody == other.isLogRequestBody() &&
      logResponseBody == other.isLogResponseBody() &&
      Objects.equals(headerNames, other.getHeaderNames());
  }

  @Override
  public int hashCode() {
    return Objects.hash(httpMethod, logRequestBody, logResponseBody, headerNames);
  }

  @Override
  public String toString() {
    return "AuditLogContent{" +
      "httpMethod=" + httpMethod +
      ", logRequestBody=" + logRequestBody +
      ", logResponseBody=" + logResponseBody +
      ", headerNames=" + headerNames +
      '}';
  }
}
