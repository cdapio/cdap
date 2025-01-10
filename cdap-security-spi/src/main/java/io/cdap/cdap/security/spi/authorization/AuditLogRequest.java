/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.security.spi.authorization;

import java.util.Queue;
import javax.annotation.Nullable;

/**
 * The object that will be sent to Auth Extension which would contain the collections of audit logs
 * {@link AuditLogContext} related to a Single Call or Operation.
 * This also contains various other metadata required for Audit Logging.
 */
public class AuditLogRequest {

  private final int operationResponseCode;
  private final String userIp;
  private final String uri;
  private final String handler;
  private final String method;
  private final String methodType;
  private final Queue<AuditLogContext> auditLogContextQueue;
  private final Long startTimeNanos;
  private final Long endTimeNanos;

  public AuditLogRequest(int operationResponseCode, String userIp, String uri, String handler, String method,
                         String methodType, Queue<AuditLogContext> auditLogContextQueue, @Nullable  Long startTimeNanos,
                         Long endTimeNanos) {
    this.operationResponseCode = operationResponseCode;
    this.userIp = userIp;
    this.uri = uri;
    this.handler = handler;
    this.method = method;
    this.methodType = methodType;
    this.auditLogContextQueue = auditLogContextQueue;
    this.startTimeNanos = startTimeNanos;
    this.endTimeNanos = endTimeNanos;
  }

  public int getOperationResponseCode() {
    return operationResponseCode;
  }

  public String getUserIp() {
    return userIp;
  }

  public String getUri() {
    return uri;
  }

  public String getHandler() {
    return handler;
  }

  public String getMethod() {
    return method;
  }

  public String getMethodType() {
    return methodType;
  }

  public Queue<AuditLogContext> getAuditLogContextQueue() {
    return auditLogContextQueue;
  }

  public long getStartTimeNanos() {
    return startTimeNanos;
  }

  public long getEndTimeNanos() {
    return endTimeNanos;
  }

  /**
   * A Builder class to store metadata as in when fetched along the netty pipeline. And later used to build at the last
   * stage.
   */
  public static class Builder {
    private int operationResponseCode;
    private String userIp;
    private String uri;
    private String handler;
    private String method;
    private String methodType;
    private Queue<AuditLogContext> auditLogContextQueue;
    private Long startTimeNanos;
    private Long endTimeNanos;

    public Builder operationResponseCode(int operationResponseCode) {
      this.operationResponseCode = operationResponseCode;
      return this;
    }

    public Builder userIp(String userIp) {
      this.userIp = userIp;
      return this;
    }

    public Builder uri(String uri) {
      this.uri = uri;
      return this;
    }

    public Builder handler(String handler) {
      this.handler = handler;
      return this;
    }

    public Builder method(String method) {
      this.method = method;
      return this;
    }

    public Builder methodType(String methodType) {
      this.methodType = methodType;
      return this;
    }

    public Builder auditLogContextQueue(Queue<AuditLogContext> auditLogContextQueue) {
      this.auditLogContextQueue = auditLogContextQueue;
      return this;
    }

    public Builder startTimeNanos(Long startTimeNanos) {
      this.startTimeNanos = startTimeNanos;
      return this;
    }

    public Builder endTimeNanos(Long endTimeNanos) {
      this.endTimeNanos = endTimeNanos;
      return this;
    }

    public AuditLogRequest build() {
      return new AuditLogRequest(operationResponseCode, userIp, uri, handler,  method, methodType, auditLogContextQueue,
                                 startTimeNanos, endTimeNanos);
    }
  }
}
