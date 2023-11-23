/*
 * Copyright © 2023 Cask Data, Inc.
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

import javax.annotation.Nullable;

/**
 * An interface to store the audit context received from Authorization Extensions that will used for further
 * audit logging.
 */
public class AuditLogContext {

  private final boolean auditLoggingRequired;
  private final String auditLogBody;

  private AuditLogContext(boolean auditLoggingRequired, String auditLogBody) {
    this.auditLoggingRequired = auditLoggingRequired;
    this.auditLogBody = auditLogBody;
  }

  private AuditLogContext(Builder builder) {
    this.auditLoggingRequired = builder.auditLoggingRequired;
    this.auditLogBody = builder.auditLogBody;
  }

  /**
   * Whether we need to emit the audit log.
   *
   * @return true or false
   */
  public boolean isAuditLoggingRequired() {
    return auditLoggingRequired;
  }

  /**
   * The encoded body of audit log that needs to be emitted.
   *
   * @return encoded String
   */
  @Nullable
  public String getAuditLogBody() {
    return auditLogBody;
  }

  /**
   * The Builder class for creating a {@link AuditLogContext}.
   */
  public static class Builder {
    private boolean auditLoggingRequired;
    private String auditLogBody;

    public Builder setAuditLoggingRequired(boolean auditLoggingRequired) {
      this.auditLoggingRequired = auditLoggingRequired;
      return this;
    }

    public Builder setAuditLogBody(String auditLogBody) {
      this.auditLogBody = auditLogBody;
      return this;
    }

    /**
     * A default implementation / instance of {@link AuditLogContext} where audit logging is not required.
     *
     * @return {@link AuditLogContext}
     */
    public static AuditLogContext defaultNotRequired() {
      return new Builder()
        .setAuditLoggingRequired(false)
        .setAuditLogBody(null)
        .build();
    }

    public AuditLogContext build() {
      return new AuditLogContext(this);
    }
  }
}
