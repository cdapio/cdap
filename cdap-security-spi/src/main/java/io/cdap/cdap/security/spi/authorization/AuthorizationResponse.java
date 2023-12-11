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

import java.util.Set;
import javax.annotation.Nullable;

/**
 * This is the response object expected from a external auth service implementation for every authorization call.
 */
public class AuthorizationResponse {

  /**
   * The status of a call for authorization check.
   */
  public enum AuthorizationStatus {
    AUTHORIZED,
    UNAUTHORIZED,
    NOT_REQUIRED
  }

  private final AuditLogContext auditLogContext;
  private final AuthorizationStatus authorized;

  // Fields to handle Unauthorized exception
  private final String principal;
  private final Set<String> missingPermissions;
  private final String entity;
  private final boolean requiresAllPermissions;
  private final boolean includePrincipal;
  private final String addendum;

  private AuthorizationResponse(Builder builder) {
    this.auditLogContext = builder.auditLogContext;
    this.authorized = builder.authorized;
    this.principal = builder.principal;
    this.missingPermissions = builder.missingPermissions;
    this.entity = builder.entity;
    this.requiresAllPermissions = builder.requiresAllPermissions;
    this.includePrincipal = builder.includePrincipal;
    this.addendum = builder.addendum;
  }

  /**
   * the audit log context object from the enforcement response.
   *
   * @return {@link AuditLogContext}
   */
  public AuditLogContext getAuditLogContext() {
    return auditLogContext;
  }

  /**
   * if the enforcement/ authorization was allowed.
   *
   * @return true/false
   */
  public AuthorizationStatus isAuthorized() {
    return authorized;
  }


  /**
   * Returns the string which represents the user principal identity who failed the authorization
   * check.
   *
   * @return The principal string
   */
  @Nullable
  public String getPrincipal() {
    return principal;
  }

  /**
   * Returns all permissions which were missing for the authorization check.
   *
   * @return The missing permissions
   */
  public Set<String> getMissingPermissions() {
    return missingPermissions;
  }

  /**
   * Returns the string which represents the entity upon which the permission check failed.
   *
   * @return The entity string
   */
  @Nullable
  public String getEntity() {
    return entity;
  }

  /**
   * Returns if all the permissions are required.
   *
   * @return whether all the permissions are required
   */
  public boolean isRequiresAllPermissions() {
    return requiresAllPermissions;
  }

  /**
   * Returns whether this exception includes the principal in the error message or not.
   *
   * @return whether to include the principal in the exception message
   */
  public boolean isIncludePrincipal() {
    return includePrincipal;
  }

  /**
   * Returns the custom addendum message for this exception message.
   *
   * @return The custom addendum message
   */
  @Nullable
  public String getAddendum() {
    return addendum;
  }

  /**
   * A builder class to create a {@link AuthorizationResponse}.
   */
  public static class Builder {
    private AuditLogContext auditLogContext;
    private AuthorizationStatus authorized;

    private String principal;
    private Set<String> missingPermissions;
    private String entity;
    private boolean requiresAllPermissions;
    private boolean includePrincipal;
    private String addendum;

    public Builder setAuditLogContext(AuditLogContext auditLogContext) {
      this.auditLogContext = auditLogContext;
      return this;
    }

    public Builder setAuthorized(AuthorizationStatus authorized) {
      this.authorized = authorized;
      return this;
    }

    public Builder setPrincipal(String principal) {
      this.principal = principal;
      return this;
    }

    public Builder setMissingPermissions(Set<String> missingPermissions) {
      this.missingPermissions = missingPermissions;
      return this;
    }

    public Builder setEntity(String entity) {
      this.entity = entity;
      return this;
    }

    public Builder setRequiresAllPermissions(boolean requiresAllPermissions) {
      this.requiresAllPermissions = requiresAllPermissions;
      return this;
    }

    public Builder setIncludePrincipal(boolean includePrincipal) {
      this.includePrincipal = includePrincipal;
      return this;
    }

    public Builder setAddendum(String addendum) {
      this.addendum = addendum;
      return this;
    }

    /**
     * A default instance of {@link AuthorizationResponse} where authorization and audit logging is not required.
     */
    public static AuthorizationResponse defaultNotRequired() {
      return new AuthorizationResponse.Builder()
        .setAuthorized(AuthorizationStatus.NOT_REQUIRED)
        .setAuditLogContext(AuditLogContext.Builder.defaultNotRequired())
        .build();
    }

    public AuthorizationResponse build() {
      return new AuthorizationResponse(this);
    }
  }
}
