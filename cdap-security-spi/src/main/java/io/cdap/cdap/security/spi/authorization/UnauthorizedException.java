/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.proto.security.Principal;

import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Exception thrown when Authentication is successful, but a {@link Principal} is not authorized to perform an
 * {@link Action} on an {@link EntityId}.
 */
public class UnauthorizedException extends RuntimeException implements HttpErrorStatusProvider {
  @Nullable
  private final String principal;
  private final Set<String> missingPermissions;
  @Nullable
  private final String entity;
  @Nullable
  private final String addendum;
  private final boolean includePrincipal;
  private final String message;

  public UnauthorizedException(Principal principal, Action action, EntityId entityId) {
    this(principal.toString(), Collections.singleton(action.toString()),
         String.format("entity '%s'", entityId.toString()), null, true, true, null);
  }

  public UnauthorizedException(Principal principal, Set<Action> actions, EntityId entityId) {
    this(principal.toString(), actions.stream().map(action -> action.toString())
           .collect(Collectors.toCollection(LinkedHashSet::new)), String.format("entity '%s'", entityId.toString()),
         null, true, true, null);
  }

  public UnauthorizedException(Principal principal, Set<Action> actions, EntityId entityId, Throwable ex) {
    this(principal.toString(), actions.stream().map(action -> action.toString())
           .collect(Collectors.toCollection(LinkedHashSet::new)), String.format("entity '%s'", entityId.toString()),
         ex, true, true, null);
  }

  public UnauthorizedException(Principal principal, EntityId entityId) {
    this(principal.toString(), Collections.emptySet(), String.format("entity '%s'", entityId.toString()), null, true,
         true, null);
  }

  public UnauthorizedException(Principal principal, Set<Action> actions, EntityId entityId,
                               boolean mustHaveAllPermissions) {
    this(principal.toString(), actions.stream().map(action -> action.toString())
           .collect(Collectors.toCollection(LinkedHashSet::new)), String.format("entity '%s'", entityId.toString()),
         null, mustHaveAllPermissions, true, null);
  }

  public UnauthorizedException(@Nullable String principal, Set<String> missingPermissions, @Nullable String entity,
                               @Nullable Throwable ex, boolean requiresAllPermissions, boolean includePrincipal,
                               @Nullable String addendum) {
    super(ex);
    this.principal = principal;
    this.missingPermissions = Collections.unmodifiableSet(missingPermissions);
    this.entity = entity;
    this.includePrincipal = includePrincipal;
    this.addendum = addendum;
    // Construct the message.
    StringBuilder messageBuilder = new StringBuilder();
    if (includePrincipal) {
      messageBuilder.append(String.format("Principal '%s' has insufficient permissions ", principal));
    } else {
      messageBuilder.append("Insufficient permissions ");
    }
    messageBuilder.append("to ");
    if (missingPermissions.isEmpty()) {
      messageBuilder.append("access ");
    } else {
      messageBuilder.append("perform ");
      if (missingPermissions.size() == 1) {
        messageBuilder.append(String.format("action '%s' on ", missingPermissions.iterator().next()));
      } else {
        if (!requiresAllPermissions) {
          messageBuilder.append("any one of the ");
        }
        messageBuilder.append(String.format("actions '%s' on ", missingPermissions));
      }
    }
    messageBuilder.append(entity + ".");
    if (addendum != null) {
      messageBuilder.append(addendum);
    }
    this.message = messageBuilder.toString();
  }

  public UnauthorizedException(String message) {
    this.principal = null;
    this.missingPermissions = Collections.emptySet();
    this.entity = null;
    this.includePrincipal = false;
    this.addendum = null;
    this.message = message;
  }

  @Override
  public int getStatusCode() {
    return HttpURLConnection.HTTP_FORBIDDEN;
  }

  @Override
  public String getMessage() {
    return message;
  }

  /**
   * Returns the string which represents the user principal identity who failed the authorization check.
   * @return The principal string
   */
  @Nullable
  public String getPrincipal() {
    return principal;
  }

  /**
   * Returns all permissions which were missing for the authorization check.
   * @return The missing permissions
   */
  public Set<String> getMissingPermissions() {
    return missingPermissions;
  }

  /**
   * Returns the string which represents the entity upon which the permission check failed.
   * @return The entity string
   */
  @Nullable
  public String getEntity() {
    return entity;
  }

  /**
   * Returns whether this exception includes the principal in the error message or not.
   * @return whether to include the principal in the exception message
   */
  public boolean includePrincipal() {
    return includePrincipal;
  }

  /**
   * Returns the custom addendum message for this exception message.
   * @return The custom addendum message
   */
  @Nullable
  public String getAddendum() {
    return addendum;
  }
}
