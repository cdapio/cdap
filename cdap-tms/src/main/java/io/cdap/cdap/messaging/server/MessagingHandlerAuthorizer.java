/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.messaging.server;

import com.google.inject.Inject;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import javax.annotation.Nullable;

/**
 * Authorizes requests to the messaging HTTP handlers on the namespace of the target topic.
 *
 * <p>Topics are authorized through their namespace because authorization does not support topics
 * as entities. When internal authentication is enabled, only internal callers can access the
 * topics of the system namespace.
 */
final class MessagingHandlerAuthorizer {

  /**
   * Credential value set by {@code WorkerAuthenticationContext} when a task worker or preview
   * runner makes a background call outside of a user request.
   */
  private static final String EMPTY_WORKER_CREDENTIAL_VALUE = "empty-worker-user-credential";

  private final boolean internalAuthEnabled;
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  MessagingHandlerAuthorizer(CConfiguration cConf, AccessEnforcer accessEnforcer,
      AuthenticationContext authenticationContext) {
    this.internalAuthEnabled = cConf.getBoolean(Constants.Security.INTERNAL_AUTH_ENABLED);
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
  }

  /**
   * Authorizes the caller to perform the given permission on the topics of a namespace.
   *
   * <p>Every endpoint calls this first, so that a caller without permission gets the same answer
   * whether or not the topic exists and whether or not the rest of its request is valid.
   *
   * @return the id of the namespace
   * @throws IllegalArgumentException if the namespace name is invalid
   * @throws UnauthorizedException if the caller is not allowed to perform the permission
   */
  NamespaceId authorize(String namespace, StandardPermission permission) throws AccessException {
    NamespaceId namespaceId = new NamespaceId(namespace);
    Principal principal = authenticationContext.getPrincipal();
    if (internalAuthEnabled && NamespaceId.SYSTEM.equals(namespaceId)
        && !hasInternalCredential(principal)) {
      throw new UnauthorizedException(
          "Only internal services can access topics of the system namespace.");
    }
    accessEnforcer.enforce(namespaceId, principal, permission);
    return namespaceId;
  }

  /**
   * Authorizes the caller to publish messages to a topic in the given namespace.
   *
   * <p>Requires {@link StandardPermission#UPDATE} on the namespace. When internal authentication
   * is enabled, publishing to the system namespace requires an internal credential, and background
   * calls from a task worker or preview runner carrying the worker placeholder internal credential
   * are permitted without calling the access enforcer.
   *
   * @return the id of the namespace
   * @throws IllegalArgumentException if the namespace name is invalid
   * @throws UnauthorizedException if the caller is not allowed to publish to the namespace
   */
  NamespaceId authorizePublish(String namespace) throws AccessException {
    NamespaceId namespaceId = new NamespaceId(namespace);
    Principal principal = authenticationContext.getPrincipal();
    if (internalAuthEnabled && NamespaceId.SYSTEM.equals(namespaceId)) {
      if (!hasInternalCredential(principal)) {
        throw new UnauthorizedException(
            "Only internal services can access topics of the system namespace.");
      }
      if (hasEmptyWorkerCredential(principal)) {
        return namespaceId;
      }
    }
    accessEnforcer.enforce(namespaceId, principal, StandardPermission.UPDATE);
    return namespaceId;
  }

  private static boolean hasInternalCredential(@Nullable Principal principal) {
    if (principal == null) {
      return false;
    }
    Credential credential = principal.getFullCredential();
    return credential != null && credential.getType() == Credential.CredentialType.INTERNAL;
  }

  private static boolean hasEmptyWorkerCredential(@Nullable Principal principal) {
    if (principal == null) {
      return false;
    }
    Credential credential = principal.getFullCredential();
    return credential != null && credential.getType() == Credential.CredentialType.INTERNAL
        && EMPTY_WORKER_CREDENTIAL_VALUE.equals(credential.getValue());
  }
}
