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

import com.google.common.base.Strings;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.AuthenticationChannelHandler;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Authorizes requests to the messaging HTTP handlers on the namespace of the target topic.
 *
 * <p>Topics are authorized at the namespace level rather than per-topic. When internal
 * authentication is enabled, only internal callers can access the topics of the system namespace,
 * except for workers publishing to their own system topics.
 */
final class MessagingHandlerAuthorizer {

  private static final String SYSTEM_NAMESPACE_INTERNAL_ONLY_MESSAGE =
      "Only internal services can access topics of the system namespace.";
  private static final String WORKER_SYSTEM_TOPIC_ONLY_MESSAGE =
      "Workers can only publish to their own topics of the system namespace.";
  // Placeholder credential set by WorkerAuthenticationContext.
  private static final String EMPTY_WORKER_CREDENTIAL = "empty-worker-user-credential";
  // Preview log topic, set by PreviewConfigModule.
  private static final String PREVIEW_LOG_TOPIC = "previewlog0";

  private final boolean internalAuthEnabled;
  private final Set<String> workerSystemTopics;
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  MessagingHandlerAuthorizer(CConfiguration cConf, AccessEnforcer accessEnforcer,
      AuthenticationContext authenticationContext) {
    this.internalAuthEnabled = cConf.getBoolean(Constants.Security.INTERNAL_AUTH_ENABLED);
    this.workerSystemTopics = buildWorkerSystemTopics(cConf,
        Strings.emptyToNull(cConf.get(Constants.Preview.MESSAGING_TOPIC)));
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
  }

  /**
   * Authorizes the caller to perform the given permission on the topics of a namespace.
   *
   * <p>Every endpoint other than {@link StoreHandler#publish} (which uses
   * {@link #authorizePublish}) calls this first, so that a caller without permission gets the same
   * answer whether or not the topic exists and whether or not the rest of its request is valid.
   *
   * @return the id of the namespace
   * @throws IllegalArgumentException if the namespace name is invalid
   * @throws UnauthorizedException if the caller is not allowed to perform the permission
   * @throws AccessException if the access enforcer fails to check the caller
   */
  NamespaceId authorize(String namespace, StandardPermission permission) throws AccessException {
    NamespaceId namespaceId = new NamespaceId(namespace);
    authorize(namespaceId, authenticationContext.getPrincipal(), permission);
    return namespaceId;
  }

  private void authorize(NamespaceId namespaceId, @Nullable Principal principal,
      StandardPermission permission) throws AccessException {
    if (internalAuthEnabled && isSystemNamespace(namespaceId)
        && (!hasInternalCredential(principal) || hasPlaceholderCredential(principal))) {
      throw new UnauthorizedException(SYSTEM_NAMESPACE_INTERNAL_ONLY_MESSAGE);
    }
    if (internalAuthEnabled && hasPlaceholderCredential(principal)) {
      throw new UnauthorizedException(
          principal, Collections.singleton(permission), namespaceId, true, false);
    }
    accessEnforcer.enforce(namespaceId, principal, permission);
  }

  /**
   * Authorizes the caller to publish messages to a topic in the given namespace.
   *
   * <p>Requires {@link StandardPermission#UPDATE}, like {@link #authorize(String,
   * StandardPermission)}. When internal authentication is enabled, task workers and preview
   * runners, which carry the worker placeholder credential, may also publish to the worker system
   * topics without a signed internal token or an access enforcer check.
   *
   * @return the id of the namespace
   * @throws IllegalArgumentException if the namespace name is invalid
   * @throws UnauthorizedException if the caller is not allowed to publish to the topic
   * @throws AccessException if the access enforcer fails to check the caller
   */
  NamespaceId authorizePublish(String namespace, String topic) throws AccessException {
    NamespaceId namespaceId = new NamespaceId(namespace);
    Principal principal = authenticationContext.getPrincipal();
    if (internalAuthEnabled && isSystemNamespace(namespaceId)
        && hasEmptyWorkerCredential(principal)) {
      if (!workerSystemTopics.contains(topic)) {
        throw new UnauthorizedException(WORKER_SYSTEM_TOPIC_ONLY_MESSAGE);
      }
      return namespaceId;
    }
    authorize(namespaceId, principal, StandardPermission.UPDATE);
    return namespaceId;
  }

  /**
   * Compares the namespace name case-insensitively against {@code "system"}, because the Spanner
   * messaging tables are named after the namespace and Spanner table names are case-insensitive.
   */
  private static boolean isSystemNamespace(NamespaceId namespaceId) {
    return NamespaceId.SYSTEM.getNamespace().equalsIgnoreCase(namespaceId.getNamespace());
  }

  private static Set<String> buildWorkerSystemTopics(CConfiguration cConf,
      @Nullable String previewTopic) {
    Set<String> topics = new HashSet<>();
    addNumberedTopics(topics, cConf.get(Constants.Metrics.TOPIC_PREFIX),
        cConf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM));
    addNumberedTopics(topics, cConf.get(Constants.AuditLogging.AUDIT_LOG_EVENT_TOPIC),
        cConf.getInt(Constants.AuditLogging.AUDIT_LOG_EVENT_NUM_PARTITIONS));
    topics.add(PREVIEW_LOG_TOPIC);
    if (previewTopic != null) {
      topics.add(previewTopic);
    }
    return Collections.unmodifiableSet(topics);
  }

  private static void addNumberedTopics(Set<String> topics, @Nullable String prefix, int count) {
    if (Strings.isNullOrEmpty(prefix)) {
      return;
    }
    for (int i = 0; i < count; i++) {
      topics.add(prefix + i);
    }
  }

  private static boolean hasInternalCredential(@Nullable Principal principal) {
    Credential credential = principal == null ? null : principal.getFullCredential();
    return credential != null && credential.getType() == Credential.CredentialType.INTERNAL
        && !Strings.isNullOrEmpty(credential.getValue());
  }

  private static boolean hasEmptyWorkerCredential(@Nullable Principal principal) {
    return hasInternalCredential(principal)
        && EMPTY_WORKER_CREDENTIAL.equals(principal.getCredential());
  }

  /**
   * Returns {@code true} for the internal placeholder credentials, which are not signed access
   * tokens and are rejected with 403 before reaching the access enforcer.
   */
  private static boolean hasPlaceholderCredential(@Nullable Principal principal) {
    return hasInternalCredential(principal)
        && (EMPTY_WORKER_CREDENTIAL.equals(principal.getCredential())
            || AuthenticationChannelHandler.EMPTY_USER_CREDENTIAL.getValue()
                .equals(principal.getCredential()));
  }
}
