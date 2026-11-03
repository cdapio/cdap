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
 * except for publishing to the preview topic with a user credential and background publishing from
 * workers to their own system topics.
 */
final class MessagingHandlerAuthorizer {

  private static final String SYSTEM_NAMESPACE_INTERNAL_ONLY_MESSAGE =
      "Only internal services can access topics of the system namespace.";
  private static final String WORKER_SYSTEM_TOPIC_ONLY_MESSAGE =
      "Workers can only publish to their own topics of the system namespace.";

  private final boolean internalAuthEnabled;
  @Nullable
  private final String previewTopic;
  private final Set<String> workerSystemTopics;
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  MessagingHandlerAuthorizer(CConfiguration cConf, AccessEnforcer accessEnforcer,
      AuthenticationContext authenticationContext) {
    this.internalAuthEnabled = cConf.getBoolean(Constants.Security.INTERNAL_AUTH_ENABLED);
    this.previewTopic = Strings.emptyToNull(cConf.get(Constants.Preview.MESSAGING_TOPIC));
    this.workerSystemTopics = buildWorkerSystemTopics(cConf, this.previewTopic);
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
   */
  NamespaceId authorize(String namespace, StandardPermission permission) throws AccessException {
    NamespaceId namespaceId = new NamespaceId(namespace);
    Principal principal = authenticationContext.getPrincipal();
    if (internalAuthEnabled && isSystemNamespace(namespaceId)
        && (!hasInternalCredential(principal) || hasPlaceholderCredential(principal))) {
      throw new UnauthorizedException(SYSTEM_NAMESPACE_INTERNAL_ONLY_MESSAGE);
    }
    if (internalAuthEnabled && hasPlaceholderCredential(principal)) {
      throw new UnauthorizedException(principal, Collections.singleton(permission), namespaceId);
    }
    accessEnforcer.enforce(namespaceId, principal, permission);
    return namespaceId;
  }

  /**
   * Authorizes the caller to publish messages to a topic in the given namespace.
   *
   * <p>Requires {@link StandardPermission#UPDATE} on the namespace. When internal authentication
   * is enabled, publishing to the system namespace is permitted without calling the access
   * enforcer in two cases:
   * <ul>
   *   <li>Background calls from a task worker or preview runner carrying the worker placeholder
   *       internal credential, when the target topic is one of the worker system topics.
   *   <li>Calls from a preview runner on a preview request thread carrying the user's non-internal
   *       credential, when the target topic is the configured preview topic.
   * </ul>
   * All other callers publishing to the system namespace must carry a valid internal credential
   * and pass the access enforcer.
   *
   * @return the id of the namespace
   * @throws IllegalArgumentException if the namespace name is invalid
   * @throws UnauthorizedException if the caller is not allowed to publish to the topic
   */
  NamespaceId authorizePublish(String namespace, String topic) throws AccessException {
    NamespaceId namespaceId = new NamespaceId(namespace);
    Principal principal = authenticationContext.getPrincipal();
    if (!internalAuthEnabled || !isSystemNamespace(namespaceId)) {
      if (internalAuthEnabled && hasPlaceholderCredential(principal)) {
        throw new UnauthorizedException(
            principal, Collections.singleton(StandardPermission.UPDATE), namespaceId);
      }
      accessEnforcer.enforce(namespaceId, principal, StandardPermission.UPDATE);
      return namespaceId;
    }

    if (hasEmptyWorkerCredential(principal)) {
      if (!workerSystemTopics.contains(topic)) {
        throw new UnauthorizedException(WORKER_SYSTEM_TOPIC_ONLY_MESSAGE);
      }
      return namespaceId;
    }
    if (!hasInternalCredential(principal)) {
      if (previewTopic != null && previewTopic.equals(topic) && hasCredential(principal)) {
        return namespaceId;
      }
      throw new UnauthorizedException(SYSTEM_NAMESPACE_INTERNAL_ONLY_MESSAGE);
    }
    if (hasPlaceholderCredential(principal)) {
      throw new UnauthorizedException(SYSTEM_NAMESPACE_INTERNAL_ONLY_MESSAGE);
    }
    accessEnforcer.enforce(namespaceId, principal, StandardPermission.UPDATE);
    return namespaceId;
  }

  /**
   * Compares the namespace name case-insensitively against {@code "system"} so that mixed-case
   * spellings (e.g. {@code "SYSTEM"} or {@code "System"}) cannot bypass the system-namespace gate
   * on case-insensitive storage backends such as Cloud Spanner.
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
    topics.add(Constants.Preview.LOG_TOPIC);
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

  @Nullable
  private static Credential credentialOf(@Nullable Principal principal) {
    return principal == null ? null : principal.getFullCredential();
  }

  private static boolean hasCredential(@Nullable Principal principal) {
    Credential credential = credentialOf(principal);
    return credential != null && credential.getType() != null
        && !Strings.isNullOrEmpty(credential.getValue());
  }

  private static boolean hasInternalCredential(@Nullable Principal principal) {
    Credential credential = credentialOf(principal);
    return credential != null && credential.getType() == Credential.CredentialType.INTERNAL;
  }

  private static boolean hasEmptyWorkerCredential(@Nullable Principal principal) {
    Credential credential = credentialOf(principal);
    return credential != null && credential.getType() == Credential.CredentialType.INTERNAL
        && Constants.Security.Authentication.EMPTY_WORKER_CREDENTIAL.equals(credential.getValue());
  }

  /**
   * Returns {@code true} if the principal carries one of the internal placeholder credentials
   * ({@link Constants.Security.Authentication#EMPTY_WORKER_CREDENTIAL} or
   * {@link Constants.Security.Authentication#EMPTY_USER_CREDENTIAL}) that are not signed access
   * tokens and must be rejected with 403 before reaching {@code InternalAccessEnforcer}.
   */
  private static boolean hasPlaceholderCredential(@Nullable Principal principal) {
    Credential credential = credentialOf(principal);
    if (credential == null || credential.getType() != Credential.CredentialType.INTERNAL) {
      return false;
    }
    String value = credential.getValue();
    return Constants.Security.Authentication.EMPTY_WORKER_CREDENTIAL.equals(value)
        || Constants.Security.Authentication.EMPTY_USER_CREDENTIAL.equals(value);
  }
}
