/*
 * Copyright © 2014-2021 Cask Data, Inc.
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

package io.cdap.cdap.security.spi.authentication;

import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import java.util.ArrayDeque;
import java.util.Queue;
import javax.annotation.Nullable;

/**
 * RequestContext that maintains a ThreadLocal {@link #userId} and {@link #userIP} of the
 * authenticated user.
 */
public final class SecurityRequestContext {

  private static final ThreadLocal<String> userId = new InheritableThreadLocal<>();
  private static final ThreadLocal<Credential> userCredential = new InheritableThreadLocal<>();
  private static final ThreadLocal<String> userIP = new InheritableThreadLocal<>();
  private static final ThreadLocal<Queue<AuditLogContext>> auditLogContextQueue = new InheritableThreadLocal<>();

  private SecurityRequestContext() {
  }

  /**
   * Get the userId set on the current thread or null if userId is not set.
   *
   * @return the userId in String
   */
  @Nullable
  public static String getUserId() {
    return userId.get();
  }

  /**
   * Get the user credential set on the current thread or null if user credential is not set.
   *
   * @return the user {@link Credential}
   */
  @Nullable
  public static Credential getUserCredential() {
    return userCredential.get();
  }

  /**
   * Get the userIP set on the current thread or null if userIP is not set.
   *
   * @return the userIP in string
   */
  @Nullable
  public static String getUserIp() {
    return userIP.get();
  }

  /**
   * Set the userId on the current thread.
   *
   * @param userIdParam userId to be set
   */
  public static void setUserId(String userIdParam) {
    userId.set(userIdParam);
  }

  /**
   * Set the user credential on the current thread.
   *
   * @param userCredentialParam user credential to be set
   */
  public static void setUserCredential(@Nullable Credential userCredentialParam) {
    userCredential.set(userCredentialParam);
  }

  /**
   * Set the userIP on the current thread.
   *
   * @param userIpParam userIP to be set
   */
  public static void setUserIp(String userIpParam) {
    userIP.set(userIpParam);
  }

  /**
   * Returns a {@link Principal} for the user set on the current thread.
   */
  public static Principal toPrincipal() {
    return new Principal(userId.get(), Principal.PrincipalType.USER, userCredential.get());
  }

  /**
   * Clears security state for this thread.
   */
  public static void reset() {
    userId.remove();
    userIP.remove();
    userCredential.remove();
    auditLogContextQueue.remove();
  }

  /**
   * Creates a queue if not present and adds the {@link AuditLogContext} to it.
   */
  public static void enqueueAuditLogContext(AuditLogContext auditLog) {
    Queue<AuditLogContext> queue = auditLogContextQueue.get();
    if (queue == null) {
      ArrayDeque<AuditLogContext> newQueue = new ArrayDeque<>();
      newQueue.add(auditLog);
      auditLogContextQueue.set(newQueue);
    } else {
      queue.add(auditLog);
    }
  }

  /**
   * Resets / removes the audit log queue.
   */
  public static void clearAuditLogQueue(AuditLogContext auditLog) {
    auditLogContextQueue.remove();
  }

  /**
   * Get the collection of {@link AuditLogContext}s for this thread.
   * @return AuditLogContexts
   */
  public static Queue<AuditLogContext> getAuditLogQueue() {
    Queue<AuditLogContext> queue = auditLogContextQueue.get();
    if (queue == null) {
      return new ArrayDeque<>();
    }
    return queue;
  }

}
