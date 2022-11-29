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

import javax.annotation.Nullable;

/**
 * RequestContext that maintains a ThreadLocal {@link #userId} and {@link #userIP} of the authenticated user.
 */
public final class SecurityRequestContext {
  /**
   * IMPORTANT NOTE: Currently, SecurityRequestContext variables are only set in AuthenticationChannelHandler.
   *                 All variables here explicitly use ThreadLocal instead of InheritableThreadLocal to prevent the
   *                 Netty HTTP handler threads from passing credentials or other sensitive user contexts to executor
   *                 pools which may cause the SecurityRequestContext to be reused between tasks. See CDAP-20146 for
   *                 details.
   */
  private static final ThreadLocal<String> userId = new ThreadLocal<>();
  private static final ThreadLocal<Credential> userCredential = new ThreadLocal<>();
  private static final ThreadLocal<String> userIP = new ThreadLocal<>();

  private SecurityRequestContext() {
  }

  /**
   * @return the userId set on the current thread or null if userId is not set
   */
  @Nullable
  public static String getUserId() {
    return userId.get();
  }

  /**
   * @return the user credential set on the current thread or null if user credential is not set
   */
  @Nullable
  public static Credential getUserCredential() {
    return userCredential.get();
  }

  /**
   * @return the userIP set on the current thread or null if userIP is not set
   */
  @Nullable
  public static String getUserIP() {
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
   * @param userIPParam userIP to be set
   */
  public static void setUserIP(String userIPParam) {
    userIP.set(userIPParam);
  }

  /**
   * Returns a {@link Principal} for the user set on the current thread
   */
  public static Principal toPrincipal() {
    return new Principal(userId.get(), Principal.PrincipalType.USER, userCredential.get());
  }

  /**
   * Clears security state for this thread
   */
  public static void reset() {
    userId.remove();
    userIP.remove();
    userCredential.remove();
  }
}
