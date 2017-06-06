/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package co.cask.cdap.security.spi.authentication;

import co.cask.cdap.proto.security.Principal;

import javax.annotation.Nullable;

/**
 * RequestContext that maintains a ThreadLocal {@link #userId} and {@link #userIP} of the authenticated user.
 */
public final class SecurityRequestContext {
  private static final ThreadLocal<String> userId = new InheritableThreadLocal<>();
  private static final ThreadLocal<String> userIP = new InheritableThreadLocal<>();
  private static final ThreadLocal<Principal.PrincipalType> principalType = new InheritableThreadLocal<>();

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
   * Set the userIP on the current thread.
   *
   * @param userIPParam userIP to be set
   */
  public static void setUserIP(String userIPParam) {
    userIP.set(userIPParam);
  }

  /**
   * @return the principal type set on the current thread or PrincipalType.USER if not set
   */
  public static Principal.PrincipalType getPrincipalType() {
    Principal.PrincipalType type = principalType.get();
    return type == null ? Principal.PrincipalType.USER : type;
  }

  /**
   * Set the principalType on the current thread.
   *
   * @param type principalType to be set
   */
  public static void setPrincipalType(Principal.PrincipalType type) {
    principalType.set(type);
  }

  /**
   * Returns a {@link Principal} for the user set on the current thread
   */
  public static Principal toPrincipal() {
    return new Principal(getUserId(), getPrincipalType());
  }
}
