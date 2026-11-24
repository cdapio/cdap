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

package io.cdap.cdap.security.spi.authentication;

import java.util.Objects;

/**
 * Represents a cryptographically verified identity extracted from a CDAP access token.
 */
public final class ValidatedTokenIdentity {

  private final String username;
  private final boolean internal;

  /**
   * Creates a verified token identity.
   *
   * @param username verified principal username embedded in the token
   * @param internal {@code true} if the token represents an internal system identity, {@code false}
   *     for an external user identity
   */
  public ValidatedTokenIdentity(String username, boolean internal) {
    if (username == null) {
      throw new IllegalArgumentException("Username cannot be null");
    }
    this.username = username;
    this.internal = internal;
  }

  public String getUsername() {
    return username;
  }

  public boolean isInternal() {
    return internal;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ValidatedTokenIdentity)) {
      return false;
    }
    ValidatedTokenIdentity that = (ValidatedTokenIdentity) o;
    return internal == that.internal && Objects.equals(username, that.username);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, internal);
  }

  @Override
  public String toString() {
    return "ValidatedTokenIdentity{"
        + "username='" + username + '\''
        + ", internal=" + internal
        + '}';
  }
}
