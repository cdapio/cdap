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

package io.cdap.cdap.proto.sourcecontrol;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Auth Configuration for the linked repository
 */
public class AuthConfig {
  private final AuthType authType;
  private final String tokenName;
  private final String username;

  public AuthConfig(AuthType authType, String tokenName, @Nullable String username) {
    this.authType = authType;
    this.tokenName = tokenName;
    this.username = username;
  }

  public AuthType getAuthType() {
    return authType;
  }

  public String getTokenName() {
    return tokenName;
  }

  @Nullable
  public String getUsername() {
    return username;
  }

  public boolean isValid() {
    return authType != null && tokenName != null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuthConfig that = (AuthConfig) o;
    return Objects.equals(authType, that.authType) &&
      Objects.equals(tokenName, that.tokenName) &&
      Objects.equals(username, that.username);
  }

  @Override
  public int hashCode() {
    return Objects.hash(authType, tokenName, username);
  }

  @Override
  public String toString() {
    return "AuthConfig{" +
      "authType=" + authType +
      ", tokenName=" + tokenName +
      ", username=" + username +
      '}';
  }
}
