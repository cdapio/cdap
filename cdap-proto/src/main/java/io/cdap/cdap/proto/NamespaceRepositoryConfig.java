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

package io.cdap.cdap.proto;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents the repository configuration of a namespace. This class needs to be GSON serializable.
 */
public class NamespaceRepositoryConfig {
  private final String provider;
  private final String link;
  private final String defaultBranch;
  private final AuthType authType;
  private final String tokenName;
  private final String username;
  private final String pathPrefix;

  public NamespaceRepositoryConfig(String provider, String link, String defaultBranch, AuthType authType,
                                   String tokenName, @Nullable String username, @Nullable String pathPrefix) {
    this.provider = provider;
    this.link = link;
    this.defaultBranch = defaultBranch;
    this.authType = authType;
    this.tokenName = tokenName;
    this.username = username;
    this.pathPrefix = pathPrefix;
  }

  /**
   * Auth Type Enums.
   */
  public enum AuthType {
    @SerializedName(value = "pat")
    PAT;
  }

  public String getProvider() {
    return provider;
  }

  public String getLink() {
    return link;
  }

  public String getDefaultBranch() {
    return defaultBranch;
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
  
  @Nullable
  public String getPathPrefix() {
    return pathPrefix;
  }

  public boolean isValid() {
    return provider != null && link != null && defaultBranch != null && authType != null && tokenName != null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NamespaceRepositoryConfig that = (NamespaceRepositoryConfig) o;
    return Objects.equals(provider, that.provider) &&
      Objects.equals(link, that.link) &&
      Objects.equals(defaultBranch, that.defaultBranch) &&
      Objects.equals(authType, that.authType) &&
      Objects.equals(tokenName, that.tokenName) &&
      Objects.equals(username, that.username) &&
      Objects.equals(pathPrefix, that.pathPrefix);
  }

  @Override
  public int hashCode() {
    return Objects.hash(provider, link, defaultBranch, authType, tokenName, username, pathPrefix);
  }

  @Override
  public String toString() {
    return "NamespaceRepositoryConfig{" +
      "provider=" + provider +
      ", link=" + link +
      ", defaultBranch=" + defaultBranch +
      ", authType=" + authType +
      ", tokenName=" + tokenName +
      ", username=" + username +
      ", pathPrefix=" + pathPrefix +
      '}';
  }
}
