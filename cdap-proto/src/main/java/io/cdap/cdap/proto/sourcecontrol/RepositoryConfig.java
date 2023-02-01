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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents the repository configuration of a namespace. This class needs to be GSON serializable.
 */
public class RepositoryConfig {
  private final Provider provider;
  private final String link;
  private final String defaultBranch;
  private final String pathPrefix;
  private final AuthConfig auth;

  private RepositoryConfig(Provider provider, String link, @Nullable String defaultBranch,
                           AuthConfig authConfig, @Nullable String pathPrefix) {
    this.provider = provider;
    this.link = link;
    this.defaultBranch = defaultBranch;
    this.auth = authConfig;
    this.pathPrefix = pathPrefix;
  }

  public Provider getProvider() {
    return provider;
  }

  public String getLink() {
    return link;
  }

  @Nullable
  public String getDefaultBranch() {
    return defaultBranch;
  }

  @Nullable
  public String getPathPrefix() {
    return pathPrefix;
  }

  public AuthConfig getAuth() {
    return auth;
  }

  public void validate() {
    Collection<RepositoryValidationFailure> failures = new ArrayList<>();

    if (provider == null) {
      failures.add(new RepositoryValidationFailure("'provider' field must be specified."));
    }

    if (link == null || link.equals("")) {
      failures.add(new RepositoryValidationFailure("'link' field must be specified."));
    }

    if (auth == null) {
      failures.add(new RepositoryValidationFailure("'auth' field must be specified."));
    } else if (!auth.isValid()) {
      failures.add(new RepositoryValidationFailure("'type' and 'tokenName' fields must be specified."));
    }

    if (!failures.isEmpty()) {
      throw new InvalidRepositoryConfigException(failures);
    }
  }

  /**
   * Builder used to build {@link RepositoryConfig}
   */
  public static final class Builder {
    private Provider provider;
    private String link;
    private String defaultBranch;
    private String pathPrefix;
    private AuthType authType;
    private String tokenName;
    private String username;

    public Builder() {
      // no-op
    }

    public Builder(RepositoryConfig repoConfig) {
      this.provider = repoConfig.getProvider();
      this.link = repoConfig.getLink();
      this.defaultBranch = repoConfig.getDefaultBranch();
      this.pathPrefix = repoConfig.getPathPrefix();
      if (repoConfig.getAuth() != null) {
        this.authType = repoConfig.getAuth().getType();
        this.tokenName = repoConfig.getAuth().getTokenName();
        this.username = repoConfig.getAuth().getUsername();
      }
    }

    public Builder setProvider(Provider provider) {
      this.provider = provider;
      return this;
    }

    public Builder setLink(String link) {
      this.link = link;
      return this;
    }

    public Builder setDefaultBranch(String defaultBranch) {
      this.defaultBranch = defaultBranch;
      return this;
    }

    public Builder setPathPrefix(String pathPrefix) {
      this.pathPrefix = pathPrefix;
      return this;
    }

    public Builder setAuthType(AuthType authType) {
      this.authType = authType;
      return this;
    }

    public Builder setTokenName(String tokenName) {
      this.tokenName = tokenName;
      return this;
    }

    public Builder setUsername(String username) {
      this.username = username;
      return this;
    }

    public RepositoryConfig build() {
      RepositoryConfig repoConfig = new RepositoryConfig(provider, link, defaultBranch,
                                                         new AuthConfig(authType, tokenName, username), pathPrefix);
      repoConfig.validate();
      return repoConfig;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RepositoryConfig that = (RepositoryConfig) o;
    return Objects.equals(provider, that.provider) &&
      Objects.equals(link, that.link) &&
      Objects.equals(defaultBranch, that.defaultBranch) &&
      Objects.equals(auth, that.auth) &&
      Objects.equals(pathPrefix, that.pathPrefix);
  }

  @Override
  public int hashCode() {
    return Objects.hash(provider, link, defaultBranch, auth, pathPrefix);
  }

  @Override
  public String toString() {
    return "RepositoryConfig{" +
      "provider=" + provider +
      ", link=" + link +
      ", defaultBranch=" + defaultBranch +
      ", auth=" + auth +
      ", pathPrefix=" + pathPrefix +
      '}';
  }
}
