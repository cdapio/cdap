/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import io.cdap.cdap.proto.id.NamespaceId;

import javax.annotation.Nullable;

/**
 * Information required for external source control management.
 */
public class SourceControlInfo {
  private final NamespaceId namespaceId;
  private final String repositoryLink;
  @Nullable
  private final String repositoryRootPath;
  private final RepositoryProvider repositoryProvider;
  private final String accessToken;

  public enum RepositoryProvider {
    GITHUB
  }

  public SourceControlInfo(NamespaceId namespaceId, String repositoryLink,
                           @Nullable String repositoryRootPath,
                           RepositoryProvider repositoryProvider, String accessToken) {
    this.namespaceId = namespaceId;
    this.repositoryLink = repositoryLink;
    this.repositoryRootPath = repositoryRootPath;
    this.repositoryProvider = repositoryProvider;
    this.accessToken = accessToken;
  }

  public NamespaceId getNamespaceId() {
    return namespaceId;
  }

  public String getRepositoryLink() {
    return repositoryLink;
  }

  @Nullable
  public String getRepositoryRootPath() {
    return repositoryRootPath;
  }

  public RepositoryProvider getRepositoryProvider() {
    return repositoryProvider;
  }

  public String getAccessToken() {
    return accessToken;
  }

  /**
   * Builder class for the {@link SourceControlInfo}.
   */
  public static final class Builder {
    private NamespaceId namespaceId;
    private String repositoryLink;
    @Nullable
    private String repositoryRootPath;
    private RepositoryProvider repositoryProvider;
    private String accessToken;

    public Builder() {
      // Only for the builder() method to use
    }


    public Builder setNamespaceId(String namespaceId) {
      this.namespaceId = new NamespaceId(namespaceId);
      return this;
    }

    public Builder setRepositoryLink(String repositoryLink) {
      this.repositoryLink = repositoryLink;
      return this;

    }

    public Builder setRepositoryRootPath(@Nullable String repositoryRootPath) {
      this.repositoryRootPath = repositoryRootPath;
      return this;

    }

    public Builder setRepositoryProvider(RepositoryProvider repositoryProvider) {
      this.repositoryProvider = repositoryProvider;
      return this;

    }

    public Builder setAccessToken(String accessToken) {
      this.accessToken = accessToken;
      return this;
    }


    public SourceControlInfo build() {
      if (accessToken == null) {
        throw new IllegalStateException("Missing access token");
      }
      if (repositoryProvider == null) {
        throw new IllegalStateException("Missing repository provider");
      }
      if (repositoryLink == null) {
        throw new IllegalStateException("Missing repository link");
      }
      
      return new SourceControlInfo(namespaceId, repositoryLink, repositoryRootPath, repositoryProvider, accessToken);
    }
  }

}
