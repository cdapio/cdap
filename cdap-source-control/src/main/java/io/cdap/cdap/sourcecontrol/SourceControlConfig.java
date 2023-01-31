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

package io.cdap.cdap.sourcecontrol;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Configuration for Source Control operations performed by {@link RepositoryManager}.
 */
public class SourceControlConfig {
  private final String namespaceID;
  private final AuthenticationStrategy authenticationStrategy;
  // Path where local git repository is stored.
  private final Path localRepoPath;
  private final int gitCommandTimeoutSeconds;
  private final RepositoryConfig repositoryConfig;

  public SourceControlConfig(NamespaceId namespaceID, RepositoryConfig repositoryConfig, CConfiguration cConf) {
    this.namespaceID = namespaceID.getNamespace();
    this.repositoryConfig = repositoryConfig;
    String gitCloneDirectory = cConf.get(Constants.SourceControlManagement.GIT_REPOSITORIES_CLONE_DIRECTORY_PATH);
    this.gitCommandTimeoutSeconds = cConf.getInt(Constants.SourceControlManagement.GIT_COMMAND_TIMEOUT_SECONDS);
    this.localRepoPath = Paths.get(gitCloneDirectory, "namespace", this.namespaceID);
    try {
      this.authenticationStrategy = getAuthStrategy();
    } catch (AuthenticationStrategyNotFoundException e) {
      //  This exception isn't expected as the API handlers only store auth config after validations,
      throw new RuntimeException(e);
    }
  }

  public String getNamespaceID() {
    return namespaceID;
  }

  public AuthenticationStrategy getAuthenticationStrategy() {
    return authenticationStrategy;
  }

  public Path getLocalRepoPath() {
    return localRepoPath;
  }

  public int getGitCommandTimeoutSeconds() {
    return gitCommandTimeoutSeconds;
  }

  public RepositoryConfig getRepositoryConfig() {
    return repositoryConfig;
  }

  /**
   * Returns an {@link AuthenticationStrategy} for the given Git repository provider and auth type.
   *
   * @return an instance of {@link  AuthenticationStrategy}.
   * @throws AuthenticationStrategyNotFoundException when the corresponding {@link AuthenticationStrategy} is not found.
   */
  private AuthenticationStrategy getAuthStrategy() throws AuthenticationStrategyNotFoundException {
    RepositoryConfig config = this.repositoryConfig;
    if (config.getProvider() == Provider.GITHUB) {
      if (config.getAuth().getType() == AuthType.PAT) {
        return new GitPATAuthenticationStrategy();
      }
    }
    throw new AuthenticationStrategyNotFoundException(String.format("No strategy found for provider %s and type %s.",
                                                                    config.getProvider(),
                                                                    config.getAuth().getType()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SourceControlConfig)) {
      return false;
    }
    SourceControlConfig that = (SourceControlConfig) o;
    return gitCommandTimeoutSeconds == that.gitCommandTimeoutSeconds && namespaceID.equals(that.namespaceID) &&
      localRepoPath.equals(that.localRepoPath) && repositoryConfig.equals(that.repositoryConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceID, localRepoPath, gitCommandTimeoutSeconds, repositoryConfig);
  }
}
