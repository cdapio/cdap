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
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Configuration for Source Control operations performed by {@link RepositoryManager}.
 */
public class SourceControlConfig {
  private final String namespaceID;
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
  }

  public String getNamespaceID() {
    return namespaceID;
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
