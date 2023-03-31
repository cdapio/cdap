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

  private final String namespaceId;
  // Path where local git repositories are stored.
  private final Path localReposClonePath;
  private final int gitCommandTimeoutSeconds;
  private final RepositoryConfig repositoryConfig;

  public SourceControlConfig(NamespaceId namespaceId, RepositoryConfig repositoryConfig,
      CConfiguration cConf) {
    this.namespaceId = namespaceId.getNamespace();
    this.repositoryConfig = repositoryConfig;
    this.gitCommandTimeoutSeconds = cConf.getInt(
        Constants.SourceControlManagement.GIT_COMMAND_TIMEOUT_SECONDS);
    // if local dir is set use it for cloned storage
    // for task workers it would use emptydir volume
    String defaultCloneDir = cConf.get(Constants.SourceControlManagement.GIT_REPOSITORIES_CLONE_DIRECTORY_PATH);
    String workDir = cConf.get(Constants.TaskWorker.WORK_DIR);
    String gitCloneDirectory = workDir == null
        ? defaultCloneDir : String.format("%s/source-control", workDir);
    this.localReposClonePath = Paths.get(gitCloneDirectory, "namespace", this.namespaceId);
  }

  public String getNamespaceId() {
    return namespaceId;
  }

  public Path getLocalReposClonePath() {
    return localReposClonePath;
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
    return gitCommandTimeoutSeconds == that.gitCommandTimeoutSeconds && namespaceId.equals(
        that.namespaceId)
        && localReposClonePath.equals(that.localReposClonePath) && repositoryConfig.equals(
        that.repositoryConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceId, localReposClonePath, gitCommandTimeoutSeconds,
        repositoryConfig);
  }
}
