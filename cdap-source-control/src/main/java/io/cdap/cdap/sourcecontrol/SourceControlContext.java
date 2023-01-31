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

import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;

import java.io.File;
import java.nio.file.Path;

public class SourceControlContext {
  // Default value for git timeout.
  private static final int GIT_COMMAND_TIMEOUT_SECONDS_DEFAULT = 30;
  // Default value for git repo
  private static final String GIT_REPOSITORIES_CLONE_DIRECTORY_PATH_DEFAULT = "/tmp/source-control/";
  private final SecureStore store;
  private final Path repoBasePath;
  private final Path validationRepoPath;
  private final RepositoryConfig repositoryConfig;

  private final int gitCommandTimeoutSeconds;
  private final NamespaceId namespaceId;

  public SourceControlContext(SecureStore store,
                              RepositoryConfig repositoryConfig,
                              NamespaceId namespaceId,
                              CConfiguration cConf) {
    this.store = store;
    this.repositoryConfig = repositoryConfig;
    this.namespaceId = namespaceId;
    this.gitCommandTimeoutSeconds =
      cConf.getInt(Constants.SourceControl.GIT_COMMAND_TIMEOUT_SECONDS, GIT_COMMAND_TIMEOUT_SECONDS_DEFAULT);
    String gitCloneDirectory = cConf.get(Constants.SourceControl.GIT_REPOSITORIES_CLONE_DIRECTORY_PATH,
                                         GIT_REPOSITORIES_CLONE_DIRECTORY_PATH_DEFAULT);
    Path commonPath = new File(gitCloneDirectory).toPath().resolve("namespace").resolve(namespaceId.getNamespace());
    this.repoBasePath = commonPath.resolve("actual");
    this.validationRepoPath = commonPath.resolve("validate");
  }

  public SecureStore getStore() {
    return store;
  }

  public Path getRepoBasePath() {
    return repoBasePath;
  }

  public RepositoryConfig getRepositoryConfig() {
    return repositoryConfig;
  }

  public NamespaceId getNamespaceId() {
    return namespaceId;
  }

  public Path getValidationRepoPath() {
    return validationRepoPath;
  }

  public int getGitCommandTimeoutSeconds() {
    return gitCommandTimeoutSeconds;
  }
}
