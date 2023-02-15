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

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigValidationException;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LsRemoteCommand;
import org.eclipse.jgit.api.TransportCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.transport.CredentialsProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * A git repository manager that is responsible for handling interfacing with git. It provides version control
 * operations.
 */
public class RepositoryManager implements AutoCloseable {
  private final SourceControlConfig sourceControlConfig;
  private final RefreshableCredentialsProvider credentialsProvider;
  private Git git;

  public RepositoryManager(SecureStore secureStore, SourceControlConfig sourceControlConfig) {
    this.sourceControlConfig = sourceControlConfig;
    try {
      this.credentialsProvider = new AuthenticationStrategyProvider(sourceControlConfig.getNamespaceID(),
                                                                    secureStore)
        .get(sourceControlConfig.getRepositoryConfig())
        .getCredentialsProvider();
    } catch (AuthenticationStrategyNotFoundException e) {
      // This is not expected as only valid auth configs will be stored.
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the base path in the git repo to store CDAP applications. If an optional Path prefix is provided in the
   * repository configuration, the returned path includes it.
   *
   * @return the path for the repository base directory.
   */
  public Path getBasePath() {
    Path localRepoPath = sourceControlConfig.getLocalRepoPath();
    String pathPrefix = sourceControlConfig.getRepositoryConfig().getPathPrefix();
    if (pathPrefix == null) {
      return localRepoPath;
    }
    return localRepoPath.resolve(pathPrefix);
  }

  /**
   * Validates the provided configuration.
   *
   * @param secureStore         A secure store for fetching credentials if required.
   * @param sourceControlConfig Configuration for source control operations.
   * @throws RepositoryConfigValidationException when provided repository configuration is invalid.
   * @throws IOException                         when there are internal errors while performing network operations
   *                                             such as fetching secrets from Secure stores.
   */
  public static void validateConfig(SecureStore secureStore, SourceControlConfig sourceControlConfig) throws
    RepositoryConfigValidationException, IOException {
    RepositoryConfig config = sourceControlConfig.getRepositoryConfig();
    RefreshableCredentialsProvider credentialsProvider;
    try {
      credentialsProvider = new AuthenticationStrategyProvider(sourceControlConfig.getNamespaceID(),
                                                               secureStore)
        .get(sourceControlConfig.getRepositoryConfig())
        .getCredentialsProvider();
    } catch (AuthenticationStrategyNotFoundException e) {
      throw new RepositoryConfigValidationException(e.getMessage(), e);
    }
    try {
      credentialsProvider.refresh();
    } catch (AuthenticationConfigException e) {
      throw new RepositoryConfigValidationException("Failed to get authentication credentials: " + e.getMessage(), e);
    }
    // Try fetching heads in the remote repository.
    try (Git git = Git.wrap(new InMemoryRepository.Builder().build())) {
      LsRemoteCommand cmd =
        createCommand(git::lsRemote, sourceControlConfig, credentialsProvider).setRemote(config.getLink())
          .setHeads(true)
          .setTags(false);
      validateDefaultBranch(cmd.callAsMap(), config.getDefaultBranch());
    } catch (TransportException e) {
      throw new RepositoryConfigValidationException("Failed to connect with remote repository: " + e.getMessage(), e);
    } catch (GitAPIException e) {
      throw new RepositoryConfigValidationException("Failed to list remotes in remote repository: " + e.getMessage(),
                                                    e);
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, RepositoryConfigValidationException.class);
      throw new RepositoryConfigValidationException("Failed to list remotes in remote repository.", e);
    }
  }

  /**
   * Initializes the Git repository by cloning remote.
   *
   * @throws GitAPIException               when a Git operation fails.
   * @throws IOException                   when file or network I/O fails.
   * @throws AuthenticationConfigException when there is a failure while fetching authentication credentials for Git.
   */
  public void cloneRemote() throws Exception {
    if (git != null) {
      return;
    }
    // Clean up the directory if it already exists.
    deletePathIfExists(sourceControlConfig.getLocalRepoPath());
    RepositoryConfig repositoryConfig = sourceControlConfig.getRepositoryConfig();
    credentialsProvider.refresh();
    CloneCommand command = createCommand(Git::cloneRepository).setURI(repositoryConfig.getLink())
      .setDirectory(sourceControlConfig.getLocalRepoPath().toFile());
    String branch = getBranchRefName(repositoryConfig.getDefaultBranch());
    if (branch != null) {
      command.setBranchesToClone(Collections.singleton((branch))).setBranch(branch);
    }
    git = command.call();
  }

  @Override
  public void close() throws Exception {
    if (git != null) {
      git.close();
    }
    git = null;
    deletePathIfExists(sourceControlConfig.getLocalRepoPath());
  }

  private <C extends TransportCommand> C createCommand(Supplier<C> creator) {
    return createCommand(creator, sourceControlConfig, credentialsProvider);
  }

  private static <C extends TransportCommand> C createCommand(Supplier<C> creator,
                                                              SourceControlConfig sourceControlConfig,
                                                              CredentialsProvider credentialsProvider) {
    C command = creator.get();
    command.setCredentialsProvider(credentialsProvider);
    command.setTimeout(sourceControlConfig.getGitCommandTimeoutSeconds());
    return command;
  }

  /**
   * Returns the ref name for a given branch name.
   *
   * @param branch name without refs/head prefix
   * @return the ref name for the branch or Null if the branch name is null.
   */
  @Nullable
  private static String getBranchRefName(@Nullable String branch) {
    if (Strings.isNullOrEmpty(branch)) {
      return null;
    }
    return "refs/heads/" + branch;
  }

  private static void validateDefaultBranch(Map<String, Ref> refs, @Nullable String defaultBranchName) throws
    RepositoryConfigValidationException {
    // If default branch is not provided, skip validation.
    if (getBranchRefName(defaultBranchName) == null) {
      return;
    }
    // Check if default branch exists.
    if (refs.get(getBranchRefName(defaultBranchName)) == null) {
      throw new RepositoryConfigValidationException(String.format(
        "Default branch not found in remote repository. Ensure branch '%s' already exists.",
        defaultBranchName));
    }
  }

  /**
   * Validates whether git is initialized.
   */
  private void validateInitialized() {
    if (git == null) {
      throw new IllegalStateException("Initialize source control manager before performing operation.");
    }
  }

  private static void deletePathIfExists(Path path) throws IOException {
    if (Files.exists(path)) {
      DirUtils.deleteDirectoryContents(path.toFile());
    }
  }
}
