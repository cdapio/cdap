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
import com.google.common.util.concurrent.AbstractIdleService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigValidationException;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LsRemoteCommand;
import org.eclipse.jgit.api.PushCommand;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.TransportCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * A git repository manager that is responsible for handling interfacing with git. It fetches credentials
 * from a {@link io.cdap.cdap.api.security.store.SecureStore}, builds an {@link AuthenticationStrategy} for different
 * Git hosting services and provides version control operations.
 */
public class RepositoryManager extends AbstractIdleService {
  private final SecureStore secureStore;
  private final SourceControlConfig sourceControlConfig;
  private Git git;

  public RepositoryManager(SecureStore secureStore, SourceControlConfig sourceControlConfig) {
    this.secureStore = secureStore;
    this.sourceControlConfig = sourceControlConfig;
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
   */
  public static void validateConfig(SecureStore secureStore, SourceControlConfig sourceControlConfig) throws
    RepositoryConfigValidationException {
    RepositoryConfig config = sourceControlConfig.getRepositoryConfig();
    // Try fetching heads in the remote repository.
    try (Git git = Git.wrap(new InMemoryRepository.Builder().build())) {
      LsRemoteCommand cmd = createCommand(git::lsRemote, sourceControlConfig, secureStore).setRemote(config.getLink())
        .setHeads(true)
        .setTags(false);
      validateDefaultBranch(cmd.callAsMap(), config.getDefaultBranch());
    } catch (TransportException e) {
      throw new RepositoryConfigValidationException(
        "Failed to connect with repository. Authentication credentials invalid or the repository may not exist.",
        e);
    } catch (RepositoryConfigValidationException e) {
      throw e;
    } catch (AuthenticationConfigException e) {
      throw new RepositoryConfigValidationException(e.getMessage(), e);
    } catch (Exception e) {
      throw new RepositoryConfigValidationException("Failed to list remotes in remote repository.", e);
    }
  }

  public String getFileHash(Path filePath) throws IOException, NotFoundException {
    return "";
  }

  /**
   * Commits and pushes all changes under the repositiry base path
   * @param commitMeta Details for the commit including author, committer and commit message
   * @throws UnexpectedRepositoryChangesException when there are changes outside the repository base path
   * @throws GitAPIException when the underlying git commands fail
   * @throws AuthenticationConfigException when correct authentication credentials are not available.
   */
  public void commitAndPush(CommitMeta commitMeta) throws UnexpectedRepositoryChangesException,
    GitAPIException, AuthenticationConfigException {
    validateInitialized();

    // if the status is clean skip
    Status preStageStatus = git.status().call();
    if (preStageStatus.isClean()) {
      return;
    }

    // We should only add files inside the base path. There should not be any change outside the base path.
    git.add().addFilepattern(getBasePath().toString()).call();

    // Fail if the status is not clean i.e. there are changes outside base path.
    Status postStageStatus = git.status().call();
    if (!postStageStatus.isClean()) {
      throw new UnexpectedRepositoryChangesException(String.format("Unexpected file changes outside base dir %s",getBasePath()));
    }

    getCommitCommand(commitMeta).call();
    PushCommand pushCommand = createCommand(git::push, sourceControlConfig, secureStore);
    pushCommand.call();
  }

  private CommitCommand getCommitCommand(CommitMeta commitMeta){
    // We only set email
    PersonIdent author = new PersonIdent("", commitMeta.getAuthor());
    PersonIdent authorWithDate = new PersonIdent(author, new Date(commitMeta.getTimestampMillis()));

    PersonIdent committer = new PersonIdent("", commitMeta.getCommiter());

    return git.commit().setAuthor(authorWithDate).setCommitter(committer).setMessage(commitMeta.getMessage());
  }


  /**
   * Initializes the Git repository by cloning remote.
   *
   * @throws GitAPIException               when a Git operation fails.
   * @throws AuthenticationConfigException when correct authentication credentials are not available.
   * @throws IOException                   when file or network I/O fails.
   */
  @Override
  protected void startUp() throws Exception {
    if (git != null) {
      return;
    }
    // Clean up the directory if it already exists.
    deletePathIfExists(sourceControlConfig.getLocalRepoPath());
    RepositoryConfig repositoryConfig = sourceControlConfig.getRepositoryConfig();
    CloneCommand command = createCommand(Git::cloneRepository).setURI(repositoryConfig.getLink())
      .setDirectory(sourceControlConfig.getLocalRepoPath()
                      .toFile());

    String branch = getBranchRefName(repositoryConfig.getDefaultBranch());
    if (branch != null) {
      command.setBranchesToClone(Collections.singleton((branch))).setBranch(branch);
    }
    git = command.call();
  }

  @Override
  protected void shutDown() throws Exception {
    if (git != null) {
      git.close();
    }
    git = null;
    deletePathIfExists(sourceControlConfig.getLocalRepoPath());
  }

  private <C extends TransportCommand> C createCommand(Supplier<C> creator) throws AuthenticationConfigException {
    return createCommand(creator, sourceControlConfig, secureStore);
  }

  private static <C extends TransportCommand> C createCommand(Supplier<C> creator,
                                                              SourceControlConfig sourceControlConfig,
                                                              SecureStore secureStore) throws
    AuthenticationConfigException {
    C command = creator.get();
    command.setCredentialsProvider(sourceControlConfig.getAuthenticationStrategy()
                                     .getCredentialsProvider(secureStore,
                                                             sourceControlConfig.getRepositoryConfig(),
                                                             sourceControlConfig.getNamespaceID()));
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
    if (defaultBranchName == null) {
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
