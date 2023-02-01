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
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryValidationFailure;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LsRemoteCommand;
import org.eclipse.jgit.api.TransportCommand;
import org.eclipse.jgit.api.TransportConfigCallback;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * A Source Control Manager that is responsible for handling interfacing with git. It fetches credentials
 * from a {@link io.cdap.cdap.api.security.store.SecureStore}, builds an {@link AuthStrategy} for different
 * Git hosting services and provides version control operations.
 */
public class SourceControlManager implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SourceControlManager.class);
  private final SourceControlContext context;
  private final AuthStrategy authStrategy;
  private final File localRepoDirectory;
  private final File validationRepoDirectory;
  private final int gitCommandTimeoutSeconds;
  private Git git;

  public SourceControlManager(SourceControlContext context) throws AuthStrategyNotFoundException {
    this.context = context;
    this.authStrategy = getAuthStrategy();
    this.localRepoDirectory = context.getRepoBasePath().resolve("real").toFile();
    this.validationRepoDirectory = context.getRepoBasePath().resolve("validation").toFile();
    this.gitCommandTimeoutSeconds = context.getGitCommandTimeoutSeconds();
  }

  private <C extends TransportCommand> C getAuthenticatedCommandWithTimeout(C command) throws AuthenticationException {
    command.setCredentialsProvider(authStrategy.getCredentialProvider(context));
    Optional<TransportConfigCallback> transport = authStrategy.getTransportConfigCallback(context);
    transport.ifPresent(command::setTransportConfigCallback);
    command.setTimeout(gitCommandTimeoutSeconds);
    return command;
  }

  /**
   * Initialises the Git repository by cloning remote.
   *
   * @throws GitAPIException         when a Git operation fails.
   * @throws AuthenticationException when correct authentication credentials are not available.
   * @throws IOException             when file or network I/O fails.
   */
  public void initialise() throws GitAPIException, AuthenticationException, IOException {
    if (git != null) {
      return;
    }
    // Clean up the directory if it already exists.
    deleteIfRequired(localRepoDirectory);
    CloneCommand command =
      Git.cloneRepository().setURI(context.getRepositoryConfig().getDefaultBranch()).setDirectory(localRepoDirectory);

    String branch = getBranchRefName(context.getRepositoryConfig().getDefaultBranch());
    if (branch != null) {
      command.setBranchesToClone(Collections.singleton((branch))).setBranch(branch);
    }
    command = getAuthenticatedCommandWithTimeout(command);
    command.call();
  }

  // Returns the ref name for the branch or Null if the branch name is null.
  @Nullable
  private String getBranchRefName(@Nullable String branch) {
    if (Strings.isNullOrEmpty(branch)) {
      return null;
    }
    return "refs/heads/" + branch;
  }

  /**
   * Returns an {@link AuthStrategy} for the given Git repository provider and auth type.
   *
   * @return an instance of {@link  AuthStrategy}.
   * @throws AuthStrategyNotFoundException when the corresponding {@link AuthStrategy} is not found.
   */
  private AuthStrategy getAuthStrategy() throws AuthStrategyNotFoundException {
    RepositoryConfig config = this.context.getRepositoryConfig();
    if (config.getProvider() == Provider.GITHUB) {
      if (config.getAuth().getType() == AuthType.PAT) {
        return new GitPATAuthStrategy();
      }
    }
    throw new AuthStrategyNotFoundException(String.format("No strategy found for provider %s and type %s.",
                                                          config.getProvider(),
                                                          config.getAuth().getType()));
  }

  @Nullable
  private RepositoryValidationFailure validateDefaultBranch(Collection<Ref> refs) {
    // If refs are null, i.e. couldn't be fetched or
    if (refs == null) {
      return new RepositoryValidationFailure("No branched found in remote repository");
    }

    String defaultBranchRefName = getBranchRefName(context.getRepositoryConfig().getDefaultBranch());
    // If default branch is not provided, skip validation.
    if (defaultBranchRefName == null) {
      return null;
    }
    // Check if default branch exists.
    String foundDefaultBranch =
      refs.stream().map(Ref::getName).filter(defaultBranchRefName::equals).findAny().orElse(null);
    if (foundDefaultBranch != null) {
      return null;
    }
    return new RepositoryValidationFailure(String.format(
      "Default branch not found in remote repository. Ensure ref '%s' already exists.",
      defaultBranchRefName));
  }

  /**
   * Validated the configuration provided to this manager.
   *
   * @return a {@link List} of {@link  RepositoryValidationFailure}s occurred during validation.
   */
  public RepositoryValidationFailure validateConfig() throws IOException {
    // Validate existence of secret in Secure Store.
    RepositoryConfig config = context.getRepositoryConfig();
    LOG.info("Checking existence of secret in secure store: " + config.getAuth().getTokenName());
    try {
      context.getStore().get(context.getNamespaceId().getNamespace(), config.getAuth().getTokenName()).get();
    } catch (Exception e) {
      LOG.error("Failed to get auth credentials from secure store.", e);
      return new RepositoryValidationFailure("Failed to get password/token from secure store.");
    }

    // Try fetching heads in the remote repository.
    FileRepository localRepo = null;
    deleteIfRequired(validationRepoDirectory);
    try {
      localRepo = new FileRepository(validationRepoDirectory);
    } catch (IOException e) {
      throw new IOException(String.format("Failed to create local git repository at path '%s'",
                                          validationRepoDirectory.getAbsolutePath()), e);
    }

    LOG.info("Created local repo in directory for validation: " + localRepoDirectory.getAbsolutePath());
    try (Git git = new Git(localRepo)) {
      LsRemoteCommand cmd = git.lsRemote().setRemote(config.getLink()).setHeads(true).setTags(false);
      cmd = getAuthenticatedCommandWithTimeout(cmd);
      RepositoryValidationFailure failure = validateDefaultBranch(cmd.call());
      if (failure != null) {
        return failure;
      }
    } catch (TransportException e) {
      LOG.error("Failed to list remotes in remote repository.", e);
      return new RepositoryValidationFailure(
        "Failed to connect with repository. Authentication credentials invalid or" + "the repository may not exist.");
    } catch (Exception e) {
      LOG.error("Failed to list remotes in remote repository.", e);
      return new RepositoryValidationFailure(e.getMessage());
    }
    deleteIfRequired(validationRepoDirectory);
    return null;
  }

  // Validated whether git is initialised.
  private void validateInitialised() {
    if (git == null) {
      throw new IllegalStateException("Initialise source control manager before performing operation.");
    }
  }

  public void push(CommitMeta meta) {
    validateInitialised();
    // TODO: Check status and skip if not required. Add + commit + push.
    //  Fail if conflict.
  }

  /**
   * Returns the currently checked out branch.
   *
   * @return The name of the checked out branch.
   * @throws IOException if unable to get current branch from git.
   */
  public String getCurrentBranch() throws IOException {
    validateInitialised();
    return git.getRepository().getBranch();
  }

  public void switchToCleanBranch(String branchName) {
    // TODO: git fetch origin branch
    // TODO: git reset hard origin branch
  }

  public Path getBasePath() {
    if (context.getRepositoryConfig().getPathPrefix() == null) {
      return localRepoDirectory.toPath();
    }
    return localRepoDirectory.toPath().resolve(context.getRepositoryConfig().getPathPrefix());
  }

  private void deleteIfRequired(File directory) throws IOException {
    if (directory.exists()) {
      DirUtils.deleteDirectoryContents(directory);
    }
  }

  public String getFileHash(Path filePath) {
    validateInitialised();
    // TODO
    return "";
  }

  @Override
  public void close() throws IOException {
    if (this.git != null) {
      this.git.close();
    }
    deleteIfRequired(this.localRepoDirectory);
  }
}
