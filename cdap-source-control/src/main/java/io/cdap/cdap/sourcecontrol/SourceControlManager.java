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
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

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
    this.localRepoDirectory = context.getRepoBasePath().toFile();
    this.gitCommandTimeoutSeconds = context.getGitCommandTimeoutSeconds();
    this.validationRepoDirectory = new File("/tmp/" + UUID.randomUUID());
  }

  // Deletes a directory by recursively deleting its contents.
  private boolean deleteDirectory(File dir) {
    File[] allContents = dir.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
    return dir.delete();
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
    if (localRepoDirectory.exists() && !deleteDirectory(localRepoDirectory)) {
      throw new IOException("Failed to clean up local repository directory " + context.getRepoBasePath());
    }
    String branch = "refs/heads/" + context.getRepositoryConfig().getDefaultBranch();
    CloneCommand command = Git.cloneRepository()
      .setURI(context.getRepositoryConfig().getDefaultBranch())
      .setDirectory(localRepoDirectory)
      .setBranchesToClone(Collections.singleton((branch)))
      .setBranch(branch);

    command = getAuthenticatedCommandWithTimeout(command);
    command.call();
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
        return new GitPATAuthStrategy(this.context.getStore());
      }
    }
    throw new AuthStrategyNotFoundException(String.format("No strategy found for provider %s and type %s.",
                                                          config.getProvider(),
                                                          config.getAuth().getType()));
  }

  /**
   * Validated the configuration provided to this manager.
   *
   * @return a {@link List} of {@link  Exception}s occurred during validation.
   */
  public List<Exception> validateConfig() {
    ArrayList<Exception> validationErrors = new ArrayList<>();
    // Validate existence of secret in Secure Store.
    RepositoryConfig config = context.getRepositoryConfig();
    LOG.info("Checking existence of secret in secure store: " + config.getAuth().getTokenName());
    try {
      context.getStore().get(context.getNamespaceId().getNamespace(), config.getAuth().getTokenName()).get();
    } catch (Exception e) {
      validationErrors.add(new Exception("Failed to get password/token from secure store", e));
      return validationErrors;
    }

    // Try fetching heads in the remote repository.
    FileRepository localRepo = null;
    if (validationRepoDirectory.exists() && !deleteDirectory(validationRepoDirectory)) {
      validationErrors.add(new Exception(String.format("Failed to clean up local directory '%s' for temp repository",
                                                       validationRepoDirectory.getAbsolutePath())));
      return validationErrors;
    }
    try {
      localRepo = new FileRepository(validationRepoDirectory);
    } catch (IOException e) {
      validationErrors.add(new Exception(String.format("Failed to create local git repository at path '%s'",
                                                       validationRepoDirectory.getAbsolutePath()), e));
    }

    if (localRepo == null) {
      return validationErrors;
    }

    Collection<Ref> refs = null;
    LOG.info("Created local repo in directory for validation: " + localRepoDirectory.getAbsolutePath());
    try (Git git = new Git(localRepo)) {
      LsRemoteCommand cmd = git.lsRemote().setRemote(config.getLink()).setHeads(true).setTags(false);
      cmd = getAuthenticatedCommandWithTimeout(cmd);
      refs = cmd.call();
    } catch (TransportException e) {
      validationErrors.add(new Exception("Failed to connect with remote repository. " +
                                           "Authentication credentials may be invalid or the repository may not exist.",
                                         e));
    } catch (Exception e) {
      validationErrors.add(e);
    }

    // Check if default branch exists.
    if (refs != null && !Strings.isNullOrEmpty(config.getDefaultBranch())) {
      String defaultBranchRefName = "refs/heads/" + config.getDefaultBranch();
      boolean foundDefaultBranch = false;
      for (Ref ref : refs) {
        if (defaultBranchRefName.equals(ref.getName())) {
          foundDefaultBranch = true;
        }
      }
      if (!foundDefaultBranch) {
        validationErrors.add(new IllegalArgumentException(String.format(
          "Default branch not found in remote repository. Ensure '%s' already exists.",
          config.getDefaultBranch())));
      }
    }
    deleteDirectory(validationRepoDirectory);
    return validationErrors;
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
    return localRepoDirectory.toPath().resolve(context.getRepositoryConfig().getPathPrefix());
  }

  public String getFileHash(Path filePath) {
    validateInitialised();
    // TODO
    return "";
  }

  @Override
  public void close() throws Exception {
    deleteDirectory(this.localRepoDirectory);
    if (this.git != null) {
      this.git.close();
    }
  }
}
