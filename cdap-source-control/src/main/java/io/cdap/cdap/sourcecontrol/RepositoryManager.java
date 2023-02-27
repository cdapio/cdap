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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RemoteRepositoryValidationException;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigValidationException;
import io.cdap.cdap.sourcecontrol.operationrunner.PushFailureException;
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
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.PushResult;
import org.eclipse.jgit.transport.RemoteRefUpdate;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * A git repository manager that is responsible for handling interfacing with git. It provides version control
 * operations. This is not thread safe.
 */
public class RepositoryManager implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(RepositoryManager.class);
  private final SourceControlConfig sourceControlConfig;
  private final RefreshableCredentialsProvider credentialsProvider;
  private Git git;
  // We clone the repository inside a randomly named directory to prevent multiple repository managers for the
  // same namespace from interfering with each other.
  private final String randomDirectoryName;

  public RepositoryManager(SecureStore secureStore, CConfiguration cConf, NamespaceId namespace,
                           RepositoryConfig repoConfig) {
    this.sourceControlConfig = new SourceControlConfig(namespace, repoConfig, cConf);
    try {
      this.credentialsProvider = new AuthenticationStrategyProvider(namespace.getNamespace(), secureStore)
        .get(repoConfig)
        .getCredentialsProvider();
    } catch (AuthenticationStrategyNotFoundException e) {
      // This is not expected as only valid auth configs will be stored.
      throw new RuntimeException(e);
    }
    this.randomDirectoryName = UUID.randomUUID().toString();
  }

  /**
   * Returns the base path in the git repo to store CDAP applications. If an optional Path prefix is provided in the
   * repository configuration, the returned path includes it.
   *
   * @return the path for the repository base directory.
   */
  public Path getBasePath() {
    Path localRepoPath = getRepositoryRoot();
    String pathPrefix = sourceControlConfig.getRepositoryConfig().getPathPrefix();
    if (Strings.isNullOrEmpty(pathPrefix)) {
      return localRepoPath;
    }
    return localRepoPath.resolve(pathPrefix);
  }

  /**
   * Gets the relative path of a file in git repository based on the user configured path prefix.
   * @param fileName The filename
   * @return the relative {@link Path}
   */
  public Path getFileRelativePath(String fileName) {
    String pathPrefix = sourceControlConfig.getRepositoryConfig().getPathPrefix();
    if (Strings.isNullOrEmpty(pathPrefix)) {
      return Paths.get(fileName);
    }
    return Paths.get(pathPrefix, fileName);
  }

  /**
   * Validates the provided configuration.
   *
   * @param secureStore         A secure store for fetching credentials if required.
   * @param sourceControlConfig Configuration for source control operations.
   * @throws RepositoryConfigValidationException when provided repository configuration is invalid.
   */
  public static void validateConfig(SecureStore secureStore, SourceControlConfig sourceControlConfig)
    throws RemoteRepositoryValidationException {
    RepositoryConfig config = sourceControlConfig.getRepositoryConfig();
    RefreshableCredentialsProvider credentialsProvider;
    try {
      credentialsProvider = new AuthenticationStrategyProvider(sourceControlConfig.getNamespaceID(), secureStore)
        .get(sourceControlConfig.getRepositoryConfig())
        .getCredentialsProvider();
    } catch (AuthenticationStrategyNotFoundException e) {
      throw new RepositoryConfigValidationException(e.getMessage(), e);
    }
    try {
      credentialsProvider.refresh();
    } catch (AuthenticationConfigException e) {
      throw new RepositoryConfigValidationException("Failed to get authentication credentials: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new RemoteRepositoryValidationException("Internal error: " + e.getMessage(), e);
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
      throw new RemoteRepositoryValidationException("Failed to list remotes in remote repository.", e);
    }
  }

  /**
   * Commits and pushes the changes of a given file under the repository root path.
   *
   * @param commitMeta Details for the commit including author, committer and commit message
   * @param fileChanged The relative path to repository root where the file is updated
   * @throws GitAPIException                      when the underlying git commands fail
   * @throws NoChangesToPushException             when there's no file changes for the commit
   * @return the hash of the written file. It returns null if the push succeeds but failed to get the fileHash from
   * pushed {@link RevCommit}
   */
  @Nullable
  public String commitAndPush(CommitMeta commitMeta, Path fileChanged)
    throws NoChangesToPushException, GitAPIException, PushFailureException {
    validateInitialized();

    // if the status is clean skip
    Status preStageStatus = git.status().call();
    if (preStageStatus.isClean()) {
      throw new NoChangesToPushException("No changes have been maid for the applications to push.");
    }

    git.add().addFilepattern(fileChanged.toString()).call();

    RevCommit commit = getCommitCommand(commitMeta).call();

    PushCommand pushCommand = createCommand(git::push, sourceControlConfig, credentialsProvider);
    Iterable<PushResult> pushResults = pushCommand.call();
    
    for (PushResult result : pushResults) {
      for (RemoteRefUpdate rru : result.getRemoteUpdates()) {
        if (rru.getStatus() != RemoteRefUpdate.Status.OK && rru.getStatus() != RemoteRefUpdate.Status.UP_TO_DATE) {
          throw new PushFailureException(String.format("Push failed for %s: %s", fileChanged, rru.getStatus()));
        }
      }
    }

    try {
      TreeWalk walk = TreeWalk.forPath(git.getRepository(), fileChanged.toString(), commit.getTree());
      return walk.getObjectId(0).getName();
    } catch (IOException e) {
      LOG.warn(String.format("Failed to get the fileHash for file: %s", fileChanged), e);
      return null;
    }
  }

  private CommitCommand getCommitCommand(CommitMeta commitMeta) {
    // We only set email
    PersonIdent author = new PersonIdent(commitMeta.getAuthor(), "");
    PersonIdent authorWithDate = new PersonIdent(author, new Date(commitMeta.getTimestampMillis()));

    PersonIdent committer = new PersonIdent(commitMeta.getCommitter(), "");

    return git.commit().setAuthor(authorWithDate).setCommitter(committer).setMessage(commitMeta.getMessage());
  }


  /**
   * Returns the <a href="https://git-scm.com/docs/git-hash-object">Git Hash</a>
   * of the requested file path in the provided commit. For symlinks, it returns the hash of
   * the target file. This ensures that the file hash of a symlink is equal to the hash of the target file. It
   * doesn't return the hash for directories. If the directory contains symlinks, we can't depend on the
   * directory hash to ensure the contents are unchanged.
   *
   * @param relativePath The path relative to the repository base path (returned by
   *                     {@link RepositoryManager#getRepositoryRoot()}) on the filesystem.
   * @param commitHash   The commit ID hash for which to get the file hash.
   * @return The git file hash of the requested file.
   * @throws IOException              when file or commit isn't found, there are circular symlinks or other file IO
   *                                  errors.
   * @throws IllegalArgumentException when the provided file isn't a regular file, eg: directory.
   * @throws NotFoundException        when the file isn't committed to Git.
   * @throws IllegalStateException    when {@link RepositoryManager#cloneRemote()} isn't called before this.
   */
  public String getFileHash(Path relativePath, String commitHash) throws IOException, NotFoundException,
    GitAPIException {
    validateInitialized();
    // Save changes before checking out the requested commit.
    ObjectId previousCommit = resolveHead();
    // Stash changes in current working directory. Stash ID will be null if working directory is clean.
    RevCommit stashID = git.stashCreate().setIncludeUntracked(true).call();

    try {
      ObjectId commitId = ObjectId.fromString(commitHash);
      // Each commit points to a tree that contains the files/directories as nodes. The ObjectId (hashes) of the nodes
      // represent the state of a file at that commit.
      RevCommit commit = new RevWalk(git.getRepository()).parseCommit(commitId);
      // Checkout the requested commit.
      git.checkout().setName(commitHash).call();

      // Git considers symlinks as separate files. So the hash of the symlink will be different from the target file.
      // Resolve path in case of symlinks. This ensures the hash of the target file is returned.
      Path realPath = getRepositoryRoot().resolve(relativePath).toRealPath();
      // We should not be comparing hashes of directories. If the directory contains symlinks, we can't depend on the
      // directory hash to ensure the contents are unchanged.
      if (!Files.isRegularFile(realPath)) {
        throw new IllegalArgumentException(String.format("Path %s doesn't refer to a regular file.",
                                                         realPath.toAbsolutePath()));
      }
      Path realRelativePath = getRepositoryRoot().toRealPath().relativize(realPath);
      // Find the node representing the exact file path in the tree.
      TreeWalk walk = TreeWalk.forPath(git.getRepository(), realRelativePath.toString(), commit.getTree());
      if (walk == null) {
        throw new NotFoundException("File not found in Git revision tree.");
      }
      return walk.getObjectId(0).getName();
    } finally {
      // Restore the changes to working directory.
      git.checkout().setName(previousCommit.getName()).call();
      popStash(stashID);
    }
  }

  /**
   * Initializes the Git repository by cloning remote. This method doesn't re-clone if the remote has already been
   * cloned earlier.
   *
   * @return the commit ID of the present HEAD.
   * @throws GitAPIException               when a Git operation fails.
   * @throws IOException                   when file or network I/O fails.
   * @throws AuthenticationConfigException when there is a failure while fetching authentication credentials for Git.
   */
  public String cloneRemote() throws IOException, AuthenticationConfigException, GitAPIException {
    if (git != null) {
      return resolveHead().getName();
    }
    // Clean up the directory if it already exists.
    deletePathIfExists(getRepositoryRoot());
    RepositoryConfig repositoryConfig = sourceControlConfig.getRepositoryConfig();
    credentialsProvider.refresh();
    CloneCommand command =
      createCommand(Git::cloneRepository).setURI(repositoryConfig.getLink()).setDirectory(getRepositoryRoot().toFile());
    String branch = getBranchRefName(repositoryConfig.getDefaultBranch());
    if (branch != null) {
      command.setBranchesToClone(Collections.singleton((branch))).setBranch(branch);
    }
    git = command.call();
    return resolveHead().getName();
  }

  @Override
  public void close() {
    if (git != null) {
      git.close();
    }
    git = null;
    
    try {
      deletePathIfExists(getRepositoryRoot());
    } catch (IOException e) {
      LOG.warn("Failed to close the RepositoryManager, there may be leftover files", e);
    }
  }

  /**
   * @return the absolute path for repository root
   */
  public Path getRepositoryRoot() {
    return sourceControlConfig.getLocalReposClonePath().resolve(randomDirectoryName);
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

  @VisibleForTesting
  ObjectId resolveHead() throws IOException {
    if (git == null) {
      throw new IllegalStateException("Call cloneRemote() before getting HEAD.");
    }
    return git.getRepository().resolve(Constants.HEAD);
  }

  /**
   * Pops the stash with the given rev commit.
   */
  private void popStash(@Nullable RevCommit stashID) throws GitAPIException {
    if (stashID == null) {
      LOG.debug("Pop stash called with null stashID, nothing will be popped.");
      return;
    }
    // pop the stash.
    git.stashApply().setStashRef(stashID.getName()).setRestoreUntracked(true).call();
    Collection<RevCommit> stashes = git.stashList().call();
    int stashIndex = 0;
    for (RevCommit stash : stashes) {
      if (stash.equals(stashID)) {
        git.stashDrop().setStashRef(stashIndex).call();
        return;
      }
      stashIndex++;
    }
    LOG.warn("Couldn't find Git stash with ID {} to drop. Ignoring missing stash.", stashID.getName());
  }

}
