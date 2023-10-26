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
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.Metrics.SourceControlManagement;
import io.cdap.cdap.common.conf.Constants.Metrics.Tag;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RemoteRepositoryValidationException;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigValidationException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LsRemoteCommand;
import org.eclipse.jgit.api.PushCommand;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.TransportCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.PushResult;
import org.eclipse.jgit.transport.RemoteConfig;
import org.eclipse.jgit.transport.RemoteRefUpdate;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A git repository manager that is responsible for handling interfacing with
 * git. It provides version control operations. This is not thread safe.
 */
public class RepositoryManager implements AutoCloseable {

  private static final long MEGA_BYTES = 1000000;
  private static final Logger LOG = LoggerFactory.getLogger(
      RepositoryManager.class);
  private final SourceControlConfig sourceControlConfig;
  private final RefreshableCredentialsProvider credentialsProvider;
  private final MetricsContext metricsContext;
  private Git git;
  // We clone the repository inside a randomly named directory to prevent
  // multiple repository managers for the same namespace from interfering with
  // each other.
  private final String randomDirectoryName;

  /**
   * Constructs a {@link RepositoryManager}.
   *
   * @param secureStore A store to fetch Git passwords.
   * @param cConf CDAP configurations.
   * @param namespace The CDAP namespace to whcih the Git repository is linked.
   * @param repoConfig The repository configuration for the CDAP namespace.
   * @param metricsCollectionService A metrics service to emit metrics related
   *                                 to SCM operations.
   */
  public RepositoryManager(final SecureStore secureStore,
      final CConfiguration cConf, final NamespaceId namespace,
      final RepositoryConfig repoConfig,
      final MetricsCollectionService metricsCollectionService) {
    Map<String, String> tags = ImmutableMap.of(Tag.NAMESPACE,
        namespace.getNamespace());
    this.metricsContext = metricsCollectionService.getContext(tags);
    this.sourceControlConfig = new SourceControlConfig(namespace, repoConfig,
        cConf);
    try {
      this.credentialsProvider = new AuthenticationStrategyProvider(
          namespace.getNamespace(), secureStore).get(repoConfig)
          .getCredentialsProvider();
    } catch (AuthenticationStrategyNotFoundException e) {
      // This is not expected as only valid auth configs will be stored.
      throw new RuntimeException(e);
    }
    this.randomDirectoryName = UUID.randomUUID().toString();
  }

  /**
   * Returns the base path in the git repo to store CDAP applications. If an
   * optional Path prefix is provided in the repository configuration, the
   * returned path includes it.
   *
   * @return the path for the repository base directory.
   */
  public Path getBasePath() {
    Path localRepoPath = getRepositoryRoot();
    String pathPrefix = sourceControlConfig.getRepositoryConfig()
        .getPathPrefix();
    if (Strings.isNullOrEmpty(pathPrefix)) {
      return localRepoPath;
    }
    return localRepoPath.resolve(pathPrefix);
  }

  /**
   * Gets the relative path of a file in git repository based on the user
   * configured path prefix.
   *
   * @param fileName The filename
   * @return the relative {@link Path}
   */
  public Path getFileRelativePath(final String fileName) {
    String pathPrefix = sourceControlConfig.getRepositoryConfig()
        .getPathPrefix();
    if (Strings.isNullOrEmpty(pathPrefix)) {
      return Paths.get(fileName);
    }
    return Paths.get(pathPrefix, fileName);
  }

  /**
   * Validates the provided configuration.
   *
   * @param secureStore         A secure store for fetching credentials if
   *                            required.
   * @param sourceControlConfig Configuration for source control operations.
   * @throws RepositoryConfigValidationException when provided repository
   *                                             configuration is invalid.
   */
  public static void validateConfig(final SecureStore secureStore,
      final SourceControlConfig sourceControlConfig)
      throws RemoteRepositoryValidationException {
    RepositoryConfig config = sourceControlConfig.getRepositoryConfig();
    RefreshableCredentialsProvider credentialsProvider;
    try {
      credentialsProvider = new AuthenticationStrategyProvider(
          sourceControlConfig.getNamespaceId(),
          secureStore)
          .get(sourceControlConfig.getRepositoryConfig())
          .getCredentialsProvider();
    } catch (AuthenticationStrategyNotFoundException e) {
      throw new RepositoryConfigValidationException(e.getMessage(), e);
    }
    try {
      credentialsProvider.refresh();
    } catch (AuthenticationConfigException e) {
      throw new RepositoryConfigValidationException(
          "Failed to get authentication credentials: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new RemoteRepositoryValidationException(
          "Internal error: " + e.getMessage(), e);
    }
    // Try fetching heads in the remote repository.
    try (Git git = Git.wrap(new InMemoryRepository.Builder().build())) {
      LsRemoteCommand cmd =
          createCommand(git::lsRemote, sourceControlConfig, credentialsProvider)
              .setRemote(config.getLink())
              .setTags(false);
      validateDefaultBranch(cmd.callAsMap(), config.getDefaultBranch());
    } catch (TransportException e) {
      throw new RepositoryConfigValidationException(
          "Failed to connect with remote repository: " + e.getMessage(), e);
    } catch (GitAPIException e) {
      throw new RepositoryConfigValidationException(
          "Failed to list remotes in remote repository: " + e.getMessage(),
          e);
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e,
          RepositoryConfigValidationException.class);
      throw new RemoteRepositoryValidationException(
          "Failed to list remotes in remote repository.",
          e);
    }
  }

  /**
   * Commits and pushes the changes of given files under the repository root
   * path.
   *
   * @param commitMeta  Details for the commit including author, committer and
   *                    commit message
   * @param filesChanged The relative path to repository root where the file is
   *                    updated
   * @return the hash of the written file.
   * @throws GitAPIException          when the underlying git commands fail
   * @throws NoChangesToPushException when there's no file changes for the
   *                                  commit
   * @throws GitOperationException    when failed to get filehash due to IOException
   *                                  or the {@link PushResult} status is not OK
   * @throws SourceControlException   when failed to get the fileHash before
   *                                  push
   */
  public Map<Path, String> commitAndPush(final CommitMeta commitMeta,
      final Set<Path> filesChanged)
      throws NoChangesToPushException, GitAPIException {
    validateInitialized();
    final Stopwatch stopwatch = new Stopwatch().start();

    // if the status is clean skip
    Status preStageStatus = git.status().call();
    if (preStageStatus.isClean()) {
      throw new NoChangesToPushException(
          "No changes have been made for the applications to push.");
    }

    for (Path fileChanged : filesChanged) {
      git.add().addFilepattern(fileChanged.toString()).call();
    }

    RevCommit commit = getCommitCommand(commitMeta).call();

    Map<Path, String> fileHashes = new HashMap<>();
    for (Path fileChanged : filesChanged) {
      try {
        String fileHash = getFileHash(fileChanged, commit);
        fileHashes.put(fileChanged, fileHash);
        if (fileHash == null) {
          throw new SourceControlException(
              String.format(
                  "Failed to get fileHash for %s, because the path is not "
                      + "found in Git tree", fileChanged));
        }
      } catch (IOException e) {
        throw new GitOperationException(
            String.format("Failed to get fileHash for %s", fileChanged),
            e);
      }
    }

    if (fileHashes.size() != filesChanged.size()) {
      throw new SourceControlException(
          String.format(
              "Failed to get fileHash for %s because some paths are not "
                  + "found in Git tree", filesChanged));
    }

    PushCommand pushCommand = createCommand(git::push, sourceControlConfig,
        credentialsProvider);
    Iterable<PushResult> pushResults = pushCommand.call();

    for (PushResult result : pushResults) {
      for (RemoteRefUpdate rru : result.getRemoteUpdates()) {
        if (rru.getStatus() != RemoteRefUpdate.Status.OK
            && rru.getStatus() != RemoteRefUpdate.Status.UP_TO_DATE) {
          throw new GitOperationException(
              String.format("Push failed for %s: %s", filesChanged,
                  rru.getStatus()));
        }
      }
    }

    metricsContext.event(
        SourceControlManagement.COMMIT_PUSH_LATENCY_MILLIS,
        stopwatch.stop().elapsedTime(TimeUnit.MILLISECONDS));
    return fileHashes;
  }

  private CommitCommand getCommitCommand(final CommitMeta commitMeta) {
    // We only set email
    PersonIdent author = new PersonIdent(commitMeta.getAuthor(), "");
    PersonIdent authorWithDate = new PersonIdent(author,
        new Date(commitMeta.getTimestampMillis()));

    PersonIdent committer = new PersonIdent(commitMeta.getCommitter(), "");

    return git.commit().setAuthor(authorWithDate).setCommitter(committer)
        .setMessage(commitMeta.getMessage());
  }

  /**
   * Initializes the Git repository by cloning remote. If the repository is
   * already cloned, it resets the git repository to the state in the remote.
   * All local changes will be lost.
   *
   * @return the commit ID of the present HEAD.
   * @throws GitAPIException               when a Git operation fails.
   * @throws IOException                   when file or network I/O fails.
   * @throws AuthenticationConfigException when there is a failure while
   *                                       fetching authentication credentials
   *                                       for Git.
   */
  public String cloneRemote()
      throws IOException, AuthenticationConfigException, GitAPIException {
    credentialsProvider.refresh();
    if (git != null) {
      return resetToCleanBranch(git.getRepository().getBranch());
    }
    // Clean up the directory if it already exists.
    deletePathIfExists(getRepositoryRoot());
    RepositoryConfig repositoryConfig = sourceControlConfig.getRepositoryConfig();
    CloneCommand command = createCommand(Git::cloneRepository).setURI(
        repositoryConfig.getLink()).setDirectory(getRepositoryRoot().toFile());
    String branch = getBranchRefName(repositoryConfig.getDefaultBranch());
    if (branch != null) {
      command.setBranchesToClone(Collections.singleton((branch)))
          .setBranch(branch);
    }

    final Stopwatch stopwatch = new Stopwatch().start();
    git = command.call();
    final long cloneTimeMillis = stopwatch.stop()
        .elapsedTime(TimeUnit.MILLISECONDS);

    // Record the repository size metric.
    try {
      long repoSize = calculateDirectorySize(getRepositoryRoot());
      metricsContext.event(SourceControlManagement.CLONE_REPOSITORY_SIZE_BYTES,
          repoSize);
      metricsContext.event(
          SourceControlManagement.CLONE_LATENCY_MS, cloneTimeMillis);
    } catch (IOException e) {
      LOG.warn(
          "Failed to calculate directory size, metrics for the clone operation will not be emitted.",
          e);
    }
    return resolveHead().getName();
  }

  /**
   * Closes the repository manager and frees resources.
   */
  @Override
  public void close() {
    if (git != null) {
      git.close();
    }
    git = null;
    try {
      deletePathIfExists(getRepositoryRoot());
    } catch (IOException e) {
      LOG.warn(
          "Failed to close the RepositoryManager, there may be leftover files",
          e);
    }
  }

  /**
   * Returns the root directory path where the git repo is cloned.
   *
   * @return the absolute path for repository root
   */
  public Path getRepositoryRoot() {
    return sourceControlConfig.getLocalReposClonePath()
        .resolve(randomDirectoryName);
  }

  /**
   * Returns the <a href="https://git-scm.com/docs/git-hash-object">Git Hash</a>
   * of the requested file path in the provided commit. It is the caller's
   * responsibility to handle paths that refer to directories or symlinks.
   *
   * @param relativePath The path relative to the repository base path (returned
   *                     by {@link RepositoryManager#getRepositoryRoot()}) on
   *                     the filesystem.
   * @param commitHash   The commit ID hash for which to get the file hash.
   * @return The git file hash of the requested file.
   * @throws IOException           when file or commit isn't found, there are
   *                               circular symlinks or other file IO errors.
   * @throws NotFoundException     when the file isn't committed to Git.
   * @throws IllegalStateException when {@link RepositoryManager#cloneRemote()}
   *                               isn't called before this.
   */
  public String getFileHash(final Path relativePath, final String commitHash)
      throws IOException, NotFoundException,
      GitAPIException {
    validateInitialized();
    ObjectId commitId = ObjectId.fromString(commitHash);

    // Each commit points to a tree that contains the files/directories as
    // nodes. The ObjectId (hashes) of the nodes represent the state of a
    // file at that commit.
    RevCommit commit = getRevCommit(commitId, true);
    String hash = getFileHash(relativePath, commit);
    if (hash == null) {
      throw new NotFoundException("File not found in Git revision tree.");
    }
    return hash;
  }

  @Nullable
  private String getFileHash(final Path relativePath, final RevCommit commit)
      throws IOException {
    // Find the node representing the exact file path in the tree.
    try (TreeWalk walk = TreeWalk.forPath(git.getRepository(),
        relativePath.toString(), commit.getTree())) {
      if (walk == null) {
        LOG.warn("Path {} not found in Git tree", relativePath);
        return null;
      }
      return walk.getObjectId(0).getName();
    }
  }

  private <C extends TransportCommand> C createCommand(
      final Supplier<C> creator) {
    return createCommand(creator, sourceControlConfig, credentialsProvider);
  }

  private static <C extends TransportCommand> C createCommand(
      final Supplier<C> creator,
      final SourceControlConfig sourceControlConfig,
      final CredentialsProvider credentialsProvider) {
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
  private static String getBranchRefName(final @Nullable String branch) {
    if (Strings.isNullOrEmpty(branch)) {
      return null;
    }
    return "refs/heads/" + branch;
  }

  /**
   * Returns the remote branch ref name for a given branch name.
   *
   * @param branch name without refs/head prefix.
   * @param remoteName name of the git remote.
   * @return the ref name for the remote branch.
   */
  private static String getRemoteBranchRefName(final String branch,
      final String remoteName) {
    if (Strings.isNullOrEmpty(branch)) {
      return null;
    }
    return String.format("refs/remotes/%s/%s", remoteName, branch);
  }

  private static void validateDefaultBranch(final Map<String, Ref> refs,
      final @Nullable String defaultBranchName) throws
      RepositoryConfigValidationException {

    if (getBranchRefName(defaultBranchName) == null) {
      // If default branch is not provided, check for existence of "HEAD" that
      // points to the default branch in the remote repository.
      if (refs.get(Constants.HEAD) == null) {
        throw new RepositoryConfigValidationException(
            "HEAD not found in remote repository. Ensure a default branch is set in the remote repository.");
      }
      return;
    }
    // Check if default branch exists.
    if (refs.get(getBranchRefName(defaultBranchName)) == null) {
      throw new RepositoryConfigValidationException(String.format(
          "Default branch not found in remote repository."
              + " Ensure branch '%s' already exists.",
          defaultBranchName));
    }
  }

  /**
   * Validates whether git is initialized.
   */
  private void validateInitialized() {
    if (git == null) {
      throw new IllegalStateException(
          "Initialize source control manager before performing operation.");
    }
  }

  private static void deletePathIfExists(final Path path) throws IOException {
    if (Files.exists(path)) {
      DirUtils.deleteDirectoryContents(path.toFile());
    }
  }

  /**
   * Returns the commit ID for the present head.
   *
   * @return The commit ID for the head.
   * @throws IOException when git is unable to resolve head.
   * @throws GitOperationException if the repository is not initialized.
   */
  @VisibleForTesting
  ObjectId resolveHead() throws IOException {
    if (git == null) {
      throw new IllegalStateException(
          "Call cloneRemote() before getting HEAD.");
    }

    ObjectId commitObjectId = git.getRepository().resolve(Constants.HEAD);

    if (commitObjectId == null) {
      throw new GitOperationException(
          "Failed to resolve Git HEAD. Please make sure the repository is initialized.");
    }

    return commitObjectId;
  }

  /**
   * Gets the {@link RevCommit} for a {@link ObjectId} from the local git
   * repository.
   *
   * @param commitId               the ID of the commit to be fetched.
   * @param fetchCommitsIfRequired Whether to fetch commits from remote
   *                               repository when commit isn't found locally.
   * @return the {@link RevCommit}
   * @throws IOException     when the commit for the provided commitId isn't
   *                         found.
   * @throws GitAPIException when there are failures while fetching commits from
   *                         the remote.
   */
  private RevCommit getRevCommit(final ObjectId commitId,
      final boolean fetchCommitsIfRequired)
      throws IOException,
      GitAPIException {
    try (RevWalk walk = new RevWalk(git.getRepository())) {
      return walk.parseCommit(commitId);
    } catch (MissingObjectException e) {
      if (!fetchCommitsIfRequired) {
        throw e;
      }
      LOG.warn(
          "Commit {} not found in local repository, "
              + "fetching commits from remote",
          commitId.getName());
      fetchRemote(getOrigin());
      // set fetchCommitsIfRequired as false to prevent fetching commits
      // indefinitely.
      return getRevCommit(commitId, false);
    }
  }

  /**
   * Gets the {@link RemoteConfig} for the single remote that is being tracked.
   *
   * @return the {@link RemoteConfig} for origin.
   */
  private RemoteConfig getOrigin() throws GitAPIException {
    List<RemoteConfig> remoteConfigs = git.remoteList().call();
    if (remoteConfigs.size() != 1) {
      throw new IllegalStateException(
          String.format("%s remotes found for git repository, expected 1.",
              remoteConfigs.size()));
    }
    return remoteConfigs.get(0);
  }

  /**
   * Fetched commits from the remote.
   *
   * @param remote the remote to fetch.
   */
  private void fetchRemote(final RemoteConfig remote) throws GitAPIException {
    createCommand(git::fetch).setRefSpecs(remote.getFetchRefSpecs()).call();
  }

  /**
   * Does a <a href="https://git-scm.com/docs/git-reset"> hard reset </a> to the
   * remote branch being tracked.
   *
   * @param remoteBranchName branch to reset to.
   * @return the commit ID of the new head.
   */
  private String resetToCleanBranch(final String remoteBranchName)
      throws IOException, GitAPIException {
    RemoteConfig origin = getOrigin();
    fetchRemote(origin);
    git.reset()
        .setMode(ResetCommand.ResetType.HARD)
        .setRef(getRemoteBranchRefName(remoteBranchName, origin.getName()))
        .call();
    git.clean().setCleanDirectories(true).setIgnore(true).call();
    return resolveHead().getName();
  }

  /**
   * Calculates the size of a directory in bytes while ignoring symlinks.
   *
   * @param directoryPath the path of the root directory.
   * @return the size of the directory in bytes.
   * @throws IOException when there are failures while reading the file or {@link UncheckedIOException}
   *     is thrown out and we throw to the cause.
   */
  @VisibleForTesting
  static long calculateDirectorySize(Path directoryPath) throws IOException {
    try (Stream<Path> walk = Files.walk(directoryPath.toAbsolutePath())) {
      try {
        return walk
            .filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS))
            .mapToLong(p -> p.toFile().length())
            .sum();
      } catch (UncheckedIOException e) {
        throw e.getCause();
      }
    }
  }
}
