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

package io.cdap.cdap.sourcecontrol.operationrunner;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.RepositoryManagerFactory;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * In-Memory implementation for {@link SourceControlOperationRunner}.
 * Runs all git operation inside calling service.
 */
public class InMemorySourceControlOperationRunner implements SourceControlOperationRunner {
  private final RepositoryManagerFactory repoManagerFactory;
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final Logger LOG = LoggerFactory.getLogger(InMemorySourceControlOperationRunner.class);

  @Inject
  InMemorySourceControlOperationRunner(RepositoryManagerFactory repoManagerFactory) {
    this.repoManagerFactory = repoManagerFactory;
  }

  @Override
  public PushAppResponse push(NamespaceId namespace, ApplicationDetail appToPush, CommitMeta commitDetails)
    throws PushFailureException, NoChangesToPushException, AuthenticationConfigException, RepositoryNotFoundException {
    RepositoryManager repositoryManager = repoManagerFactory.create(namespace);
    try {
      try {
        repositoryManager.cloneRemote();
      } catch (GitAPIException | IOException e) {
        throw new PushFailureException(String.format("Failed to clone remote repository: %s", e.getMessage()), e);
      }

      LOG.info("Pushing application configs for : {}", appToPush);

      //TODO: CDAP-20371, Add retry logic here in case the head at remote moved while we are doing push
      return writeAppDetailAndPush(repositoryManager, appToPush, commitDetails);
    } finally {
      try {
        repositoryManager.close();
      } catch (IOException e) {
        LOG.warn("Failed to close the RepositoryManager, there may be leftover files", e);
      }
    }
  }

  /**
   * Atomic operation of writing application and push, return the push response.
   *
   * @param repositoryManager {@link RepositoryManager} to conduct git operations
   * @param appToPush         {@link ApplicationDetail} to push
   * @param commitDetails     {@link CommitMeta} from user input
   * @return {@link PushAppResponse}
   * @throws NoChangesToPushException if there's no change between the application in namespace and git repository
   * @throws PushFailureException     for failures while writing config file or doing git operations
   */
  private PushAppResponse writeAppDetailAndPush(RepositoryManager repositoryManager,
                                                ApplicationDetail appToPush,
                                                CommitMeta commitDetails)
    throws NoChangesToPushException, PushFailureException {
    try {
      // Creates the base directory if it does not exist. This method does not throw an exception if the directory
      // already exists. This is for the case that the repo is new and user configured prefix path.
      Files.createDirectories(repositoryManager.getBasePath());
    } catch (IOException e) {
      throw new PushFailureException("Failed to create repository base directory", e);
    }

    String configFileName = generateConfigFileName(appToPush.getName());

    Path appRelativePath = repositoryManager.getFileRelativePath(configFileName);
    Path filePathToWrite = validateAppConfigRelativePath(repositoryManager, appRelativePath);
    // Opens the file for writing, creating the file if it doesn't exist,
    // or truncating an existing regular-file to a size of 0
    try (FileWriter writer = new FileWriter(filePathToWrite.toString())) {
      GSON.toJson(appToPush, writer);
    } catch (IOException e) {
      throw new PushFailureException(
        String.format("Failed to write application config to path %s", appRelativePath), e);
    }

    LOG.debug("Wrote application configs for {} in file {}", appToPush.getName(), appRelativePath);

    try {
      // TODO: CDAP-20383, handle NoChangesToPushException
      //  Define the case that the application to push does not have any changes
      String gitFileHash = repositoryManager.commitAndPush(commitDetails, appRelativePath);
      return new PushAppResponse(appToPush.getName(), appToPush.getAppVersion(), gitFileHash);
    } catch (GitAPIException e) {
      throw new PushFailureException(String.format("Failed to push config to git: %s", e.getMessage()), e);
    }
  }

  /**
   * Generate config file name from app name.
   * Currently, it only adds `.json` as extension with app name being the filename.
   *
   * @param appName Name of the application
   * @return The file name we want to store application config in
   */
  private String generateConfigFileName(String appName) {
    return String.format("%s.json", appName);
  }

  /**
   * Validates if the resolved path is not a symbolic link and not a directory.
   *
   * @param repositoryManager the RepositoryManager
   * @param appRelativePath   the relative {@link Path} of the application to write to
   * @return A valid application config file relative path
   */
  private Path validateAppConfigRelativePath(RepositoryManager repositoryManager,
                                             Path appRelativePath)
    throws PushFailureException {
    Path filePath = repositoryManager.getRepositoryRoot().resolve(appRelativePath);
    if (Files.isSymbolicLink(filePath)) {
      throw new PushFailureException(String.format("Cannot push %s to the remote repository because it already " +
                                                     "exists as a symbolic link. Symbolic links are not allowed.",
                                                   appRelativePath));
    }
    if (Files.isDirectory(filePath)) {
      throw new PushFailureException(String.format("Cannot push %s to the remote repository because it already " +
                                                     "exists as a directory.", appRelativePath));
    }
    return filePath;
  }
}

