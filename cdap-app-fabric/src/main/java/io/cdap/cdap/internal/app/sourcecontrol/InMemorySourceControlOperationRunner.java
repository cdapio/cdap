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

package io.cdap.cdap.internal.app.sourcecontrol;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.UnexpectedRepositoryChangesException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * In-Memory implementation for {@link SourceControlOperationRunner}.
 * Runs all git operation inside calling service.
 */
public class InMemorySourceControlOperationRunner implements SourceControlOperationRunner {
  private final RepositoryManager repositoryManager;
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  private static final Logger LOG = LoggerFactory.getLogger(InMemorySourceControlOperationRunner.class);

  @Inject
  public InMemorySourceControlOperationRunner(@Assisted RepositoryManager repositoryManager) {
    this.repositoryManager = repositoryManager;
  }

  @Override
  public PushAppsResponse push(List<ApplicationDetail> appsToPush, CommitMeta commitDetails)
    throws PushFailureException, NoChangesToPushException {
    try {
      repositoryManager.cloneRemote();
    } catch (Exception e) {
      throw new PushFailureException("Failed to clone remote repository", e);
    }
    //TODO CDAP-20371: handle if appsToPush is too large
    LOG.info("Pushing application configs for : {}", appsToPush.stream().map(ApplicationDetail::getName));

    //TODO CDAP-20371: Add retry logic here in case the head at remote moved while we are doing push
    String pushedCommit = modifyApplicationConfigAndPush(appsToPush, commitDetails);

    List<PushAppResponse> responses = new ArrayList<>();

    try {
      for (ApplicationDetail detail : appsToPush) {
        Path relativeFilePath = getAndValidateAppConfigRelativePath(detail.getName());
        // In the case that the app config is unchanged, we can still get the fileHash from pushedCommit
        String gitFileHash = repositoryManager.getFileHash(relativeFilePath, pushedCommit);
        responses.add(new PushAppResponse(detail.getName(), detail.getAppVersion(), gitFileHash));
      }
    } catch (Exception e) {
      throw new PushFailureException("Failed to fetch push details", e);
    }

    return new PushAppsResponse(responses);
  }

  @Override
  public PullAppResponse pull(ApplicationId appId) {
    return null;
  }

  private String modifyApplicationConfigAndPush(List<ApplicationDetail> appsToPush, CommitMeta commitDetails)
    throws PushFailureException, NoChangesToPushException {
    List<Path> modifiedPaths = new ArrayList<>();
    try {
      for (ApplicationDetail appDetail : appsToPush) {
        String appName = appDetail.getName();
        // Symlinks will be followed but if the path points to an outside directory it will fail
        Path appRelativePath = getAndValidateAppConfigRelativePath(appName);
        // Opens the file for writing, creating the file if it doesn't exist,
        // or truncating an existing regular-file to a size of 0
        Path filePathToWrite = repositoryManager.getRepositoryRoot().resolve(appRelativePath);
        try (FileWriter writer = new FileWriter(filePathToWrite.toString())) {
          GSON.toJson(appDetail, writer);
        }
        
        modifiedPaths.add(appRelativePath);
        LOG.debug("Written application configs for {} in file {}", appName, appRelativePath);
      }
    } catch (Exception e) {
      throw new PushFailureException("Failed to write configs", e);
    }

    try {
      String pushedCommit = repositoryManager.commitAndPush(commitDetails, modifiedPaths);
      LOG.info("Pushed application configs for : {}", appsToPush.stream().map(ApplicationDetail::getName));
      return pushedCommit;
    } catch (UnexpectedRepositoryChangesException | GitAPIException e) {
      throw new PushFailureException("Failed to push to git", e);
    }
  }

  /**
   * Generates the config file relative path in git root directory based on application name.
   * It also validates if the resolved path ( considering symlinks) exists outside git repository
   * or not.
   *
   * @param applicationName the name of this application
   * @return A valid application config file path
   */
  private Path getAndValidateAppConfigRelativePath(String applicationName) throws InvalidPathInSourceControl,
                                                                                  IOException {
    String fileName = String.format("%s.json", applicationName);
    Path filePath = repositoryManager.getBasePath().resolve(fileName);
    if (Files.exists(filePath)) {
      Path resolvedPath = filePath.toRealPath();
      Path resolvedRepoRootPath = repositoryManager.getRepositoryRoot().toRealPath();
      if (!resolvedPath.startsWith(resolvedRepoRootPath)) {
        throw new InvalidPathInSourceControl(String.format("Config file %s pointing to invalid path %s", filePath,
                                                           resolvedPath));
      }
    }
    return repositoryManager.getRepositoryRoot().relativize(filePath);
  }
}

