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
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.ConfigFileWriteException;
import io.cdap.cdap.sourcecontrol.GitOperationException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceControlOperationUtils {
  private static final Gson DECODE_GSON =
      new GsonBuilder().registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory()).create();
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final Logger LOG = LoggerFactory.getLogger(InMemorySourceControlOperationRunner.class);

  /**
   * Atomic operation of writing application and push, return the push response.
   *
   * @param repositoryManager {@link RepositoryManager} to conduct git operations
   * @param appsToPush        List of {@link ApplicationDetail} to push
   * @param commitDetails     {@link CommitMeta} from user input
   * @return {@link PushAppResponse}
   * @throws NoChangesToPushException if there's no change between the application in namespace and git repository
   * @throws SourceControlException   for failures while writing config file or doing git operations
   */
  public static List<PushAppResponse> writeAppDetailsAndPush(RepositoryManager repositoryManager,
      List<ApplicationDetail> appsToPush,
      CommitMeta commitDetails)
      throws NoChangesToPushException {
    try {
      // Creates the base directory if it does not exist. This method does not throw an exception if the directory
      // already exists. This is for the case that the repo is new and user configured prefix path.
      Files.createDirectories(repositoryManager.getBasePath());
    } catch (IOException e) {
      throw new SourceControlException("Failed to create repository base directory", e);
    }

    List<Path> appRelativePaths = new ArrayList<>();
    for (ApplicationDetail appToPush : appsToPush) {
      String configFileName = generateConfigFileName(appToPush.getName());
      Path appRelativePath = repositoryManager.getFileRelativePath(configFileName);
      Path filePathToWrite;
      try {
        filePathToWrite = validateAppConfigRelativePath(repositoryManager, appRelativePath);
      } catch (IllegalArgumentException e) {
        throw new SourceControlException(String.format("Failed to push application %s: %s",
            appToPush.getName(),
            e.getMessage()), e);
      }
      // Opens the file for writing, creating the file if it doesn't exist,
      // or truncating an existing regular-file to a size of 0
      try (FileWriter writer = new FileWriter(filePathToWrite.toString())) {
        GSON.toJson(appToPush, writer);
      } catch (IOException e) {
        throw new ConfigFileWriteException(
            String.format("Failed to write application config to path %s", appRelativePath), e
        );
      }
      LOG.debug("Wrote application configs for {} in file {}", appToPush.getName(), appRelativePath);
      appRelativePaths.add(appRelativePath);
    }

    try {
      // TODO: CDAP-20383, handle NoChangesToPushException
      //  Define the case that the application to push does not have any changes
      List<String> gitFileHashes = repositoryManager.commitAndPush(commitDetails, appRelativePaths);
      List<PushAppResponse> responses = new ArrayList<>();
      for (int i = 0; i < appsToPush.size(); i++) {
        ApplicationDetail currentApp = appsToPush.get(i);
        responses.add(new PushAppResponse(
            currentApp.getName(),
            currentApp.getAppVersion(),
            gitFileHashes.get(i)));
      }

      return responses;
    } catch (GitAPIException e) {
      throw new GitOperationException(String.format("Failed to push config to git: %s", e.getMessage()), e);
    }
  }

  public static String generateConfigFileName(String appName) {
    return String.format("%s.json", appName);
  }

  /**
   * Validates if the resolved path is not a symbolic link and not a directory.
   *
   * @param repositoryManager the RepositoryManager
   * @param appRelativePath   the relative {@link Path} of the application to write to
   * @return A valid application config file relative path
   */
  public static Path validateAppConfigRelativePath(RepositoryManager repositoryManager, Path appRelativePath) throws
      IllegalArgumentException {
    Path filePath = repositoryManager.getRepositoryRoot().resolve(appRelativePath);
    if (Files.isSymbolicLink(filePath)) {
      throw new IllegalArgumentException(String.format(
          "%s exists but refers to a symbolic link. Symbolic links are " + "not allowed.", appRelativePath));
    }
    if (Files.isDirectory(filePath)) {
      throw new IllegalArgumentException(String.format("%s refers to a directory not a file.", appRelativePath));
    }
    return filePath;
  }

}
