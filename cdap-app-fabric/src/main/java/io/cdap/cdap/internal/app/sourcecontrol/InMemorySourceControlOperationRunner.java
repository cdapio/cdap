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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class InMemorySourceControlOperationRunner implements SourceControlOperationRunner {
  private final RepositoryManager repositoryManager;
  private final CConfiguration cConf;

  private static final Logger LOG = LoggerFactory.getLogger(InMemorySourceControlOperationRunner.class);

  @Inject
  public InMemorySourceControlOperationRunner(CConfiguration cConf, RepositoryManager repositoryManager) {
    this.cConf = cConf;
    this.repositoryManager = repositoryManager;
  }

  @Override
  public ListenableFuture<PushAppsResponse> push(List<AppDetailsToPush> appsToPush, CommitMeta commitDetails,
                                                 @Nullable String branchName) throws PushFailureException {

    try {
      repositoryManager.startAndWait();
      repositoryManager.switchToCleanBranch(branchName);
    } catch (Exception e) {
      LOG.debug("Failed to initialise repository: %s", e.getCause());
      throw new PushFailureException("Failed to initialise repository", e);
    }

    Path repoBasePath = repositoryManager.getBasePath();

    //TODO: handle if appsToPush is too large
    LOG.info("Pushing application configs for : {}", appsToPush.stream().map(AppDetailsToPush::getApplicationName));

    Map<String, Path> applicationPathMap =
      appsToPush.stream().collect(Collectors.toMap(AppDetailsToPush::getApplicationName,
                                                   appDetail -> repoBasePath.resolve(appDetail.getApplicationName())));


    try {
      // Create base dir if not exist.
      // This already handles cases where one of the path prefix is not a directory
      Files.createDirectories(repoBasePath);

      for (AppDetailsToPush appDetail : appsToPush) {
        Path configFilePath = applicationPathMap.get(appDetail.getApplicationName());
        byte[] configData = appDetail.getApplicationSpecString().getBytes(StandardCharsets.UTF_8);

        // Opens the file for writing, creating the file if it doesn't exist,
        // or truncating an existing regular-file to a size of 0
        // Symlinks will be followed but if the path points to a directory it will fail
        Files.write(configFilePath, configData);

        LOG.debug("Written application configs for {} in file {}", appDetail.getApplicationName(),
                  applicationPathMap.get(appDetail.getApplicationName()));
      }
    } catch (IOException e) {
      LOG.debug("Failed to write configs: %s", e.getCause());
      throw new PushFailureException("Failed to write configs", e);
    }

    try {
      repositoryManager.push(commitDetails);
      LOG.info("Pushed application configs for : {}", appsToPush.stream().map(AppDetailsToPush::getApplicationName));
    } catch (Exception e) {
      LOG.debug("Failed to push to git", e.getCause());
      throw new PushFailureException("Failed to push to git", e);
    }

    List<PushAppResponse> responses = new ArrayList<>();

    try {
      for (AppDetailsToPush details : appsToPush) {
        Path filePath = applicationPathMap.get(details.getApplicationName());
        String gitFileHash = repositoryManager.getFileHash(filePath);
        responses.add(new PushAppResponse(details.getApplicationName(), gitFileHash));
      }
    } catch (Exception e) {
      LOG.debug("Failed to fetch push details: %s", e.getCause());
      throw new PushFailureException("Failed to fetch push details", e);
    }

    SettableFuture<PushAppsResponse> result = SettableFuture.create();
    result.set(new PushAppsResponse(responses));
    return result;
  }

  @Override
  public ListenableFuture<PullAppResponse> pull(String applicationName, String branchName) throws IOException {
    // TODO (CDAP-20325) : Implement pull operation
    return null;
  }

  @Override
  public List<ListAppResponse> list() {
    // TODO (CDAP-20326) Implement list operation
    return null;
  }

}
