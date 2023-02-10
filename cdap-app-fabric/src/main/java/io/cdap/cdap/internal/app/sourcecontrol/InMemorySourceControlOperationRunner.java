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
  public PushAppsResponse push(List<AppDetailsToPush> appsToPush, CommitMeta commitDetails) throws PushFailureException {
    try{
      return pushInternal(appsToPush, commitDetails);
    } finally {
      repositoryManager.stopAndWait();
    }
  }

  private PushAppsResponse pushInternal(List<AppDetailsToPush> appsToPush, CommitMeta commitDetails) throws PushFailureException {

    //TODO: handle if appsToPush is too large
    LOG.info("Pushing application configs for : {}", appsToPush.stream().map(AppDetailsToPush::getApplicationName));

    // TODO: Add retry logic here in case the head at remote moved while we are doing push
    modifyApplicationConfigAndPush(appsToPush, commitDetails);

    List<PushAppResponse> responses = new ArrayList<>();

    try {
      for (AppDetailsToPush details : appsToPush) {
        Path filePath = getApplicationConfigPath(details.getApplicationName());
        String gitFileHash = repositoryManager.getFileHash(filePath);
        responses.add(new PushAppResponse(details.getApplicationName(), gitFileHash));
      }
    } catch (Exception e) {
      LOG.debug("Failed to fetch push details: %s", e.getCause());
      throw new PushFailureException("Failed to fetch push details", e);
    }

    return new PushAppsResponse(responses);
  }

  private void modifyApplicationConfigAndPush(List<AppDetailsToPush> appsToPush, CommitMeta commitDetails) throws PushFailureException{
    try {
      // Create base dir if not exist.
      // This already handles cases where one of the path prefix is not a directory
      Files.createDirectories(repositoryManager.getBasePath());

      for (AppDetailsToPush appDetail : appsToPush) {
        String appName = appDetail.getApplicationName();
        Path configFilePath = getApplicationConfigPath(appName);
        byte[] configData = appDetail.getApplicationSpecString().getBytes(StandardCharsets.UTF_8);

        // Opens the file for writing, creating the file if it doesn't exist,
        // or truncating an existing regular-file to a size of 0
        // Symlinks will be followed but if the path points to a directory it will fail
        Files.write(configFilePath, configData);

        LOG.debug("Written application configs for {} in file {}", appName, configFilePath);
      }
    } catch (IOException e) {
      LOG.debug("Failed to write configs: %s", e.getCause());
      throw new PushFailureException("Failed to write configs", e);
    }

    try {
      repositoryManager.commitAndPush(commitDetails);
      LOG.info("Pushed application configs for : {}", appsToPush.stream().map(AppDetailsToPush::getApplicationName));
    } catch (Exception e) {
      LOG.debug("Failed to push to git", e.getCause());
      throw new PushFailureException("Failed to push to git", e);
    }
  }
  private Path getApplicationConfigPath(String applicationName){
    return repositoryManager.getBasePath().resolve(applicationName);
  }
}

