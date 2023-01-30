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

package io.cdap.cdap.sourcecontrol.SourceControlOperationRunner;

import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.SourceControlManager;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InMemorySourceControlOperationRunner implements SourceControlOperationRunner {
  private SourceControlManager sourceControlManager;


  @Override
  public List<PushAppResponse> push(List<AppDetailsToPush> appsToPush, CommitMeta commitDetails) throws IOException {
    Path repoBasePath = sourceControlManager.getBasePath();

    Map<String, Path> applicationPathMap =
      appsToPush.stream().collect(Collectors.toMap(AppDetailsToPush::getApplicationName,
                                                   appDetail -> repoBasePath.resolve(appDetail.getApplicationName())));
    for (AppDetailsToPush appDetail : appsToPush) {
      Files.write(applicationPathMap.get(appDetail.getApplicationName()),
                  appDetail.getApplicationSpecString().getBytes(StandardCharsets.UTF_8));
    }

    sourceControlManager.push(commitDetails);

    List<PushAppResponse> response = appsToPush.stream().map(appDetails -> {
      Path filePath = applicationPathMap.get(appDetails.getApplicationName());
      return new PushAppResponse(appDetails.getApplicationName(), sourceControlManager.getFileHash(filePath));
    }).collect(Collectors.toList());

    return response;
  }

  @Override
  public PullAppResponse pull(String applicationName, String branchName) throws IOException {
    Path filePath = sourceControlManager.getBasePath().resolve(applicationName);

    sourceControlManager.switchToCleanBranch(branchName);

    String fileHash = sourceControlManager.getFileHash(filePath);
    String applicationSpecString = new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8);

    return new PullAppResponse(applicationName, fileHash, applicationSpecString);
  }

  @Override
  public List<ListAppResponse> list() {
    Path baseDir = sourceControlManager.getBasePath();
    List<File> listOfFiles = DirUtils.listFiles(baseDir.toFile(), File::isFile);
    List<ListAppResponse> response = listOfFiles.stream().map(file -> {
      String fileHash = sourceControlManager.getFileHash(file.toPath());
      return new ListAppResponse(file.getName(), fileHash);
    }).collect(Collectors.toList());
    return response;
  }
}
