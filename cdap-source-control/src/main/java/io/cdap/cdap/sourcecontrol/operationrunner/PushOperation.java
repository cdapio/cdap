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

import io.cdap.cdap.api.retry.Idempotency;
import io.cdap.cdap.api.service.operation.LongRunningOperation;
import io.cdap.cdap.api.service.operation.OperationError;
import io.cdap.cdap.api.service.operation.OperationMeta;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.ConfigFileWriteException;
import io.cdap.cdap.sourcecontrol.GitOperationException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.RepositoryManagerFactory;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushOperation implements LongRunningOperation<MultiPushAppOperationRequest> {
  private final RepositoryManagerFactory repositoryManagerFactory;
  private final RemoteClient remoteClient;
  private static final Logger LOG = LoggerFactory.getLogger(PushOperation.class);

  public PushOperation(RepositoryManagerFactory repositoryManagerFactory, RemoteClient remoteClient) {
    this.repositoryManagerFactory = repositoryManagerFactory;
    this.remoteClient = remoteClient;
  }


  public String getType() {
    return "PUSH_APPS";
  }

  public String getRequestClassName() {
    return MultiPushAppOperationRequest.class.getName();
  }

  @Override
  public List<OperationError> run(
      MultiPushAppOperationRequest request, Consumer<OperationMeta> updateMetadata
  ) throws Exception {
    try (
      RepositoryManager repositoryManager = repositoryManagerFactory.create(request.getNamespaceId(),
                                                                      request.getRepositoryConfig())
    ) {
      try {
        repositoryManager.cloneRemote();
      } catch (GitAPIException | IOException e) {
        throw new GitOperationException(String.format("Failed to clone remote repository: %s",
                                                      e.getMessage()), e);
      }

      //TODO: CDAP-20371, Add retry logic here in case the head at remote moved while we are doing push
      writeAppDetailAndPush(
        repositoryManager,
        request.getApps(),
        request.getCommitDetails(),
        remoteClient
      );
    }
    return Collections.emptyList();
  }


  /**
   * Atomic operation of writing application and push, return the push response.
   *
   * @param repositoryManager {@link RepositoryManager} to conduct git operations
   * @param appsToPush         {@link ApplicationDetail} list to push
   * @param commitDetails     {@link CommitMeta} from user input
   * @return {@link PushAppResponse}
   * @throws NoChangesToPushException if there's no change between the application in namespace and git repository
   * @throws SourceControlException   for failures while writing config file or doing git operations
   */
  private PushAppResponse writeAppDetailAndPush(RepositoryManager repositoryManager,
                                                List<ApplicationId> appsToPush,
                                                CommitMeta commitDetails,
                                                RemoteClient remoteClient)
    throws NoChangesToPushException {
    try {
      // Creates the base directory if it does not exist. This method does not throw an exception if the directory
      // already exists. This is for the case that the repo is new and user configured prefix path.
      Files.createDirectories(repositoryManager.getBasePath());
    } catch (IOException e) {
      throw new SourceControlException("Failed to create repository base directory", e);
    }

    List<Path> appRelativePaths = new ArrayList<>();
    Random random = new Random();

    appsToPush.parallelStream().forEach(appToPush -> {
      String configFileName = OperationUtils.generateConfigFileName(appToPush.getApplication()+ random.nextInt(200));

      Path appRelativePath = repositoryManager.getFileRelativePath(configFileName);
      Path filePathToWrite;
      try {
        filePathToWrite = OperationUtils.validateAppConfigRelativePath(repositoryManager, appRelativePath);
      } catch (IllegalArgumentException e) {
        throw new SourceControlException(String.format("Failed to push application %s: %s",
                                                       appToPush.getApplication(),
                                                       e.getMessage()), e);
      }

      HttpRequest.Builder requestBuilder =
        remoteClient.requestBuilder(
          HttpMethod.GET,
          String.format("namespaces/%s/apps/%s", appToPush.getParent().getEntityName(), appToPush.getApplication())
        );

      // Opens the file for writing, creating the file if it doesn't exist,
      // or truncating an existing regular-file to a size of 0
      try (FileWriter writer = new FileWriter(filePathToWrite.toString())) {
        HttpResponse response = remoteClient.execute(requestBuilder.build(), Idempotency.AUTO);
        writer.write(response.getResponseBodyAsString());
      } catch (IOException e) {
        throw new ConfigFileWriteException(
          String.format("Failed to write application config to path %s", appRelativePath), e
        );
      }

      LOG.debug("Wrote application configs for {} in file {}", appToPush.getApplication(),
                appRelativePath);

      appRelativePaths.add(appRelativePath);
    });

    try {
      // TODO: CDAP-20383, handle NoChangesToPushException
      //  Define the case that the application to push does not have any changes
      Map<Path, String> gitHashes = repositoryManager.commitAndPush(commitDetails, appRelativePaths);
      return new PushAppResponse("test", "test", "test");
    } catch (GitAPIException e) {
      throw new GitOperationException(String.format("Failed to push config to git: %s", e.getMessage()), e);
    }
  }

}
