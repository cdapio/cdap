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

import static io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationUtils.writeAppDetailsAndPush;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.operation.LongRunningOperation;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.operationrun.OperationError;
import io.cdap.cdap.proto.operationrun.OperationMeta;
import io.cdap.cdap.sourcecontrol.GitOperationException;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.RepositoryManagerFactory;
import io.cdap.cdap.sourcecontrol.operationrunner.MultiPushAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppResponse;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.eclipse.jgit.api.errors.GitAPIException;

/**
 * {@link LongRunningOperation} for scm push.
 */
public class PushOperation implements LongRunningOperation<MultiPushAppOperationRequest> {
  private final RepositoryManagerFactory repositoryManagerFactory;
  private final ApplicationLifecycleService applicationLifecycleService;

  @Inject
  public PushOperation(
      RepositoryManagerFactory repositoryManagerFactory,
      ApplicationLifecycleService applicationLifecycleService
      ) {
    super();
    this.repositoryManagerFactory = repositoryManagerFactory;
    this.applicationLifecycleService = applicationLifecycleService;
  }

  @Override
  public Type getRequestType() {
    return MultiPushAppOperationRequest.class;
  }

  @Override
  public ListenableFuture<OperationError> run(MultiPushAppOperationRequest request, Consumer<OperationMeta> updateMetadata)
      throws Exception {
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

      List<ApplicationDetail> appDetails = request.getApps().stream()
          .map(appRef -> {
            ApplicationDetail appDetail = null;
            try {
               appDetail = applicationLifecycleService.getLatestAppDetail(appRef);
            } catch (IOException | NotFoundException e) {
              // Should return an operation error in the end
            }
            return appDetail;
          })
          .collect(Collectors.toList());

      List<PushAppResponse> pushAppResponses = writeAppDetailsAndPush(
          repositoryManager,
          appDetails,
          request.getCommitDetails()
      );

      // Then call the update git metadata api to update the filehashes
      // the following line use the method updateGitMetaMulti which is currently in a separate PR
      applicationLifecycleService.updateGitMetaMulti(
          request.getNamespaceId(),
          pushAppResponses.stream().map(
              pushResponse -> UpdateSourceControlMetaRequest.fromPushAppResponse(pushResponse))
              .collect(Collectors.toList())
      );
    }

    // should return an operation error
    return null;
  }
}
