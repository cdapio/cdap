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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.internal.operation.LongRunningOperation;
import io.cdap.cdap.internal.operation.LongRunningOperationContext;
import io.cdap.cdap.internal.operation.OperationException;
import io.cdap.cdap.metadata.ApplicationDetailFetcher;
import io.cdap.cdap.proto.app.UpdateMultiSourceControlMetaReqeust;
import io.cdap.cdap.proto.app.UpdateSourceControlMetaRequest;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.operation.OperationResource;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.sourcecontrol.ApplicationManager;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import io.cdap.cdap.sourcecontrol.operationrunner.InMemorySourceControlOperationRunner;
import io.cdap.cdap.sourcecontrol.operationrunner.MultiPushAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Defines operation for doing SCM Push for connected repositories.
 **/
public class PushAppsOperation implements LongRunningOperation {

  private final PushAppsRequest request;

  private final InMemorySourceControlOperationRunner scmOpRunner;
  private final ApplicationManager applicationManager;

  /**
   * Only request is passed using AssistedInject. See {@link PushAppsOperationFactory}
   *
   * @param request contains apps to push
   * @param runner runs git operations. The reason we do not use
   *     {@link io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner} rather than
   *     concrete implementation is because the git operations should always run inMemory.
   * @param applicationManager provides utilities to provide app-fabric exposed
   *     functionalities.
   */
  @Inject
  PushAppsOperation(@Assisted PushAppsRequest request,
      InMemorySourceControlOperationRunner runner,
      ApplicationManager applicationManager, ApplicationDetailFetcher appDetailsFetcher) {
    this.request = request;
    this.applicationManager = applicationManager;
    this.scmOpRunner = runner;
  }

  @Override
  public ListenableFuture<Set<OperationResource>> run(LongRunningOperationContext context)
      throws OperationException {
    RepositoryConfig repositoryConfig = request.getConfig();
    NamespaceId namespaceId = context.getRunId().getNamespaceId();
    MultiPushAppOperationRequest pushReq = new MultiPushAppOperationRequest(
        namespaceId,
        repositoryConfig,
        request.getApps(),
        request.getCommitDetails()
    );

    List<PushAppResponse> responses = new ArrayList<>();

    try {
      // pull and deploy applications one at a time
      responses = scmOpRunner.multiPush(pushReq, applicationManager);
      context.updateOperationResources(getResources(namespaceId, responses));
    } catch (SourceControlException | NoChangesToPushException e) {
      throw new OperationException(
          String.format("Failed to push applications: %s", e.getMessage()), Collections.emptyList()
      );
    }

    try {
      // update git metadata for the pushed application
      applicationManager.updateSourceControlMeta(namespaceId, getUpdateMetaRequest(responses));
    } catch (NotFoundException | BadRequestException | IOException | SourceControlException e) {
      throw new OperationException(
          String.format("Failed to update git metadata: %s", e.getMessage()),
          Collections.emptySet()
      );
    }

    // TODO(samik) Update this after along with the runner implementation
    return Futures.immediateFuture(getResources(namespaceId, responses));
  }

  private UpdateMultiSourceControlMetaReqeust getUpdateMetaRequest(
      List<PushAppResponse> responses) {
    List<UpdateSourceControlMetaRequest> reqs = responses.stream()
        .map(response -> new UpdateSourceControlMetaRequest(
            response.getName(), response.getVersion(), response.getFileHash()))
        .collect(Collectors.toList());
    return new UpdateMultiSourceControlMetaReqeust(reqs);
  }

  private Set<OperationResource> getResources(NamespaceId namespaceId,
      List<PushAppResponse> responses) {
    return responses.stream()
        .map(response -> new OperationResource(
            namespaceId.app(response.getName(), response.getVersion()).toString()))
        .collect(Collectors.toSet());
  }
}
