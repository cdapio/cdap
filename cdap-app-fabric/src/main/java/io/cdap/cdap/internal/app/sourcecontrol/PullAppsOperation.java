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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.internal.operation.LongRunningOperation;
import io.cdap.cdap.internal.operation.LongRunningOperationContext;
import io.cdap.cdap.internal.operation.OperationException;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.operation.OperationResource;
import io.cdap.cdap.proto.operation.OperationResourceScopedError;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.sourcecontrol.ApplicationManager;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import io.cdap.cdap.sourcecontrol.operationrunner.InMemorySourceControlOperationRunner;
import io.cdap.cdap.sourcecontrol.operationrunner.MultiPullAppOperationRequest;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Defines operation for doing SCM Pull for connected repositories.
 **/
public class PullAppsOperation implements LongRunningOperation {

  private final PullAppsRequest request;

  private final InMemorySourceControlOperationRunner scmOpRunner;
  private final ApplicationManager applicationManager;

  private final Set<ApplicationId> deployed;

  /**
   * Only request is passed using AssistedInject. See {@link PullAppsOperationFactory}
   *
   * @param request contains apps to pull
   * @param runner runs git operations. The reason we do not use
   *     {@link io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner} rather than
   *     concrete implementation is because the git operations should always run inMemory.
   * @param applicationManager provides utilities to provide app-fabric exposed
   *     functionalities.
   */
  @Inject
  PullAppsOperation(@Assisted PullAppsRequest request,
      InMemorySourceControlOperationRunner runner,
      ApplicationManager applicationManager) {
    this.request = request;
    this.applicationManager = applicationManager;
    this.scmOpRunner = runner;
    this.deployed = new HashSet<>();
  }

  @Override
  public ListenableFuture<Set<OperationResource>> run(LongRunningOperationContext context)
      throws OperationException {
    AtomicReference<ApplicationReference> appTobeDeployed = new AtomicReference<>();

    try {
      RepositoryConfig repositoryConfig = request.getConfig();
      MultiPullAppOperationRequest pullReq = new MultiPullAppOperationRequest(
          repositoryConfig,
          context.getRunId().getNamespaceId(),
          request.getApps()
      );

      // pull and deploy applications one at a time
      scmOpRunner.multiPull(pullReq, response -> {
        appTobeDeployed.set(new ApplicationReference(context.getRunId().getNamespace(),
            response.getApplicationName()));
        try {
          ApplicationId deployedVersion = applicationManager.deployApp(
              appTobeDeployed.get(), response
          );
          deployed.add(deployedVersion);
          context.updateOperationResources(getResources());
        } catch (Exception e) {
          throw new SourceControlException(e);
        }
      });
    } catch (Exception e) {
      throw new OperationException(
          "Failed to deploy applications",
          appTobeDeployed.get() != null ? ImmutableList.of(
              new OperationResourceScopedError(appTobeDeployed.get().toString(), e.getMessage()))
              : Collections.emptyList()
      );
    }

    try {
      // all deployed versions are marked latest atomically
      applicationManager.markAppVersionsLatest(
          context.getRunId().getNamespaceId(),
          deployed.stream().map(ApplicationId::getAppVersion).collect(Collectors.toList())
      );
    } catch (BadRequestException | NotFoundException | IOException e) {
      throw new OperationException(
          "Failed to mark applications latest",
          Collections.emptySet()
      );
    }

    // TODO(samik, CDAP-20855) Investigate and implement the cleanup for created versions in case of error

    // TODO(samik) Update this after along with the runner implementation
    return Futures.immediateFuture(getResources());
  }

  private Set<OperationResource> getResources() {
    return deployed.stream()
        .map(app -> new OperationResource(app.toString()))
        .collect(Collectors.toSet());
  }
}
