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

package io.cdap.cdap.internal.app.services;

import com.google.inject.Inject;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.proto.sourcecontrol.RemoteRepositoryValidationException;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.SourceControlConfig;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.PushFailureException;
import io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner;
import io.cdap.cdap.store.RepositoryStore;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Service that manages source control for repositories and applications.
 * It exposes repository CRUD apis and source control tasks that do pull/pull/list applications in linked repository.
 */
public class SourceControlManagementService {
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final CConfiguration cConf;
  private final SecureStore secureStore;
  private final SourceControlOperationRunner sourceControlOperationRunner;
  private final ApplicationLifecycleService appLifecycleService;
  private final Store store;
  private final RepositoryStore repoStore;


  @Inject
  public SourceControlManagementService(CConfiguration cConf,
                                        SecureStore secureStore,
                                        AccessEnforcer accessEnforcer,
                                        AuthenticationContext authenticationContext,
                                        SourceControlOperationRunner sourceControlOperationRunner,
                                        ApplicationLifecycleService applicationLifecycleService,
                                        Store store,
                                        RepositoryStore repoStore) {
    this.cConf = cConf;
    this.secureStore = secureStore;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.sourceControlOperationRunner = sourceControlOperationRunner;
    this.appLifecycleService = applicationLifecycleService;
    this.store = store;
    this.repoStore = repoStore;
  }

  public RepositoryMeta setRepository(NamespaceId namespace, RepositoryConfig repository)
    throws NamespaceNotFoundException {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    return repoStore.setRepository(namespace, repository);
  }

  public void deleteRepository(NamespaceId namespace) {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(), StandardPermission.DELETE);
    repoStore.deleteRepository(namespace);
  }

  public RepositoryMeta getRepositoryMeta(NamespaceId namespace) throws RepositoryNotFoundException {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(), StandardPermission.GET);
    return repoStore.getRepositoryMeta(namespace);
  }

  public void validateRepository(NamespaceId namespace, RepositoryConfig repoConfig)
    throws RemoteRepositoryValidationException {
    RepositoryManager.validateConfig(secureStore, new SourceControlConfig(namespace, repoConfig, cConf));
  }

  /**
   * The method to push an application to linked repository
   * @param appRef {@link ApplicationReference}
   * @param commitMessage optional commit message from user
   * @return {@link PushAppResponse}
   * @throws NotFoundException if the application is not found or the repository config is not found
   * @throws IOException if {@link ApplicationLifecycleService} fails to get the adminOwner store
   * @throws PushFailureException if {@link SourceControlOperationRunner} fails to push
   * @throws NoChangesToPushException if there's no change of the application between namespace and linked repository
   */
  public PushAppResponse pushApp(ApplicationReference appRef, @Nullable String commitMessage)
    throws NotFoundException, IOException, PushFailureException,
           NoChangesToPushException, AuthenticationConfigException {
    ApplicationDetail appDetail = appLifecycleService.getLatestAppDetail(appRef, false);
    String committer = authenticationContext.getPrincipal().getName();

    // TODO CDAP-20371 revisit and put correct Author and Committer, for now they are the same
    CommitMeta commitMeta = new CommitMeta(committer, committer, System.currentTimeMillis(), commitMessage);

    PushAppResponse pushResponse = sourceControlOperationRunner.push(appRef.getParent(), appDetail, commitMeta);
    SourceControlMeta sourceControlMeta = new SourceControlMeta(pushResponse.getFileHash());
    ApplicationId appId = appRef.app(appDetail.getAppVersion());
    store.setAppSourceControlMeta(appId, sourceControlMeta);

    return pushResponse;
  }
}
