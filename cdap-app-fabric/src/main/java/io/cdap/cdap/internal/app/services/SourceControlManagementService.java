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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.app.store.ScanSourceControlMetadataRequest;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.common.TooManyRequestsException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.Metrics.SourceControlManagement;
import io.cdap.cdap.common.conf.Constants.Metrics.Tag;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.sourcecontrol.PullAppsRequest;
import io.cdap.cdap.internal.app.sourcecontrol.PushAppsRequest;
import io.cdap.cdap.internal.app.sourcecontrol.SourceControlMetadataRefreshService;
import io.cdap.cdap.internal.app.store.RepositorySourceControlMetadataStore;
import io.cdap.cdap.internal.operation.OperationLifecycleManager;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.SourceControlMetadataRecord;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.operation.OperationRun;
import io.cdap.cdap.proto.security.NamespacePermission;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.proto.sourcecontrol.PullMultipleAppsRequest;
import io.cdap.cdap.proto.sourcecontrol.PushMultipleAppsRequest;
import io.cdap.cdap.proto.sourcecontrol.RemoteRepositoryValidationException;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.NoChangesToPullException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.SourceControlConfig;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAppResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppMeta;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppsResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.NamespaceTable;
import io.cdap.cdap.store.RepositoryTable;
import java.io.IOException;
import java.time.Clock;
import java.util.HashSet;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that manages source control for repositories and applications. It exposes repository CRUD
 * apis and source control tasks that do pull/pull/list applications in linked repository.
 */
public class SourceControlManagementService {

  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final TransactionRunner transactionRunner;
  private final CConfiguration cConf;
  private final SecureStore secureStore;
  private final SourceControlOperationRunner sourceControlOperationRunner;
  private final ApplicationLifecycleService appLifecycleService;
  private final Store store;
  private final OperationLifecycleManager operationLifecycleManager;
  private final MetricsCollectionService metricsCollectionService;
  private final Clock clock;
  private final FeatureFlagsProvider featureFlagsProvider;
  private final SourceControlMetadataRefreshService sourceControlMetadataRefreshService;
  private static final Logger LOG = LoggerFactory.getLogger(SourceControlManagementService.class);


  /**
   /**
   * Constructor for SourceControlManagementService with all params injected via guice.
   */
  @Inject
  public SourceControlManagementService(CConfiguration cConf,
      SecureStore secureStore,
      TransactionRunner transactionRunner,
      AccessEnforcer accessEnforcer,
      AuthenticationContext authenticationContext,
      SourceControlOperationRunner sourceControlOperationRunner,
      ApplicationLifecycleService applicationLifecycleService,
      Store store,
      OperationLifecycleManager operationLifecycleManager,
      MetricsCollectionService metricsCollectionService,
      SourceControlMetadataRefreshService sourceControlMetadataRefreshService) {
    this (cConf, secureStore, transactionRunner,
        accessEnforcer, authenticationContext,
        sourceControlOperationRunner, applicationLifecycleService,
        store, operationLifecycleManager, metricsCollectionService, Clock.systemUTC(),
        sourceControlMetadataRefreshService);
  }

  @VisibleForTesting
  SourceControlManagementService(CConfiguration cConf,
      SecureStore secureStore,
      TransactionRunner transactionRunner,
      AccessEnforcer accessEnforcer,
      AuthenticationContext authenticationContext,
      SourceControlOperationRunner sourceControlOperationRunner,
      ApplicationLifecycleService applicationLifecycleService,
      Store store,
      OperationLifecycleManager operationLifecycleManager,
      MetricsCollectionService metricsCollectionService,
      Clock clock,
      SourceControlMetadataRefreshService sourceControlMetadataRefreshService) {
    this.cConf = cConf;
    this.secureStore = secureStore;
    this.transactionRunner = transactionRunner;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.sourceControlOperationRunner = sourceControlOperationRunner;
    this.appLifecycleService = applicationLifecycleService;
    this.store = store;
    this.operationLifecycleManager = operationLifecycleManager;
    this.metricsCollectionService = metricsCollectionService;
    this.clock = clock;
    this.featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
    this.sourceControlMetadataRefreshService = sourceControlMetadataRefreshService;
  }

  private RepositoryTable getRepositoryTable(StructuredTableContext context)
      throws TableNotFoundException {
    return new RepositoryTable(context);
  }

  private NamespaceTable getNamespaceTable(StructuredTableContext context)
      throws TableNotFoundException {
    return new NamespaceTable(context);
  }

  private RepositorySourceControlMetadataStore getRepoSourceControlMetadataStore(
      StructuredTableContext context) {
    return RepositorySourceControlMetadataStore.create(context);
  }

  /**
   * Add a repository config to a namespace.
   *
   * @param namespace {@link NamespaceId} to link repository to
   * @param repository {@link RepositoryConfig} for the linked repository
   * @return {@link RepositoryMeta} representing the linked repository
   * @throws NamespaceNotFoundException if the namespace is non-existent
   */
  public RepositoryMeta setRepository(NamespaceId namespace, RepositoryConfig repository)
      throws NamespaceNotFoundException {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(),
        NamespacePermission.UPDATE_REPOSITORY_METADATA);

    return TransactionRunners.run(transactionRunner, context -> {
      NamespaceTable nsTable = getNamespaceTable(context);
      if (nsTable.get(namespace) == null) {
        throw new NamespaceNotFoundException(namespace);
      }

      RepositoryTable repoTable = getRepositoryTable(context);
      repoTable.create(namespace, repository);
      sourceControlMetadataRefreshService.addRefreshService(namespace);
      return repoTable.get(namespace);
    }, NamespaceNotFoundException.class);
  }

  /**
   * Delete the current repository config from namespace.
   *
   * @param namespace {@link NamespaceId} do unlink repository from
   */
  public void deleteRepository(NamespaceId namespace) {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(),
        NamespacePermission.UPDATE_REPOSITORY_METADATA);

    TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable repoTable = getRepositoryTable(context);
      repoTable.delete(namespace);
      sourceControlMetadataRefreshService.removeRefreshService(namespace);
    });

  }

  /**
   * Return the repository config for the given namespace.
   *
   * @throws RepositoryNotFoundException if repository config is not available for the
   *     namespace
   */
  public RepositoryMeta getRepositoryMeta(NamespaceId namespace)
      throws RepositoryNotFoundException {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(), StandardPermission.GET);

    return TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable table = getRepositoryTable(context);
      RepositoryMeta repoMeta = table.get(namespace);
      if (repoMeta == null) {
        throw new RepositoryNotFoundException(namespace);
      }

      return repoMeta;
    }, RepositoryNotFoundException.class);
  }

  /**
   * Validates the remote repository config for the given namespace.
   *
   * @throws RemoteRepositoryValidationException if validation of remote repository fails
   */
  public void validateRepository(NamespaceId namespace, RepositoryConfig repoConfig)
      throws RemoteRepositoryValidationException {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(),
        NamespacePermission.UPDATE_REPOSITORY_METADATA);
    RepositoryManager.validateConfig(secureStore,
        new SourceControlConfig(namespace, repoConfig, cConf));
  }

  /**
   * The method to push an application to linked repository.
   *
   * @param appRef {@link ApplicationReference}
   * @param commitMessage enforced commit message from user
   * @return {@link PushAppsResponse}
   * @throws NotFoundException if the application is not found or the repository config is not
   *     found
   * @throws IOException if {@link ApplicationLifecycleService} fails to get the adminOwner
   *     store
   * @throws SourceControlException if {@link SourceControlOperationRunner} fails to push
   * @throws AuthenticationConfigException if the repository configuration authentication fails
   * @throws NoChangesToPushException if there's no change of the application between namespace
   *     and linked repository
   */
  public PushAppsResponse pushApp(ApplicationReference appRef, String commitMessage)
      throws NotFoundException, IOException, NoChangesToPushException, AuthenticationConfigException {
    MetricsContext metricsContext = getMetricContext(appRef.getNamespaceId());
    metricsContext.increment(SourceControlManagement.PUSH_OPERATION_COUNT, 1);
    metricsContext.increment(SourceControlManagement.PUSH_APP_COUNT, 1);

    accessEnforcer.enforce(appRef.getParent(), authenticationContext.getPrincipal(),
        NamespacePermission.WRITE_REPOSITORY);

    // TODO: CDAP-20396 RepositoryConfig is currently only accessible from the service layer
    //  Need to fix it and avoid passing it in RepositoryManagerFactory
    RepositoryConfig repoConfig = getRepositoryMeta(appRef.getParent()).getConfig();

    // AppLifecycleService already enforces ApplicationDetail Access
    ApplicationDetail appDetail = appLifecycleService.getLatestAppDetail(appRef, false);

    String committer = appLifecycleService.decodeUserId(authenticationContext);
    // TODO CDAP-20371 revisit and put correct Author and Committer, for now they are the same
    CommitMeta commitMeta = new CommitMeta(committer, committer, System.currentTimeMillis(),
        commitMessage);

    LOG.info("Start to push app {} in namespace {} to linked repository by user {}",
        appRef.getApplication(),
        appRef.getParent(),
        appLifecycleService.decodeUserId(authenticationContext));

    PushAppsResponse pushResponse = sourceControlOperationRunner.push(
        new PushAppOperationRequest(appRef.getParent(), repoConfig, appDetail, commitMeta)
    );

    LOG.info("Successfully pushed app {} in namespace {} to linked repository by user {}",
        appRef.getApplication(),
        appRef.getParent(),
        appLifecycleService.decodeUserId(authenticationContext));

    // pushResponse should have exactly one element
    PushAppMeta appMeta = pushResponse.getApps().iterator().next();
    SourceControlMeta sourceControlMeta = new SourceControlMeta(appMeta.getFileHash(),
        pushResponse.getCommitId(), clock.instant());
    ApplicationId appId = appRef.app(appDetail.getAppVersion());
    store.setAppSourceControlMeta(appId.getAppReference(), sourceControlMeta);

    return pushResponse;
  }

  /**
   * Pull the application from linked repository and deploy it in current namespace.
   *
   * @param appRef application reference to deploy with
   * @return {@link ApplicationRecord} of the deployed application.
   * @throws Exception when {@link ApplicationLifecycleService} fails to deploy.
   * @throws NoChangesToPullException if the fileHashes are the same
   * @throws NotFoundException if the repository config is not found or the application in
   *     repository is not found
   * @throws SourceControlException if unexpected errors happen when pulling the application.
   * @throws AuthenticationConfigException if the repository configuration authentication fails
   */
  public ApplicationRecord pullAndDeploy(ApplicationReference appRef) throws Exception {
    MetricsContext metricsContext = getMetricContext(appRef.getNamespaceId());
    metricsContext.increment(SourceControlManagement.PULL_OPERATION_COUNT, 1);
    metricsContext.increment(SourceControlManagement.PULL_APP_COUNT, 1);
    // Deploy with a generated uuid
    String versionId = RunIds.generate().getId();
    ApplicationId appId = appRef.app(versionId);
    // Enforcing application CREATE permission and Namespace READ_REPOSITORY permission upfront
    accessEnforcer.enforce(appId, authenticationContext.getPrincipal(), StandardPermission.CREATE);
    accessEnforcer.enforce(appRef.getParent(), authenticationContext.getPrincipal(),
        NamespacePermission.READ_REPOSITORY);

    PullAppResponse<?> pullResponse = pullAndValidateApplication(appRef);

    AppRequest<?> appRequest = pullResponse.getAppRequest();
    SourceControlMeta sourceControlMeta = new SourceControlMeta(
        pullResponse.getApplicationFileHash(), pullResponse.getCommitId(), clock.instant());

    LOG.info("Start to deploy app {} in namespace {} by user {}",
        appId.getApplication(),
        appId.getParent(),
        appLifecycleService.decodeUserId(authenticationContext));

    ApplicationWithPrograms app = appLifecycleService.deployApp(appId, appRequest,
        sourceControlMeta, x -> {
        }, false);

    LOG.info(
        "Successfully deployed app {} in namespace {} from artifact {} with configuration {} and "
            + "principal {}", app.getApplicationId().getApplication(),
        app.getApplicationId().getNamespace(),
        app.getArtifactId(), appRequest.getConfig(), app.getOwnerPrincipal()
    );

    return new ApplicationRecord(
        ArtifactSummary.from(app.getArtifactId().toApiArtifactId()),
        app.getApplicationId().getApplication(),
        app.getApplicationId().getVersion(),
        app.getSpecification().getDescription(),
        Optional.ofNullable(app.getOwnerPrincipal()).map(KerberosPrincipalId::getPrincipal)
            .orElse(null),
        app.getChangeDetail(), app.getSourceControlMeta());
  }

  /**
   * Pull the application from repository, look up the fileHash in store and compare it with the
   * cone in repository.
   *
   * @param appRef {@link ApplicationReference} to fetch the application with
   * @return {@link PullAppResponse}
   * @throws NoChangesToPullException if the fileHashes are the same
   * @throws NotFoundException if the repository config is not found or the application in
   *     repository is not found
   * @throws SourceControlException if unexpected errors happen when pulling the application.
   * @throws AuthenticationConfigException if the repository configuration authentication fails
   */
  private PullAppResponse<?> pullAndValidateApplication(ApplicationReference appRef)
      throws NoChangesToPullException, NotFoundException, AuthenticationConfigException {
    RepositoryConfig repoConfig = getRepositoryMeta(appRef.getParent()).getConfig();
    SourceControlMeta latestMeta = store.getAppSourceControlMeta(appRef);
    PullAppResponse<?> pullResponse = sourceControlOperationRunner.pull(
        new PullAppOperationRequest(appRef, repoConfig));

    if (latestMeta != null && latestMeta.getFileHash() != null
        && latestMeta.getFileHash().equals(pullResponse.getApplicationFileHash())) {
      throw new NoChangesToPullException(
          String.format("Pipeline deployment was not successful because there is "
              + "no new change for the pulled application: %s", appRef));
    }
    return pullResponse;
  }

  /**
   * Scans repository source control metadata and processes it in batches.
   *
   * @param request     The request specifying the metadata to scan.
   * @param txBatchSize The transaction batch size for processing metadata in each iteration.
   * @param consumer    The consumer to process the scanned repository metadata records.
   * @return True if the page limit has reached, false otherwise.
   * @throws IOException If an I/O error occurs during the scanning process.
   */
  public boolean scanRepoMetadata(ScanSourceControlMetadataRequest request, int txBatchSize,
      Consumer<SourceControlMetadataRecord> consumer) throws IOException {
    NamespaceId namespaceId = new NamespaceId(request.getNamespace());

    // Triggering source control metadata refresh service
    if (isSourceControlMetadataManualRefreshFlagEnabled()) {
      sourceControlMetadataRefreshService.runRefreshService(namespaceId);
    }

    // Getting repo files
    String lastKey = request.getScanAfter();
    int currentLimit = request.getLimit();

    while (currentLimit > 0) {
      int maxLimit = Math.min(txBatchSize, currentLimit);
      ScanSourceControlMetadataRequest batchRequest = ScanSourceControlMetadataRequest
          .builder(request)
          .setScanAfter(lastKey)
          .setLimit(maxLimit)
          .build();

      request = batchRequest;

      int count = TransactionRunners.run(transactionRunner, context -> {
        return store.scanRepositorySourceControlMetadata(batchRequest, consumer);
      }, IOException.class);

      if (count < maxLimit) {
        return false;
      }
      currentLimit -= txBatchSize;
    }

    return true;
  }

  public Long getLastRefreshTime(String namespace) {
    return sourceControlMetadataRefreshService.getLastRefreshTime(new NamespaceId(namespace));
  }

  /**
   * The method to push multiple applications in the same namespace to the linked repository.
   *
   * @param namespace {@link NamespaceId} from where the apps are to be pushed
   * @param request {@link PushMultipleAppsRequest} containing the appIds and the commit
   *     message
   * @return {@link OperationRun} of the operation to push the apps
   * @throws NotFoundException when the repository or any of the apps are not found
   */
  public OperationRun pushApps(NamespaceId namespace, PushMultipleAppsRequest request)
      throws NotFoundException, IOException, TooManyRequestsException {
    MetricsContext metricsContext = getMetricContext(namespace);
    metricsContext.increment(SourceControlManagement.PUSH_OPERATION_COUNT, 1);
    metricsContext.increment(SourceControlManagement.PUSH_APP_COUNT, request.getApps().size());
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(),
        NamespacePermission.WRITE_REPOSITORY);
    RepositoryConfig repoConfig = getRepositoryMeta(namespace).getConfig();
    String principal = appLifecycleService.decodeUserId(authenticationContext);
    CommitMeta commitMeta = new CommitMeta(principal, principal, System.currentTimeMillis(),
        request.getCommitMessage());
    PushAppsRequest pushOpRequest = new PushAppsRequest(new HashSet<>(request.getApps()),
        repoConfig, commitMeta);
    return operationLifecycleManager.createPushOperation(namespace.getNamespace(),
        RunIds.generate().getId(), pushOpRequest, principal);
  }

  /**
   * The method to pull multiple applications from the linked repository and deploy them in current
   * namespace.
   *
   * @param namespace {@link NamespaceId} from where the apps are to be pushed
   * @param request {@link PullMultipleAppsRequest} containing the appIds
   * @return {@link OperationRun} of the operation to push the apps
   * @throws NotFoundException when the repository or any of the apps are not found
   */
  public OperationRun pullApps(NamespaceId namespace, PullMultipleAppsRequest request)
      throws NotFoundException, IOException, TooManyRequestsException {
    MetricsContext metricsContext = getMetricContext(namespace);
    metricsContext.increment(SourceControlManagement.PULL_OPERATION_COUNT, 1);
    metricsContext.increment(SourceControlManagement.PULL_APP_COUNT, request.getApps().size());
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(),
        NamespacePermission.READ_REPOSITORY);
    RepositoryConfig repoConfig = getRepositoryMeta(namespace).getConfig();
    String principal = authenticationContext.getPrincipal().getName();
    PullAppsRequest pullOpRequest = new PullAppsRequest(new HashSet<>(request.getApps()),
        repoConfig);
    return operationLifecycleManager.createPullOperation(namespace.getNamespace(),
        RunIds.generate().getId(), pullOpRequest, principal);
  }

  private MetricsContext getMetricContext(NamespaceId namespace) {
    return metricsCollectionService.getContext(
        ImmutableMap.of(Tag.NAMESPACE, namespace.getNamespace()));
  }

  private boolean isSourceControlMetadataManualRefreshFlagEnabled() {
    return Feature.SOURCE_CONTROL_METADATA_MANUAL_REFRESH.isEnabled(featureFlagsProvider);
  }
}
