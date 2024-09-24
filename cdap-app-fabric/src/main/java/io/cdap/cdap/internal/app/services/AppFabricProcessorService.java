/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.bootstrap.BootstrapService;
import io.cdap.cdap.internal.credential.CredentialProviderService;
import io.cdap.cdap.internal.namespace.credential.NamespaceCredentialProviderService;
import io.cdap.cdap.internal.operation.OperationNotificationSubscriberService;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.internal.sysapp.SystemAppManagementService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.scheduler.CoreSchedulerService;
import io.cdap.cdap.scheduler.ScheduleNotificationSubscriberService;
import io.cdap.cdap.sourcecontrol.RepositoryCleanupService;
import io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.spi.data.transaction.TxCallable;
import io.cdap.cdap.store.DefaultNamespaceStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AppFabric Server.
 */
public class AppFabricProcessorService extends AbstractIdleService {

  // TODO: Use Logger
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricProcessorService.class);

  private final ProgramRuntimeService programRuntimeService;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ProgramNotificationSubscriberService programNotificationSubscriberService;
  private final ProgramStopSubscriberService programStopSubscriberService;
  private final RunRecordCorrectorService runRecordCorrectorService;
  private final RunDataTimeToLiveService runDataTimeToLiveService;
  private final ProgramRunStatusMonitorService programRunStatusMonitorService;
  private final RunRecordMonitorService runRecordCounterService;
  private final CoreSchedulerService coreSchedulerService;
  private final CredentialProviderService credentialProviderService;
  private final NamespaceCredentialProviderService namespaceCredentialProviderService;
  private final ProvisioningService provisioningService;
  private final BootstrapService bootstrapService;
  private final SystemAppManagementService systemAppManagementService;
  private final SourceControlOperationRunner sourceControlOperationRunner;
  private final RepositoryCleanupService repositoryCleanupService;
  private final OperationNotificationSubscriberService operationNotificationSubscriberService;
  private final ScheduleNotificationSubscriberService scheduleNotificationSubscriberService;
  private final CConfiguration cConf;
  private final TransactionRunner transactionRunner;
  private MetricsCollectionService metricsCollectionService;

  /**
   * Construct the AppFabricServer with service factory and cConf coming from guice injection.
   */
  @Inject
  public AppFabricProcessorService(CConfiguration cConf,
      @Nullable MetricsCollectionService metricsCollectionService,
      ProgramRuntimeService programRuntimeService,
      RunRecordCorrectorService runRecordCorrectorService,
      ProgramRunStatusMonitorService programRunStatusMonitorService,
      ApplicationLifecycleService applicationLifecycleService,
      ProgramNotificationSubscriberService programNotificationSubscriberService,
      ProgramStopSubscriberService programStopSubscriberService,
      CoreSchedulerService coreSchedulerService,
      CredentialProviderService credentialProviderService,
      NamespaceCredentialProviderService namespaceCredentialProviderService,
      ProvisioningService provisioningService,
      BootstrapService bootstrapService,
      SystemAppManagementService systemAppManagementService,
      TransactionRunner transactionRunner,
      RunRecordMonitorService runRecordCounterService,
      RunDataTimeToLiveService runDataTimeToLiveService,
      SourceControlOperationRunner sourceControlOperationRunner,
      RepositoryCleanupService repositoryCleanupService,
      OperationNotificationSubscriberService operationNotificationSubscriberService,
      ScheduleNotificationSubscriberService scheduleNotificationSubscriberService) {
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
    this.programRuntimeService = programRuntimeService;
    this.applicationLifecycleService = applicationLifecycleService;
    this.programNotificationSubscriberService = programNotificationSubscriberService;
    this.programStopSubscriberService = programStopSubscriberService;
    this.runRecordCorrectorService = runRecordCorrectorService;
    this.programRunStatusMonitorService = programRunStatusMonitorService;
    this.coreSchedulerService = coreSchedulerService;
    this.credentialProviderService = credentialProviderService;
    this.namespaceCredentialProviderService = namespaceCredentialProviderService;
    this.provisioningService = provisioningService;
    this.bootstrapService = bootstrapService;
    this.systemAppManagementService = systemAppManagementService;
    this.transactionRunner = transactionRunner;
    this.runRecordCounterService = runRecordCounterService;
    this.runDataTimeToLiveService = runDataTimeToLiveService;
    this.sourceControlOperationRunner = sourceControlOperationRunner;
    this.repositoryCleanupService = repositoryCleanupService;
    this.operationNotificationSubscriberService = operationNotificationSubscriberService;
    this.scheduleNotificationSubscriberService = scheduleNotificationSubscriberService;
  }

  /**
   * Configures the AppFabricService pre-start.
   */
  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(
        new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
            Constants.Logging.COMPONENT_NAME,
            Constants.Service.APP_FABRIC_HTTP));
    List<ListenableFuture<State>> futuresList = new ArrayList<>();
    FeatureFlagsProvider featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
    if (Feature.NAMESPACED_SERVICE_ACCOUNTS.isEnabled(featureFlagsProvider)) {
      futuresList.add(namespaceCredentialProviderService.start());
    }
    futuresList.addAll(ImmutableList.of(
        provisioningService.start(),
        applicationLifecycleService.start(),
        bootstrapService.start(),
        programRuntimeService.start(),
        programNotificationSubscriberService.start(),
        programStopSubscriberService.start(),
        runRecordCorrectorService.start(),
        programRunStatusMonitorService.start(),
        scheduleNotificationSubscriberService.start(),
        coreSchedulerService.start(),
        credentialProviderService.start(),
        runRecordCounterService.start(),
        runDataTimeToLiveService.start(),
        sourceControlOperationRunner.start(),
        repositoryCleanupService.start(),
        operationNotificationSubscriberService.start()
    ));
    Futures.allAsList(futuresList).get();

    long applicationCount = TransactionRunners.run(transactionRunner,
        (TxCallable<Long>) context ->
            AppMetadataStore.create(context).getApplicationCount());
    long namespaceCount = new DefaultNamespaceStore(transactionRunner).getNamespaceCount();

    metricsCollectionService.getContext(Collections.emptyMap())
        .gauge(Constants.Metrics.Program.APPLICATION_COUNT,
            applicationCount);
    metricsCollectionService.getContext(Collections.emptyMap())
        .gauge(Constants.Metrics.Program.NAMESPACE_COUNT,
            namespaceCount);
  }

  @Override
  protected void shutDown() throws Exception {
    scheduleNotificationSubscriberService.stopAndWait();
    coreSchedulerService.stopAndWait();
    bootstrapService.stopAndWait();
    systemAppManagementService.stopAndWait();
    programRuntimeService.stopAndWait();
    applicationLifecycleService.stopAndWait();
    programNotificationSubscriberService.stopAndWait();
    programStopSubscriberService.stopAndWait();
    runRecordCorrectorService.stopAndWait();
    programRunStatusMonitorService.stopAndWait();
    provisioningService.stopAndWait();
    runRecordCounterService.stopAndWait();
    runDataTimeToLiveService.stopAndWait();
    sourceControlOperationRunner.stopAndWait();
    repositoryCleanupService.stopAndWait();
    credentialProviderService.stopAndWait();
    namespaceCredentialProviderService.stopAndWait();
    operationNotificationSubscriberService.stopAndWait();
  }
}
