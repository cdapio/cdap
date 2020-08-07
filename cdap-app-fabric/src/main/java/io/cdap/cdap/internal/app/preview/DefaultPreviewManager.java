/*
 * Copyright © 2016-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.gson.JsonElement;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.preview.PreviewConfigModule;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewRequestQueue;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.preview.PreviewDiscoveryRuntimeModule;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.preview.PreviewDataModules;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.explore.client.MockExploreClient;
import io.cdap.cdap.internal.app.namespace.DefaultNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.LocalStorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.NamespaceResourceDeleter;
import io.cdap.cdap.internal.app.namespace.NoopNamespaceResourceDeleter;
import io.cdap.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
import io.cdap.cdap.internal.app.runtime.monitor.LogAppenderLogProcessor;
import io.cdap.cdap.internal.app.runtime.monitor.RemoteExecutionLogProcessor;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.app.store.preview.DefaultPreviewStore;
import io.cdap.cdap.logging.appender.LogAppender;
import io.cdap.cdap.logging.guice.LocalLogAppenderModule;
import io.cdap.cdap.logging.read.FileLogReader;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.metadata.DefaultMetadataAdmin;
import io.cdap.cdap.metadata.MetadataAdmin;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.metrics.query.MetricsQueryHelper;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.preview.PreviewConfig;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizerInstantiator;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.DefaultUGIProvider;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerStore;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.store.DefaultOwnerStore;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Class responsible for creating the injector for preview and starting it.
 */
public class DefaultPreviewManager extends AbstractIdleService implements PreviewManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultPreviewManager.class);

  private final AuthorizerInstantiator authorizerInstantiator;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final CConfiguration previewCConf;
  private final Configuration previewHConf;
  private final SConfiguration previewSConf;
  private final DiscoveryService discoveryService;
  private final DatasetFramework datasetFramework;
  private final TransactionSystemClient transactionSystemClient;
  private final LevelDBTableService previewLevelDBTableService;
  private final PreviewRequestQueue previewRequestQueue;
  private final PreviewStore previewStore;
  private final PreviewRunStopper previewRunStopper;
  private final MessagingService messagingService;
  private final PreviewDataCleanupService previewDataCleanupService;
  private Injector previewInjector;
  private PreviewDataSubscriberService dataSubscriberService;
  private PreviewTMSLogSubscriber logSubscriberService;
  private LogAppender logAppender;
  private MetricsCollectionService metricsCollectionService;

  @Inject
  DefaultPreviewManager(DiscoveryService discoveryService,
                        @Named(DataSetsModules.BASE_DATASET_FRAMEWORK) DatasetFramework datasetFramework,
                        TransactionSystemClient transactionSystemClient,
                        AuthorizerInstantiator authorizerInstantiator, AuthorizationEnforcer authorizationEnforcer,
                        @Named(PreviewConfigModule.PREVIEW_LEVEL_DB) LevelDBTableService previewLevelDBTableService,
                        @Named(PreviewConfigModule.PREVIEW_CCONF) CConfiguration previewCConf,
                        @Named(PreviewConfigModule.PREVIEW_HCONF) Configuration previewHConf,
                        @Named(PreviewConfigModule.PREVIEW_SCONF) SConfiguration previewSConf,
                        PreviewRequestQueue previewRequestQueue,
                        PreviewRunStopper previewRunStopper, MessagingService messagingService,
                        PreviewDataCleanupService previewDataCleanupService) {
    this.previewCConf = previewCConf;
    this.previewHConf = previewHConf;
    this.previewSConf = previewSConf;
    this.datasetFramework = datasetFramework;
    this.discoveryService = discoveryService;
    this.transactionSystemClient = transactionSystemClient;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authorizerInstantiator = authorizerInstantiator;
    this.previewLevelDBTableService = previewLevelDBTableService;
    this.previewRequestQueue = previewRequestQueue;
    this.previewStore = new DefaultPreviewStore(previewLevelDBTableService);
    this.previewRunStopper = previewRunStopper;
    this.messagingService = messagingService;
    this.previewDataCleanupService = previewDataCleanupService;
  }

  @Override
  protected void startUp() throws Exception {
    previewInjector = createPreviewInjector();
    StoreDefinition.createAllTables(previewInjector.getInstance(StructuredTableAdmin.class),
                                    previewInjector.getInstance(StructuredTableRegistry.class), false);
    metricsCollectionService = previewInjector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.start();
    logAppender = previewInjector.getInstance(LogAppender.class);
    logAppender.start();
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.PREVIEW_HTTP));
    logSubscriberService = previewInjector.getInstance(PreviewTMSLogSubscriber.class);
    logSubscriberService.startAndWait();
    dataSubscriberService = previewInjector.getInstance(PreviewDataSubscriberService.class);
    dataSubscriberService.startAndWait();
    previewDataCleanupService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    stopQuietly(previewDataCleanupService);
    stopQuietly(dataSubscriberService);
    stopQuietly(logSubscriberService);
    logAppender.stop();
    stopQuietly(metricsCollectionService);
    previewLevelDBTableService.close();
  }

  @Override
  public ApplicationId start(NamespaceId namespace, AppRequest<?> appRequest) throws Exception {
    // make sure preview id is unique for each run
    ApplicationId previewApp = namespace.app(RunIds.generate().getId());
    ProgramId programId = getProgramIdFromRequest(previewApp, appRequest);
    PreviewRequest previewRequest = new PreviewRequest(programId, appRequest);

    if (state() != State.RUNNING) {
      throw new IllegalStateException("Preview service is not running. Cannot start preview for " + programId);
    }

    previewRequestQueue.add(previewRequest);
    return previewApp;
  }

  @Override
  public PreviewStatus getStatus(ApplicationId applicationId) throws NotFoundException {
    PreviewStatus status = previewStore.getPreviewStatus(applicationId);
    if (status == null) {
      throw new NotFoundException(applicationId);
    }
    if (status.getStatus() == PreviewStatus.Status.WAITING) {
      int position = previewRequestQueue.positionOf(applicationId);
      if (position == -1) {
        // status is WAITING but application is not present in in-memory queue
        position = 0;
      }
      status = new PreviewStatus(status.getStatus(), status.getThrowable(), status.getStartTime(),
                                 status.getEndTime(), position);
    }
    return status;
  }

  @Override
  public void stopPreview(ApplicationId applicationId) throws Exception {
    PreviewStatus status = getStatus(applicationId);
    if (status.getStatus().isEndState()) {
      throw new BadRequestException(String.format("Preview run cannot be stopped. It is already in %s state.",
                                                  status.getStatus().name()));
    }
    if (status.getStatus() == PreviewStatus.Status.WAITING) {
      previewStore.setPreviewStatus(applicationId, new PreviewStatus(PreviewStatus.Status.KILLED, null, null, null));
      return;
    }
    previewRunStopper.stop(applicationId);
  }

  @Override
  public Map<String, List<JsonElement>> getData(ApplicationId applicationId, String tracerName) {
    return previewStore.get(applicationId, tracerName);
  }

  @Override
  public ProgramRunId getRunId(ApplicationId applicationId) throws Exception {
    ProgramRunId programRunId = previewStore.getProgramRunId(applicationId);
    if (programRunId == null) {
      throw new NotFoundException(applicationId);
    }
    return programRunId;
  }

  @Override
  public MetricsQueryHelper getMetricsQueryHelper() {
    return previewInjector.getInstance(MetricsQueryHelper.class);
  }

  @Override
  public LogReader getLogReader() {
    return previewInjector.getInstance(LogReader.class);
  }

  @Override
  public Optional<PreviewRequest> poll(@Nullable byte[] pollerInfo) {
    return previewInjector.getInstance(PreviewRequestQueue.class).poll(pollerInfo);
  }

  /**
   * Create injector for the given application id.
   */
  @VisibleForTesting
  Injector createPreviewInjector() {
    return Guice.createInjector(
      new ConfigModule(previewCConf, previewHConf, previewSConf),
      new PreviewDataModules().getDataFabricModule(transactionSystemClient, previewLevelDBTableService),
      new PreviewDataModules().getDataSetsModule(datasetFramework),
      new AuthenticationContextModules().getMasterModule(),
      new LocalLocationModule(),
      new PreviewDiscoveryRuntimeModule(discoveryService),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new DataSetServiceModules().getStandaloneModules(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      Modules.override(new MetadataReaderWriterModules().getInMemoryModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          // we don't start a metadata service in preview, so don't attempt to create any metadata
          bind(MetadataServiceClient.class).to(NoOpMetadataServiceClient.class);
        }
      }),
      new LocalLogAppenderModule(),
      new PrivateModule() {
        @Override
        protected void configure() {
          bind(AuthorizerInstantiator.class).toInstance(authorizerInstantiator);
          expose(AuthorizerInstantiator.class);
          bind(AuthorizationEnforcer.class).toInstance(authorizationEnforcer);
          expose(AuthorizationEnforcer.class);

          bind(PreviewStore.class).toInstance(previewStore);
          expose(PreviewStore.class);

          bind(PreviewRequestQueue.class).toInstance(previewRequestQueue);
          expose(PreviewRequestQueue.class);

          bind(Store.class).to(DefaultStore.class);
          bind(OwnerStore.class).to(DefaultOwnerStore.class);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
          expose(OwnerAdmin.class);

          bind(UGIProvider.class).to(DefaultUGIProvider.class);
          expose(UGIProvider.class);

          bind(ExploreClient.class).to(MockExploreClient.class);
          expose(ExploreClient.class);
          bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);

          bind(NamespaceResourceDeleter.class).to(NoopNamespaceResourceDeleter.class).in(Scopes.SINGLETON);
          bind(NamespaceAdmin.class).to(DefaultNamespaceAdmin.class).in(Scopes.SINGLETON);
          bind(NamespaceQueryAdmin.class).to(DefaultNamespaceAdmin.class).in(Scopes.SINGLETON);
          expose(NamespaceAdmin.class);
          expose(NamespaceQueryAdmin.class);

          bind(LogReader.class).to(FileLogReader.class).in(Scopes.SINGLETON);
          expose(LogReader.class);

          bind(MetadataAdmin.class).to(DefaultMetadataAdmin.class);
          expose(MetadataAdmin.class);

          bind(MessagingService.class)
            .annotatedWith(Names.named(PreviewConfigModule.GLOBAL_TMS))
            .toInstance(messagingService);
          expose(MessagingService.class).annotatedWith(Names.named(PreviewConfigModule.GLOBAL_TMS));
        }
      },
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(LevelDBTableService.class).toInstance(previewLevelDBTableService);
          bind(RemoteExecutionLogProcessor.class).to(LogAppenderLogProcessor.class).in(Scopes.SINGLETON);
        }

        @Provides
        @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS)
        @SuppressWarnings("unused")
        public InetAddress providesHostname(CConfiguration cConf) {
          String address = cConf.get(Constants.Preview.ADDRESS);
          return Networks.resolve(address, new InetSocketAddress("localhost", 0).getAddress());
        }
      }
    );
  }

  private ProgramId getProgramIdFromRequest(ApplicationId preview, AppRequest<?> request) throws BadRequestException {
    PreviewConfig previewConfig = request.getPreview();
    if (previewConfig == null) {
      throw new BadRequestException("Preview config cannot be null");
    }

    String programName = previewConfig.getProgramName();
    ProgramType programType = previewConfig.getProgramType();

    if (programName == null || programType == null) {
      throw new IllegalArgumentException("ProgramName or ProgramType cannot be null.");
    }

    return preview.program(programType, programName);
  }

  private void stopQuietly(Service service) {
    try {
      service.stopAndWait();
    } catch (Exception e) {
      LOG.warn("Exception when stopping service {}", service, e);
    }
  }
}
