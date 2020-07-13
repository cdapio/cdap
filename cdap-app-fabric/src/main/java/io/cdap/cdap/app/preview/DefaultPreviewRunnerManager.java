/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.app.preview;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.preview.PreviewDiscoveryRuntimeModule;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.config.guice.ConfigStoreModule;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.preview.PreviewDataModules;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.internal.app.preview.PreviewRunnerService;
import io.cdap.cdap.internal.provision.ProvisionerModule;
import io.cdap.cdap.logging.appender.LogAppender;
import io.cdap.cdap.logging.appender.tms.PreviewTMSLogAppender;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.guice.preview.PreviewSecureStoreModule;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for managing {@link PreviewRunnerService}.
 */
public class DefaultPreviewRunnerManager extends AbstractIdleService implements PreviewRunnerManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultPreviewRunnerManager.class);

  private final CConfiguration previewCConf;
  private final Configuration previewHConf;
  private final SConfiguration previewSConf;
  private final int maxConcurrentPreviews;
  private final DiscoveryService discoveryService;
  private final DatasetFramework datasetFramework;
  private final SecureStore secureStore;
  private final TransactionSystemClient transactionSystemClient;
  private final PreviewRunnerModule previewRunnerModule;
  private final Set<PreviewRunnerService> previewRunnerServices;
  private final LevelDBTableService previewLevelDBTableService;
  private final PreviewRunnerServiceFactory previewRunnerServiceFactory;
  private PreviewRunner runner;

  @Inject
  DefaultPreviewRunnerManager(
    @Named(PreviewConfigModule.PREVIEW_CCONF) CConfiguration previewCConf,
    @Named(PreviewConfigModule.PREVIEW_HCONF) Configuration previewHConf,
    @Named(PreviewConfigModule.PREVIEW_SCONF) SConfiguration previewSConf,
    SecureStore secureStore, DiscoveryService discoveryService,
    @Named(DataSetsModules.BASE_DATASET_FRAMEWORK) DatasetFramework datasetFramework,
    TransactionSystemClient transactionSystemClient,
    @Named(PreviewConfigModule.PREVIEW_LEVEL_DB) LevelDBTableService previewLevelDBTableService,
    PreviewRunnerModule previewRunnerModule, PreviewRunnerServiceFactory previewRunnerServiceFactory) {
    this.previewCConf = previewCConf;
    this.previewHConf = previewHConf;
    this.previewSConf = previewSConf;
    this.datasetFramework = datasetFramework;
    this.secureStore = secureStore;
    this.discoveryService = discoveryService;
    this.transactionSystemClient = transactionSystemClient;
    this.maxConcurrentPreviews = previewCConf.getInt(Constants.Preview.CACHE_SIZE, 10);
    this.previewRunnerServices = ConcurrentHashMap.newKeySet();
    this.previewRunnerModule = previewRunnerModule;
    this.previewLevelDBTableService = previewLevelDBTableService;
    this.previewRunnerServiceFactory = previewRunnerServiceFactory;
  }

  @Override
  protected void startUp() throws Exception {
    Injector previewInjector = createPreviewInjector();
    // Starts common services
    runner = previewInjector.getInstance(PreviewRunner.class);
    if (runner instanceof Service) {
      ((Service) runner).startAndWait();
    }

    // Create and start the preview poller services.
    for (int i = 0; i < maxConcurrentPreviews; i++) {
      PreviewRunnerService pollerService = previewRunnerServiceFactory.create(runner);

      pollerService.addListener(new ServiceListenerAdapter() {
        @Override
        public void terminated(State from) {
          previewRunnerServices.remove(pollerService);
          if (previewRunnerServices.isEmpty()) {
            try {
              stop();
            } catch (Exception e) {
              // should not happen
              LOG.error("Failed to shutdown the preview runner manager service.", e);
            }
          }
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      pollerService.startAndWait();
      previewRunnerServices.add(pollerService);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    // Should stop the polling service, hence individual preview runs, before stopping the top level preview runner.
    for (Service pollerService : previewRunnerServices) {
      stopQuietly(pollerService);
    }

    if (runner instanceof Service) {
      stopQuietly((Service) runner);
    }
  }

  private void stopQuietly(Service service) {
    try {
      service.stopAndWait();
    } catch (Exception e) {
      LOG.warn("Error stopping the preview runner.", e);
    }
  }

  @Override
  public void stop(ApplicationId preview) throws Exception {
    for (PreviewRunnerService previewRunnerService : previewRunnerServices) {
      if (!preview.equals(previewRunnerService.getPreviewApplication().orElse(null))) {
        continue;
      }
      PreviewRunnerService newService = previewRunnerServiceFactory.create(runner);
      previewRunnerServices.add(newService);
      previewRunnerService.stopAndWait();
      newService.startAndWait();
      return;
    }
    throw new NotFoundException("Preview run cannot be stopped. Please try stopping again or start new preview run.");
  }

  /**
   * Create injector for the given application id.
   */
  @VisibleForTesting
  Injector createPreviewInjector() throws IOException {
    return Guice.createInjector(
      new ConfigModule(previewCConf, previewHConf, previewSConf),
      new IOModule(),
      new AuthenticationContextModules().getMasterModule(),
      new PreviewSecureStoreModule(secureStore),
      new PreviewDiscoveryRuntimeModule(discoveryService),
      new LocalLocationModule(),
      new ConfigStoreModule(),
      previewRunnerModule,
      new ProgramRunnerRuntimeModule().getStandaloneModules(),
      new PreviewDataModules().getDataFabricModule(transactionSystemClient, previewLevelDBTableService),
      new PreviewDataModules().getDataSetsModule(datasetFramework),
      new DataSetServiceModules().getStandaloneModules(),
      // Use the in-memory module for metrics collection, which metrics still get persisted to dataset, but
      // save threads for reading metrics from TMS, as there won't be metrics in TMS.
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(LogAppender.class).to(PreviewTMSLogAppender.class).in(Scopes.SINGLETON);
        }
      },
    new MessagingServerRuntimeModule().getInMemoryModules(),
      Modules.override(new MetadataReaderWriterModules().getInMemoryModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          // we don't start a metadata service in preview, so don't attempt to create any metadata
          bind(MetadataServiceClient.class).to(NoOpMetadataServiceClient.class);
        }
      }),
      new ProvisionerModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
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
}
