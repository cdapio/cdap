/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.AuthorizationModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.app.guice.TwillModule;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.gateway.handlers.meta.RemoteSystemOperationsService;
import co.cask.cdap.gateway.handlers.meta.RemoteSystemOperationsServiceModule;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metrics.store.DefaultMetricDatasetFactory;
import co.cask.cdap.metrics.store.DefaultMetricStore;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationEnforcementService;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Provides a {@link DatasetService} which uses {@link RemoteDatasetFramework}.
 * This is used to independently start a remote dataset service during upgrade. This is needed to perform upgrade steps
 * which needs access to user datasets.
 * Note: This should not be used outside upgrade tool. This class also creates its own injector from scratch which is
 * needed because it talks to the remote dataset framework which is not same as what is used  in upgrade tool
 * {@link UpgradeTool#createInjector()}. This is bad as it creates a lot of confusion while doing changes in guice
 * injection which affects UpgradeTool and should reuse common binding from upgrade tool. (CDAP-6506)
 */
class UpgradeDatasetServiceManager extends AbstractIdleService {

  private final DatasetService datasetService;
  private final ZKClientService zkClientService;
  private final DatasetFramework datasetFramework;
  private final DatasetOpExecutorService datasetOpExecutorService;
  private final RemoteSystemOperationsService remoteSystemOperationsService;
  private final MetadataStore metadataStore;

  @Inject
  UpgradeDatasetServiceManager(CConfiguration cConf, Configuration hConf,
                               AuthorizationEnforcementService authorizationEnforcementService) {
    Injector injector = createInjector(cConf, hConf, authorizationEnforcementService);
    this.datasetService = injector.getInstance(DatasetService.class);
    this.zkClientService = injector.getInstance(ZKClientService.class);
    this.datasetFramework = injector.getInstance(DatasetFramework.class);
    this.datasetOpExecutorService = injector.getInstance(DatasetOpExecutorService.class);
    this.remoteSystemOperationsService = injector.getInstance(RemoteSystemOperationsService.class);
    this.metadataStore = injector.getInstance(MetadataStore.class);
  }

  DatasetFramework getDSFramework() {
    return datasetFramework;
  }

  MetadataStore getMetadataStore() {
    return metadataStore;
  }

  @Override
  protected void startUp() throws Exception {
    if (!zkClientService.isRunning()) {
      zkClientService.startAndWait();
    }
    datasetOpExecutorService.startAndWait();
    remoteSystemOperationsService.startAndWait();
    datasetService.startAndWait();

    // wait 5 minutes for DatasetService to start up
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          getDSFramework().getInstances(NamespaceId.DEFAULT);
          return true;
        } catch (ServiceUnavailableException sue) {
          return false;
        }
      }
    }, 5, TimeUnit.MINUTES, 10, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      datasetService.stopAndWait();
      remoteSystemOperationsService.stopAndWait();
      datasetOpExecutorService.startAndWait();
      zkClientService.stopAndWait();
    } catch (Throwable e) {
      throw Throwables.propagate(e);
    }
  }

  private Injector createInjector(CConfiguration cConf, Configuration hConf,
                                  final AuthorizationEnforcementService authorizationEnforcementService) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new IOModule(),
      new KafkaClientModule(),
      new MessagingClientModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataSetServiceModules().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new ExploreClientModule(),
      new RemoteSystemOperationsServiceModule(),
      new SecureStoreModules().getDistributedModules(),
      new AuthorizationModule(),
      // we override the AuthorizationEnforcementService from the one which has been started in the upgrade tool to
      // reuse it. (CDAP-6506)
      Modules.override(new AuthorizationEnforcementModule().getMasterModule()).with(new AbstractModule() {
        @Override
        protected void configure() {
          bind(AuthorizationEnforcementService.class).toInstance(authorizationEnforcementService);
        }
      }),
      new AuthenticationContextModules().getMasterModule(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new TwillModule(),
      new ViewAdminModules().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new ServiceStoreModules().getDistributedModules(),
      new NamespaceStoreModule().getDistributedModules(),
      // don't need real notifications for upgrade, so use the in-memory implementations
      new NotificationServiceRuntimeModule().getInMemoryModules(),
      new NotificationFeedClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // the DataFabricDistributedModule needs MetricsCollectionService binding and since Upgrade tool does not do
          // anything with Metrics we just bind it to NoOpMetricsCollectionService
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
          bind(MetricDatasetFactory.class).to(DefaultMetricDatasetFactory.class).in(Scopes.SINGLETON);
          bind(MetricStore.class).to(DefaultMetricStore.class);

        }
      }
    );
  }
}
