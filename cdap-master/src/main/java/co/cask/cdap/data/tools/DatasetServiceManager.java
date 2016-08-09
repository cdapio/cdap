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

import co.cask.cdap.app.guice.AuthorizationModule;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.security.UGIProvider;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.gateway.handlers.meta.RemoteSystemOperationsService;
import co.cask.cdap.gateway.handlers.meta.RemoteSystemOperationsServiceModule;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.proto.Id;
import co.cask.cdap.security.CurrentUGIProvider;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Provides a {@link DatasetService} which uses {@link RemoteDatasetFramework}.
 * This is used to independently start a remote dataset service during upgrade. This is needed to perform upgrade steps
 * which needs access to user datasets.
 * Note: This should not be used outside upgrade tool.
 */
public class DatasetServiceManager extends AbstractIdleService {

  private final DatasetService datasetService;
  private final ZKClientService zkClientService;
  private final DatasetFramework datasetFramework;
  private final DatasetOpExecutorService datasetOpExecutorService;
  private final RemoteSystemOperationsService remoteSystemOperationsService;

  @Inject
  DatasetServiceManager(CConfiguration cConf, Configuration hConf) {
    Injector injector = createInjector(cConf, hConf);
    this.datasetService = injector.getInstance(DatasetService.class);
    this.zkClientService = injector.getInstance(ZKClientService.class);
    this.datasetFramework = injector.getInstance(DatasetFramework.class);
    this.datasetOpExecutorService = injector.getInstance(DatasetOpExecutorService.class);
    this.remoteSystemOperationsService = injector.getInstance(RemoteSystemOperationsService.class);
  }

  public DatasetFramework getDSFramework() {
    return datasetFramework;
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
          getDSFramework().getInstances(Id.Namespace.DEFAULT);
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

  private Injector createInjector(CConfiguration cConf, Configuration hConf) {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new IOModule(),
      new KafkaClientModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new NamespaceClientRuntimeModule().getDistributedModules(),
      new DataSetServiceModules().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new ExploreClientModule(),
      new NamespaceStoreModule().getDistributedModules(),
      new RemoteSystemOperationsServiceModule(),
      new SecureStoreModules().getDistributedModules(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      new AuthenticationContextModules().getMasterModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(Store.class).to(DefaultStore.class);
          // CDAP-6577 Perform impersonation when doing upgrade from CDAP 3.5.0
          bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);
        }
      }
    );
  }
}
