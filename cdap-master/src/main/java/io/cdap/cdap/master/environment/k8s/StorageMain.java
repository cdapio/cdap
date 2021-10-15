/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.ConstantTransactionSystemClient;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.guice.CoreSecurityModule;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The main class for creating store definition. It can be used as a initialization step for each container.
 */
public class StorageMain {

  private static final Logger LOG = LoggerFactory.getLogger(StorageMain.class);

  public static void main(String[] args) throws IOException {
    new StorageMain().createStorage(CConfiguration.create());
  }

  @VisibleForTesting
  void createStorage(CConfiguration cConf) throws IOException {
    LOG.info("Creating storages");

    CoreSecurityModule coreSecurityModule = CoreSecurityRuntimeModule.getDistributedModule(cConf);
    List<Module> modules = new ArrayList<>(Arrays.asList(
      new ConfigModule(cConf),
      new SystemDatasetRuntimeModule().getStandaloneModules(),
      // We actually only need the MetadataStore createIndex.
      // But due to the DataSetsModules, we need to pull in more modules.
      new DataSetsModules().getStandaloneModules(),
      new InMemoryDiscoveryModule(),
      new StorageModule(),
      new DFSLocationModule(),
      new IOModule(),
      coreSecurityModule,
      new AuthenticationContextModules().getMasterModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(AccessEnforcer.class).to(NoOpAccessController.class);
          bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);
          // The metrics collection service might not get started at this moment,
          // so inject a NoopMetricsCollectionService.
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
        }
      }
    ));

    if (coreSecurityModule.requiresZKClient()) {
      modules.add(new ZKClientModule());
    }

    Injector injector = Guice.createInjector(modules);

    // Create stores definitions
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));

    // Create metadata tables
    try (MetadataStorage metadataStorage = injector.getInstance(MetadataStorage.class)) {
      metadataStorage.createIndex();
    }
    injector.getInstance(LevelDBTableService.class).close();

    LOG.info("Storage creation completed");
  }
}
