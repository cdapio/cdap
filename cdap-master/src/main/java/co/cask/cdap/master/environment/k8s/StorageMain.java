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

package co.cask.cdap.master.environment.k8s;

import co.cask.cdap.app.guice.ConstantTransactionSystemClient;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.InMemoryDiscoveryModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.StorageModule;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import co.cask.cdap.logging.guice.RemoteLogAppenderModule;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.NoOpAuthorizer;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.spi.metadata.MetadataStorage;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new SystemDatasetRuntimeModule().getStandaloneModules(),
      // We actually only need the MetadataStore createIndex.
      // But due to the DataSetsModules, we need to pull in more modules.
      new DataSetsModules().getStandaloneModules(),
      new InMemoryDiscoveryModule(),
      new StorageModule(),
      new AuthenticationContextModules().getNoOpModule(),
      new RemoteLogAppenderModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(AuthorizationEnforcer.class).to(NoOpAuthorizer.class);
          bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);
        }
      }
    );

    // Create stores definitions
    StructuredTableRegistry tableRegistry = injector.getInstance(StructuredTableRegistry.class);
    StructuredTableAdmin tableAdmin = injector.getInstance(StructuredTableAdmin.class);

    try {
      StoreDefinition.createAllTables(tableAdmin, tableRegistry);
      LOG.info("Storage definitions creation completed");
    } catch (TableAlreadyExistsException e) {
      // Ignore the error
      LOG.debug("Store table already exists", e);
    }

    // Create metadata tables
    try (MetadataStorage metadataStorage = injector.getInstance(MetadataStorage.class)) {
      metadataStorage.createIndex();
    }
    injector.getInstance(LevelDBTableService.class).close();

    LOG.info("Storage creation completed");
  }
}
