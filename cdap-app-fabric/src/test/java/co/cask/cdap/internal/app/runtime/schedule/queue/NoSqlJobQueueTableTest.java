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

package co.cask.cdap.internal.app.runtime.schedule.queue;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocalLocationModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.InMemoryNamespaceAdmin;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.data.runtime.StorageModule;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.store.StoreDefinition;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 *
 */
public class NoSqlJobQueueTableTest extends JobQueueTableTest {
  private static TransactionManager txManager;
  private static CConfiguration cConf;
  private static TransactionRunner transactionRunner;

  @BeforeClass
  public static void beforeClass() throws IOException, TableAlreadyExistsException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_NOSQL);

    txManager = new TransactionManager(new Configuration());
    txManager.startAndWait();

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocalLocationModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new StorageModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(DatasetDefinitionRegistryFactory.class)
            .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);
          bind(DatasetFramework.class).to(InMemoryDatasetFramework.class);
          bind(NamespaceQueryAdmin.class).to(InMemoryNamespaceAdmin.class).in(Scopes.SINGLETON);
          bind(TransactionSystemClient.class).toInstance(new InMemoryTxSystemClient(txManager));
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
        }
      }
    );

    injector.getInstance(StructuredTableRegistry.class).initialize();
    StructuredTableAdmin tableAdmin = injector.getInstance(StructuredTableAdmin.class);
    transactionRunner = injector.getInstance(TransactionRunner.class);

    StoreDefinition.JobQueueStore.createTables(tableAdmin);
    StoreDefinition.AppMetadataStore.createTables(tableAdmin);
  }

  @AfterClass
  public static void afterClass() {
    txManager.stopAndWait();
  }

  @Override
  protected TransactionRunner getTransactionRunner() {
    return transactionRunner;
  }

  @Override
  protected CConfiguration getCConf() {
    return cConf;
  }
}
