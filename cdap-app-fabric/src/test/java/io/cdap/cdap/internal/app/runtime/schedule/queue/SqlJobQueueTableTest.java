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

package io.cdap.cdap.internal.app.runtime.schedule.queue;

import com.google.common.base.Joiner;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.sql.PostgresInstantiator;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 *
 */
public class SqlJobQueueTableTest extends JobQueueTableTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static EmbeddedPostgres pg;
  private static CConfiguration cConf;
  private static TransactionRunner transactionRunner;

  @BeforeClass
  public static void setup() throws Exception {
    cConf = CConfiguration.create();
    // any plugin which requires transaction will be excluded
    cConf.set(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE, Joiner.on(",").join(Table.TYPE, KeyValueTable.TYPE));

    pg = PostgresInstantiator.createAndStart(cConf, TEMP_FOLDER.newFolder());
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocalLocationModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new StorageModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(NamespaceQueryAdmin.class).to(InMemoryNamespaceAdmin.class).in(Scopes.SINGLETON);
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
        }
      }
    );

    injector.getInstance(StructuredTableRegistry.class).initialize();
    StructuredTableAdmin structuredTableAdmin = injector.getInstance(StructuredTableAdmin.class);
    transactionRunner = injector.getInstance(TransactionRunner.class);

    StoreDefinition.JobQueueStore.createTables(structuredTableAdmin, false);
    StoreDefinition.AppMetadataStore.createTables(structuredTableAdmin, false);
  }

  @Override
  protected TransactionRunner getTransactionRunner() {
    return transactionRunner;
  }

  @Override
  protected CConfiguration getCConf() {
    return cConf;
  }

  @AfterClass
  public static void afterClass() throws IOException {
    pg.close();
  }
}
