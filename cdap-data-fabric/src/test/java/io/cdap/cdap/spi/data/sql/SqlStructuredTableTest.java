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


package co.cask.cdap.spi.data.sql;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.data.runtime.StorageModule;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.StructuredTableTest;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Test for SQL structured table.
 */
public class SqlStructuredTableTest extends StructuredTableTest {
  private static EmbeddedPostgres pg;
  private static StructuredTableAdmin tableAdmin;
  private static TransactionRunner transactionRunner;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    pg = PostgresInstantiator.createAndStart(cConf, TEMP_FOLDER.newFolder());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new StorageModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
        }
      }
    );

    injector.getInstance(StructuredTableRegistry.class).initialize();
    tableAdmin = injector.getInstance(StructuredTableAdmin.class);
    transactionRunner = injector.getInstance(TransactionRunner.class);

    Assert.assertEquals(PostgresSqlStructuredTableAdmin.class, tableAdmin.getClass());
    Assert.assertEquals(RetryingSqlTransactionRunner.class, transactionRunner.getClass());
  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (pg != null) {
      pg.close();
    }
  }

  @Override
  protected StructuredTableAdmin getStructuredTableAdmin() {
    return tableAdmin;
  }

  @Override
  protected TransactionRunner getTransactionRunner() {
    return transactionRunner;
  }
}
