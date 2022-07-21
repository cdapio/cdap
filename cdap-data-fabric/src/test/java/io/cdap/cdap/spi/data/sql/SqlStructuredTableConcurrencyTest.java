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

package io.cdap.cdap.spi.data.sql;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.StructuredTableConcurrencyTest;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Tests concurrent operations on {@link PostgreSqlStructuredTable}.
 */
public class SqlStructuredTableConcurrencyTest extends StructuredTableConcurrencyTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

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

    tableAdmin = injector.getInstance(StructuredTableAdmin.class);
    transactionRunner = injector.getInstance(TransactionRunner.class);

    Assert.assertEquals(PostgreSqlStructuredTableAdmin.class, tableAdmin.getClass());
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
