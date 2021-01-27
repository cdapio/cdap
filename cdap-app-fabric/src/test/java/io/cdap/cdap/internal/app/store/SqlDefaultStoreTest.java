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

package io.cdap.cdap.internal.app.store;

import com.google.inject.Injector;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.namespace.DefaultNamespaceAdmin;
import io.cdap.cdap.internal.app.namespace.NamespaceResourceDeleter;
import io.cdap.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.sql.PostgresInstantiator;
import io.cdap.cdap.spi.data.sql.PostgresSqlStructuredTableAdmin;
import io.cdap.cdap.spi.data.sql.SqlStructuredTableRegistry;
import io.cdap.cdap.spi.data.sql.SqlTransactionRunner;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.DefaultNamespaceStore;
import io.cdap.cdap.store.StoreDefinition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import javax.sql.DataSource;

public class SqlDefaultStoreTest extends DefaultStoreTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static EmbeddedPostgres pg;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    pg = PostgresInstantiator.createAndStart(injector.getInstance(CConfiguration.class), TEMP_FOLDER.newFolder());
    DataSource dataSource = pg.getPostgresDatabase();
    StructuredTableRegistry structuredTableRegistry = new SqlStructuredTableRegistry(dataSource);
    structuredTableRegistry.initialize();
    StructuredTableAdmin structuredTableAdmin =
      new PostgresSqlStructuredTableAdmin(structuredTableRegistry, dataSource);
    TransactionRunner transactionRunner = new SqlTransactionRunner(structuredTableAdmin, dataSource);
    StoreDefinition.createAllTables(structuredTableAdmin, structuredTableRegistry, true);


    store = new DefaultStore(transactionRunner);

    nsStore = new DefaultNamespaceStore(transactionRunner);
    nsAdmin = new DefaultNamespaceAdmin(
      nsStore, store, injector.getInstance(DatasetFramework.class),
      injector.getInstance(MetricsCollectionService.class), injector.getProvider(NamespaceResourceDeleter.class),
      injector.getProvider(StorageProviderNamespaceAdmin.class), injector.getInstance(CConfiguration.class),
      injector.getInstance(Impersonator.class), injector.getInstance(AuthorizationEnforcer.class),
      injector.getInstance(AuthenticationContext.class));
  }

  @AfterClass
  public static void afterClass() throws IOException {
    pg.close();
    AppFabricTestHelper.shutdown();
  }
}
