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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.sql.PostgresSqlStructuredTableAdmin;
import co.cask.cdap.data2.sql.SqlStructuredTableRegistry;
import co.cask.cdap.data2.sql.SqlTransactionRunner;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.namespace.DefaultNamespaceAdmin;
import co.cask.cdap.internal.app.namespace.NamespaceResourceDeleter;
import co.cask.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.store.DefaultNamespaceStore;
import co.cask.cdap.store.StoreDefinition;
import com.google.inject.Injector;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.apache.tephra.TransactionSystemClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import javax.sql.DataSource;

public class SqlDefaultStoreTest extends DefaultStoreTest {

  private static EmbeddedPostgres pg;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    pg = EmbeddedPostgres.start();
    DataSource dataSource = pg.getPostgresDatabase();
    StructuredTableAdmin structuredTableAdmin =
      new PostgresSqlStructuredTableAdmin(new SqlStructuredTableRegistry(), dataSource);
    TransactionRunner transactionRunner = new SqlTransactionRunner(structuredTableAdmin, dataSource);

    // TODO(CDAP-14770): fully change this when migrating the rest of DefaultStore
    store =
      new DefaultStore(injector.getInstance(CConfiguration.class),
                       injector.getInstance(DatasetFramework.class),
                       injector.getInstance(TransactionSystemClient.class),
                       transactionRunner);

    nsStore = new DefaultNamespaceStore(transactionRunner);
    nsAdmin = new DefaultNamespaceAdmin(
      nsStore, store, injector.getInstance(DatasetFramework.class),
      injector.getProvider(NamespaceResourceDeleter.class), injector.getProvider(StorageProviderNamespaceAdmin.class),
      injector.getInstance(CConfiguration.class), injector.getInstance(Impersonator.class),
      injector.getInstance(AuthorizationEnforcer.class), injector.getInstance(AuthenticationContext.class));
    StoreDefinition.NamespaceStore.createTable(structuredTableAdmin);
    StoreDefinition.WorkflowStore.createTables(structuredTableAdmin);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    pg.close();
  }

}
