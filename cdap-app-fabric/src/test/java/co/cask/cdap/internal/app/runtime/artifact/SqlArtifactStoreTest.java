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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacePathLocator;
import co.cask.cdap.data2.sql.PostgresSqlStructuredTableAdmin;
import co.cask.cdap.data2.sql.SqlStructuredTableRegistry;
import co.cask.cdap.data2.sql.SqlTransactionRunner;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.base.Joiner;
import com.google.inject.Injector;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import javax.sql.DataSource;

public class SqlArtifactStoreTest extends ArtifactStoreTest {

  private static EmbeddedPostgres pg;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    // any plugin which requires transaction will be excluded
    cConf.set(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE, Joiner.on(",").join(Table.TYPE, KeyValueTable.TYPE));
    Injector injector = AppFabricTestHelper.getInjector(cConf);

    pg = EmbeddedPostgres.start();
    DataSource dataSource = pg.getPostgresDatabase();
    StructuredTableAdmin structuredTableAdmin =
      new PostgresSqlStructuredTableAdmin(new SqlStructuredTableRegistry(), dataSource);
    TransactionRunner transactionRunner = new SqlTransactionRunner(structuredTableAdmin, dataSource);
    artifactStore = new ArtifactStore(cConf,
                                      injector.getInstance(NamespacePathLocator.class),
                                      injector.getInstance(LocationFactory.class),
                                      injector.getInstance(Impersonator.class),
                                      transactionRunner);
    StoreDefinition.ArtifactStore.createTables(structuredTableAdmin);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    pg.close();
  }
}
