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

package co.cask.cdap.internal.profile;

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.metrics.MetricsSystemClient;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.sql.PostgresSqlStructuredTableAdmin;
import co.cask.cdap.data2.sql.SqlStructuredTableRegistry;
import co.cask.cdap.data2.sql.SqlTransactionRunner;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.base.Joiner;
import com.google.inject.Injector;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.apache.tephra.TransactionSystemClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import javax.sql.DataSource;

/**
 * Test Profile Service using SQL storage.
 */
public class SqlProfileServiceTest extends ProfileServiceTest {
  private static EmbeddedPostgres pg;
  private static Injector injector;
  private static ProfileService profileService;
  private static StructuredTableAdmin structuredTableAdmin;
  private static DefaultStore defaultStore;

  @BeforeClass
  public static void setup() throws IOException, TableAlreadyExistsException {
    CConfiguration cConf = CConfiguration.create();
    // any plugin which requires transaction will be excluded
    cConf.set(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE, Joiner.on(",").join(Table.TYPE, KeyValueTable.TYPE));
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_SQL);

    pg = EmbeddedPostgres.start();
    DataSource dataSource = pg.getPostgresDatabase();
    SqlStructuredTableRegistry registry = new SqlStructuredTableRegistry();
    registry.initialize();
    structuredTableAdmin =
      new PostgresSqlStructuredTableAdmin(registry, dataSource);
    TransactionRunner transactionRunner = new SqlTransactionRunner(structuredTableAdmin, dataSource);
    // TODO: (CDAP-14830) the creation of tables below can be removed after the storage SPI injection is done
    StoreDefinition.createAllTables(structuredTableAdmin, registry);

    injector = AppFabricTestHelper.getInjector();
    profileService = new ProfileService(injector.getInstance(MetricsSystemClient.class), transactionRunner);
    defaultStore = new DefaultStore(transactionRunner);
  }

  @Override
  protected Injector getInjector() {
    return injector;
  }

  @Override
  protected ProfileService getProfileService() {
    return profileService;
  }

  @Override
  protected StructuredTableAdmin getTableAdmin() {
    return structuredTableAdmin;
  }

  @Override
  protected DefaultStore getDefaultStore() {
    return defaultStore;
  }

  @AfterClass
  public static void afterClass() throws IOException {
    pg.close();
  }
}
