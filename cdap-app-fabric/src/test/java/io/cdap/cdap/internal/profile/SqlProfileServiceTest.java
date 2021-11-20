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

package io.cdap.cdap.internal.profile;

import com.google.common.base.Joiner;
import com.google.inject.Injector;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.sql.PostgresInstantiator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Test Profile Service using SQL storage.
 */
public class SqlProfileServiceTest extends ProfileServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static EmbeddedPostgres pg;
  private static Injector injector;
  private static ProfileService profileService;
  private static StructuredTableAdmin structuredTableAdmin;
  private static DefaultStore defaultStore;

  @BeforeClass
  public static void setup() throws IOException, TableAlreadyExistsException {
    cConf = CConfiguration.create();
    // any plugin which requires transaction will be excluded
    cConf.set(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE, Joiner.on(",").join(Table.TYPE, KeyValueTable.TYPE));

    pg = PostgresInstantiator.createAndStart(cConf, TEMP_FOLDER.newFolder());
    injector = AppFabricTestHelper.getInjector(cConf);
    structuredTableAdmin = injector.getInstance(StructuredTableAdmin.class);
    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
    StoreDefinition.createAllTables(structuredTableAdmin);

    profileService = new ProfileService(cConf, injector.getInstance(MetricsSystemClient.class), transactionRunner);
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
    AppFabricTestHelper.shutdown();
  }
}
