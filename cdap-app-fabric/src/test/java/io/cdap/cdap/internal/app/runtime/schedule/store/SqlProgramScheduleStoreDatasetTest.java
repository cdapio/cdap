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

package io.cdap.cdap.internal.app.runtime.schedule.store;

import com.google.common.base.Joiner;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.sql.PostgresInstantiator;
import io.cdap.cdap.spi.data.sql.PostgresSqlStructuredTableAdmin;
import io.cdap.cdap.spi.data.sql.SqlStructuredTableRegistry;
import io.cdap.cdap.spi.data.sql.SqlTransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import javax.sql.DataSource;

/**
 *
 */
public class SqlProgramScheduleStoreDatasetTest extends ProgramScheduleStoreDatasetTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static EmbeddedPostgres pg;
  private static TransactionRunner transactionRunner;

  @BeforeClass
  public static void setup() throws IOException, TableAlreadyExistsException {
    CConfiguration cConf = CConfiguration.create();
    // any plugin which requires transaction will be excluded
    cConf.set(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE, Joiner.on(",").join(Table.TYPE, KeyValueTable.TYPE));
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_SQL);

    pg = PostgresInstantiator.createAndStart(cConf, TEMP_FOLDER.newFolder());
    DataSource dataSource = pg.getPostgresDatabase();
    SqlStructuredTableRegistry registry = new SqlStructuredTableRegistry(dataSource);
    registry.initialize();
    StructuredTableAdmin structuredTableAdmin =
      new PostgresSqlStructuredTableAdmin(registry, dataSource);
    transactionRunner = new SqlTransactionRunner(structuredTableAdmin, dataSource);
    // TODO: (CDAP-14830) the creation of tables below can be removed after the storage SPI injection is done
    StoreDefinition.createAllTables(structuredTableAdmin, registry);
  }

  @Override
  protected TransactionRunner getTransactionRunner() {
    return transactionRunner;
  }

  @AfterClass
  public static void tearDown() throws IOException {
    pg.close();
  }
}
