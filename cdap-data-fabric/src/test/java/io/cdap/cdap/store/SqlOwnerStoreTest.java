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

package io.cdap.cdap.store;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import io.cdap.cdap.security.impersonation.OwnerStore;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.sql.PostgresInstantiator;
import io.cdap.cdap.spi.data.sql.PostgresSqlStructuredTableAdmin;
import io.cdap.cdap.spi.data.sql.SqlStructuredTableRegistry;
import io.cdap.cdap.spi.data.sql.SqlTransactionRunner;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import javax.sql.DataSource;

/**
 * Tests SQL implementation of the owner store
 */
public class SqlOwnerStoreTest extends OwnerStoreTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static TransactionRunner txRunner;
  private static OwnerStore ownerStore;
  private static EmbeddedPostgres pg;

  @BeforeClass
  public static void setup() throws Exception {
    pg = PostgresInstantiator.createAndStart(TEMP_FOLDER.newFolder());
    DataSource dataSource = pg.getPostgresDatabase();
    StructuredTableRegistry registry = new SqlStructuredTableRegistry(dataSource);
    registry.initialize();
    StructuredTableAdmin structuredTableAdmin = new PostgresSqlStructuredTableAdmin(registry, dataSource);
    txRunner = new SqlTransactionRunner(structuredTableAdmin, dataSource);
    StoreDefinition.OwnerStore.createTables(structuredTableAdmin, false);
    ownerStore = new DefaultOwnerStore(txRunner);
  }

  @Override
  public OwnerStore getOwnerStore() {
    return ownerStore;
  }

  @Override
  public void cleanup() {
    TransactionRunners.run(txRunner, context -> {
      StructuredTable ownerTable = context.getTable(StoreDefinition.OwnerStore.OWNER_TABLE);
      ownerTable.deleteAll(Range.all());
    });
  }

  @AfterClass
  public static void teardown() throws IOException {
    pg.close();
  }
}
