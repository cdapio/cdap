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

package co.cask.cdap.store;

import co.cask.cdap.data2.sql.PostgresSqlStructuredTableAdmin;
import co.cask.cdap.data2.sql.SqlStructuredTableRegistry;
import co.cask.cdap.data2.sql.SqlTransactionRunner;
import co.cask.cdap.security.impersonation.OwnerStore;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import javax.sql.DataSource;

/**
 * Tests SQL implementation of the owner store
 */
public class SqlOwnerStoreTest extends OwnerStoreTest {

  private static OwnerStore ownerStore;
  private static EmbeddedPostgres pg;

  @BeforeClass
  public static void setup() throws Exception {
    pg = EmbeddedPostgres.start();
    DataSource dataSource = pg.getPostgresDatabase();
    StructuredTableRegistry registry = new SqlStructuredTableRegistry();
    StructuredTableAdmin structuredTableAdmin = new PostgresSqlStructuredTableAdmin(registry, dataSource);
    TransactionRunner transactionRunner = new SqlTransactionRunner(structuredTableAdmin, dataSource);
    StoreDefinition.OwnerStore.createTables(structuredTableAdmin);
    ownerStore = new DefaultOwnerStore(transactionRunner);
  }

  @Override
  public OwnerStore getOwnerStore() {
    return ownerStore;
  }

  @AfterClass
  public static void teardown() throws IOException {
    pg.close();
  }
}
