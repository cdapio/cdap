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

package co.cask.cdap.security;

import co.cask.cdap.data.security.DefaultSecretStore;
import co.cask.cdap.data2.sql.PostgresSqlStructuredTableAdmin;
import co.cask.cdap.data2.sql.SqlStructuredTableRegistry;
import co.cask.cdap.data2.sql.SqlTransactionRunner;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.store.StoreDefinition;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import javax.sql.DataSource;

/**
 * Tests {@link DefaultSecretStore} with Sql storage implementation.
 */
public class SqlDefaultSecureStoreTest extends DefaultSecretStoreTest {
  private static EmbeddedPostgres postgres;

  @BeforeClass
  public static void setup() throws Exception {
    postgres = EmbeddedPostgres.start();
    DataSource dataSource = postgres.getPostgresDatabase();
    StructuredTableAdmin structuredTableAdmin =
      new PostgresSqlStructuredTableAdmin(new SqlStructuredTableRegistry(), dataSource);
    TransactionRunner transactionRunner = new SqlTransactionRunner(structuredTableAdmin, dataSource);
    store = new DefaultSecretStore(transactionRunner);
    StoreDefinition.SecretStore.createTable(structuredTableAdmin);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    postgres.close();
  }
}
