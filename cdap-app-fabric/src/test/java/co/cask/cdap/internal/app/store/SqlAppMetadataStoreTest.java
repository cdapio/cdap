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

import co.cask.cdap.data2.sql.PostgresSqlStructuredTableAdmin;
import co.cask.cdap.data2.sql.SqlTransactionRunner;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.store.StoreDefinition;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.junit.BeforeClass;

import java.io.IOException;
import javax.sql.DataSource;

/**
 * Tests {@link AppMetadataStore} using SQL.
 */
public class SqlAppMetadataStoreTest extends AppMetadataStoreTest {

  private static EmbeddedPostgres pg;

  @BeforeClass
  public static void beforeClass() throws IOException, TableAlreadyExistsException {
    pg = EmbeddedPostgres.start();
    DataSource dataSource = pg.getPostgresDatabase();
    StructuredTableAdmin structuredTableAdmin = new PostgresSqlStructuredTableAdmin(dataSource);
    transactionRunner = new SqlTransactionRunner(structuredTableAdmin, dataSource);
    StoreDefinition.AppMetadataStore.createTables(structuredTableAdmin);
  }
}
