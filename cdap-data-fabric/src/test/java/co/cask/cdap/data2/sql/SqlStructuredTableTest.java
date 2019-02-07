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


package co.cask.cdap.data2.sql;

import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.StructuredTableTest;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.junit.BeforeClass;

import javax.sql.DataSource;

/**
 * Test for SQL structured table.
 */
public class SqlStructuredTableTest extends StructuredTableTest {
  private static DataSource dataSource;
  private static PostgresSqlStructuredTableAdmin tableAdmin;

  @BeforeClass
  public static void beforeClass() throws Exception {
    EmbeddedPostgres pg = EmbeddedPostgres.start();
    dataSource = pg.getPostgresDatabase();
    tableAdmin = new PostgresSqlStructuredTableAdmin(new SqlStructuredTableRegistry(), dataSource);
  }

  @Override
  protected StructuredTableAdmin getStructuredTableAdmin() {
    return tableAdmin;
  }

  @Override
  protected TransactionRunner getTransactionRunner() {
    return new SqlTransactionRunner(tableAdmin, dataSource);
  }
}
