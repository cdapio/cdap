/*
 * Copyright © 2018 Cask Data, Inc.
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
import co.cask.cdap.spi.data.TransactionRunner;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.Connection;

public class SqlStructuredTableTest extends StructuredTableTest {
  private static Connection connection;
  private static SqlStructuredTableAdmin tableAdmin;

  @BeforeClass
  public static void beforeClass() throws Exception {
    EmbeddedPostgres pg = EmbeddedPostgres.start();
    connection = pg.getPostgresDatabase().getConnection();
    tableAdmin = new SqlStructuredTableAdmin(connection);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (connection != null) {
      connection.close();
    }
  }

  @Override
  protected StructuredTableAdmin getStructuredTableAdmin() throws Exception {
    return tableAdmin;
  }

  @Override
  protected TransactionRunner getTransactionRunner() throws Exception {
    return new SqlTransactionRunner(tableAdmin, connection);
  }
}
