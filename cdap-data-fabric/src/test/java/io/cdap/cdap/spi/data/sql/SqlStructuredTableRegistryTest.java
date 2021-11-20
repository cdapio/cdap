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

package io.cdap.cdap.spi.data.sql;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import io.cdap.cdap.spi.data.common.StructuredTableRegistry;
import io.cdap.cdap.spi.data.common.StructuredTableRegistryTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import javax.sql.DataSource;

/**
 *
 */
public class SqlStructuredTableRegistryTest extends StructuredTableRegistryTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static EmbeddedPostgres pg;
  private static StructuredTableRegistry registry;

  @BeforeClass
  public static void beforeClass() throws Exception {
    pg = PostgresInstantiator.createAndStart(TEMP_FOLDER.newFolder());
    DataSource dataSource = pg.getPostgresDatabase();
    registry = new SqlStructuredTableRegistry(dataSource);
  }

  @AfterClass
  public static void finish() throws IOException {
    pg.close();
  }

  @Override
  protected StructuredTableRegistry getStructuredTableRegistry() {
    return registry;
  }
}
