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

package io.cdap.cdap.spi.data.common;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import io.cdap.cdap.spi.data.sql.PostgresInstantiator;
import io.cdap.cdap.spi.data.sql.SqlStructuredTableRegistry;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import javax.sql.DataSource;

/**
 * SQL backend for {@link CachedStructuredTableRegistryTest}
 */
public class SqlCachedStructuredTableRegistryTest extends CachedStructuredTableRegistryTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static StructuredTableRegistry registry;
  private static SqlStructuredTableRegistry sqlRegistry;

  @BeforeClass
  public static void beforeClass() throws Exception {
    EmbeddedPostgres pg = PostgresInstantiator.createAndStart(TEMP_FOLDER.newFolder());
    DataSource dataSource = pg.getPostgresDatabase();
    // TODO: CDAP-14780 Use injector once JDBC driver is wired up in StorageModule
    sqlRegistry = new SqlStructuredTableRegistry(dataSource);
    registry = new CachedStructuredTableRegistry(sqlRegistry);
    Assert.assertTrue(registry instanceof CachedStructuredTableRegistry);
  }

  @Override
  protected StructuredTableRegistry getStructuredTableRegistry() {
    return registry;
  }

  @Override
  protected StructuredTableRegistry getNonCachedStructuredTableRegistry() {
    return sqlRegistry;
  }
}
