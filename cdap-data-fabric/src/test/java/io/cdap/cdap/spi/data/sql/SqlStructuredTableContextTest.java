/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Test for {@link SqlStructuredTableContext}
 */
public class SqlStructuredTableContextTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static EmbeddedPostgres postgres;
  private static StructuredTableAdmin tableAdmin;
  private static TransactionRunner transactionRunner;

  public static class TestTable {
    public static final FieldType FIELD_KEY = Fields.intType("field_int");
    public static final FieldType FIELD_STRING = Fields.stringType("field_string");
    public static final FieldType FIELD_LONG = Fields.longType("field_long");
    public static final StructuredTableId ID = new StructuredTableId("test_table_id");
    public static final StructuredTableSpecification SPEC = new StructuredTableSpecification.Builder()
      .withId(ID)
      .withFields(FIELD_KEY, FIELD_STRING, FIELD_LONG)
      .withPrimaryKeys(FIELD_KEY.getName())
      .withIndexes(FIELD_STRING.getName())
      .build();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    postgres = PostgresInstantiator.createAndStart(cConf, TEMP_FOLDER.newFolder());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new StorageModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
        }
      }
    );

    injector.getInstance(StructuredTableRegistry.class).initialize();
    tableAdmin = injector.getInstance(StructuredTableAdmin.class);
    Assert.assertEquals(PostgresSqlStructuredTableAdmin.class, tableAdmin.getClass());
    transactionRunner = injector.getInstance(TransactionRunner.class);
    Assert.assertEquals(RetryingSqlTransactionRunner.class, transactionRunner.getClass());

  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (postgres != null) {
      postgres.close();
    }
  }

  @After
  public void cleanup() throws Exception {
    boolean doCleanup = true;
    try {
      TransactionRunners.run(transactionRunner, context -> {
        context.getTable(TestTable.ID);
      });
    } catch (TableNotFoundException e) {
      doCleanup = false;
    }
    if (doCleanup) {
      tableAdmin.drop(TestTable.ID);
    }
  }

  @Test(expected = TableNotFoundException.class)
  public void testGetTableNotFound() {
    TransactionRunners.run(transactionRunner, context -> {
      context.getTable(TestTable.ID);
    });
  }

  @Test
  public void testGetOrCreate() {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tableCreated = context.getOrCreateTable(TestTable.ID, TestTable.SPEC);
      Assert.assertNotNull(tableCreated);
      StructuredTable tableGot = context.getTable(TestTable.ID);
      Assert.assertNotNull(tableGot);
    });
  }
}
