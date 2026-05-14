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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.StructuredTableTest;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.table.options.PreferUnionForDisjunctionOption;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for SQL structured table.
 */
public class SqlStructuredTableTest extends StructuredTableTest {
  private static EmbeddedPostgres pg;
  private static StructuredTableAdmin tableAdmin;
  private static TransactionRunner transactionRunner;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    populateCConf(cConf);
    pg = PostgresInstantiator.createAndStart(cConf, TEMP_FOLDER.newFolder());

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

    tableAdmin = injector.getInstance(StructuredTableAdmin.class);
    transactionRunner = injector.getInstance(TransactionRunner.class);

    Assert.assertEquals(PostgreSqlStructuredTableAdmin.class, tableAdmin.getClass());
    Assert.assertEquals(RetryingSqlTransactionRunner.class, transactionRunner.getClass());
  }

  /**
   * populate retry settings in cConf used by {@link SqlStructuredTableRetryTest}
   */
  private static void populateCConf(CConfiguration cConf) {
    cConf.setInt(Constants.Dataset.DATA_STORAGE_SQL_TRANSACTION_RUNNER_MAX_RETRIES, 2);
    cConf.setLong(Constants.Dataset.DATA_STORAGE_SQL_TRANSACTION_RUNNER_TRANSACTION_FAILURE_DELAY_MILLIS, 0);
    cConf.setLong(Constants.Dataset.DATA_STORAGE_SQL_TRANSACTION_RUNNER_CONNECTION_FAILURE_DELAY_MILLIS, 0);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (pg != null) {
      pg.close();
    }
  }

  @Override
  protected StructuredTableAdmin getStructuredTableAdmin() {
    return tableAdmin;
  }

  @Override
  protected TransactionRunner getTransactionRunner() {
    return transactionRunner;
  }

  @Test
  public void testUnionAllOptimization() throws Exception {
    List<Field<?>> fields1 = Arrays.asList(
        Fields.intField("key", 1),
        Fields.longField("key2", 1L),
        Fields.stringField("key3", "a"),
        Fields.booleanField("col7", true));
    List<Field<?>> fields2 = Arrays.asList(
        Fields.intField("key", 2),
        Fields.longField("key2", 2L),
        Fields.stringField("key3", "b"),
        Fields.booleanField("col7", null));
    List<Field<?>> fields3 = Arrays.asList(
        Fields.intField("key", 3),
        Fields.longField("key2", 3L),
        Fields.stringField("key3", "c"),
        Fields.booleanField("col7", false));
    getTransactionRunner().run(context -> {
      StructuredTable t = context.getTable(SIMPLE_SPEC.getTableId());
      t.upsert(fields1);
      t.upsert(fields2);
      t.upsert(fields3);
    });
    Collection<Field<?>> filterIndexes = Arrays.asList(
        Fields.booleanField("col7", true),
        Fields.booleanField("col7", null));

    List<StructuredRow> result = TransactionRunners.run(getTransactionRunner(), context -> {
      StructuredTable t = context.getTable(SIMPLE_SPEC.getTableId());
      try (CloseableIterator<StructuredRow> iterator = t.scan(Range.all(), Integer.MAX_VALUE,
          filterIndexes,
          SortOrder.ASC,
          PreferUnionForDisjunctionOption.INSTANCE)) {
        List<StructuredRow> rows = new ArrayList<>();
        iterator.forEachRemaining(rows::add);
        return rows;
      }
    });

    Assert.assertEquals(2, result.size());
  }

  @Test
  public void testUnionOptimizationMultiField() throws Exception {
    List<Field<?>> fields1 = Arrays.asList(
        Fields.intField("key", 1),
        Fields.longField("key2", 1L),
        Fields.stringField("key3", "a"),
        Fields.stringField("col1", "val1"),
        Fields.booleanField("col7", false));
    List<Field<?>> fields2 = Arrays.asList(
        Fields.intField("key", 2),
        Fields.longField("key2", 2L),
        Fields.stringField("key3", "b"),
        Fields.stringField("col1", "val2"),
        Fields.booleanField("col7", true));
    getTransactionRunner().run(context -> {
      StructuredTable t = context.getTable(SIMPLE_SPEC.getTableId());
      t.upsert(fields1);
      t.upsert(fields2);
    });
    Collection<Field<?>> filterIndexes = Arrays.asList(
        Fields.stringField("col1", "val1"),
        Fields.booleanField("col7", true));

    List<StructuredRow> result = TransactionRunners.run(getTransactionRunner(), context -> {
      StructuredTable t = context.getTable(SIMPLE_SPEC.getTableId());
      try (CloseableIterator<StructuredRow> iterator = t.scan(Range.all(), Integer.MAX_VALUE,
          filterIndexes,
          SortOrder.ASC,
          PreferUnionForDisjunctionOption.INSTANCE)) {
        List<StructuredRow> rows = new ArrayList<>();
        iterator.forEachRemaining(rows::add);
        return rows;
      }
    });

    Assert.assertEquals(2, result.size());
  }

  @Test
  public void testUnionOptimizationSingleField() throws Exception {
    getTransactionRunner().run(context -> {
      StructuredTable t = context.getTable(SIMPLE_SPEC.getTableId());
      t.upsert(Arrays.asList(
          Fields.intField("key", 1),
          Fields.longField("key2", 1L),
          Fields.stringField("key3", "a"),
          Fields.stringField("col1", "val1")));
      t.upsert(Arrays.asList(
          Fields.intField("key", 2),
          Fields.longField("key2", 2L),
          Fields.stringField("key3", "b"),
          Fields.stringField("col1", "val2")));
    });
    Collection<Field<?>> filterIndexes = Collections.singletonList(
        Fields.stringField("col1", "val1"));

    List<StructuredRow> result = TransactionRunners.run(getTransactionRunner(), context -> {
      StructuredTable t = context.getTable(SIMPLE_SPEC.getTableId());
      try (CloseableIterator<StructuredRow> iterator = t.scan(Range.all(), Integer.MAX_VALUE,
          filterIndexes,
          SortOrder.ASC,
          PreferUnionForDisjunctionOption.INSTANCE)) {
        List<StructuredRow> rows = new ArrayList<>();
        iterator.forEachRemaining(rows::add);
        return rows;
      }
    });

    Assert.assertEquals(1, result.size());
  }
}
