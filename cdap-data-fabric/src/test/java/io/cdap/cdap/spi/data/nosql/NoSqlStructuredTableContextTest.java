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

import com.google.inject.Injector;
import io.cdap.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.nosql.NoSqlStructuredTableAdmin;
import io.cdap.cdap.spi.data.nosql.NoSqlStructuredTableContext;
import io.cdap.cdap.spi.data.nosql.NoSqlStructuredTableRegistry;
import io.cdap.cdap.spi.data.nosql.NoSqlTransactionRunner;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Test for {@link NoSqlStructuredTableContext}
 */
public class NoSqlStructuredTableContextTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static NoSqlStructuredTableAdmin tableAdmin;
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
    Injector injector = dsFrameworkUtil.getInjector();
    tableAdmin = injector.getInstance(NoSqlStructuredTableAdmin.class);
    transactionRunner = dsFrameworkUtil.getInjector().getInstance(NoSqlTransactionRunner.class);
    NoSqlStructuredTableRegistry registry =
      dsFrameworkUtil.getInjector().getInstance(NoSqlStructuredTableRegistry.class);
    registry.initialize();
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
  public void testGetOrCreate() throws Exception {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable tableCreated = context.getOrCreateTable(TestTable.ID, TestTable.SPEC);
      Assert.assertNotNull(tableCreated);
      StructuredTable tableGot = context.getTable(TestTable.ID);
      Assert.assertNotNull(tableGot);
    });
  }
}
