/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.explore.service;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.test.SlowTests;
import com.continuuity.tephra.Transaction;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 *
 */
@Category(SlowTests.class)
public class WritableDatasetTest extends BaseHiveExploreServiceTest {
  @BeforeClass
  public static void start() throws Exception {
    startServices(CConfiguration.create());
    datasetFramework.addModule("keyStructValue", new KeyStructValueTableDefinition.KeyStructValueTableModule());
  }

  private static void initKeyValueTable(String tableName) throws Exception {
    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", tableName, DatasetProperties.EMPTY);
    // Accessing dataset instance to perform data operations
    KeyStructValueTableDefinition.KeyStructValueTable table =
      datasetFramework.getDataset(tableName, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);

    Transaction tx = transactionManager.startShort(100);
    table.startTx(tx);

    KeyStructValueTableDefinition.KeyValue.Value value1 =
      new KeyStructValueTableDefinition.KeyValue.Value("first", Lists.newArrayList(1, 2, 3, 4, 5));
    KeyStructValueTableDefinition.KeyValue.Value value2 =
      new KeyStructValueTableDefinition.KeyValue.Value("two", Lists.newArrayList(10, 11, 12, 13, 14));
    table.put("1", value1);
    table.put("2", value2);
    Assert.assertEquals(value1, table.get("1"));

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx, table.getTxChanges());
    transactionManager.commit(tx);

    table.postTxCommit();
  }

  @AfterClass
  public static void stop() throws Exception {
    datasetFramework.deleteModule("keyStructValue");
  }

  @Test
  public void writeIntoItselfTest() throws Exception {
    initKeyValueTable("my_table");
    try {
      ListenableFuture<ExploreExecutionResult> future =
        exploreClient.submit("insert into table my_table select * from my_table");
      ExploreExecutionResult result = future.get();
      result.close();

      // Assert the values have been inserted into the dataset
      KeyStructValueTableDefinition.KeyStructValueTable table =
        datasetFramework.getDataset("my_table", DatasetDefinition.NO_ARGUMENTS, null);
      Assert.assertNotNull(table);
      Transaction tx = transactionManager.startShort(100);
      table.startTx(tx);

      Assert.assertEquals(new KeyStructValueTableDefinition.KeyValue.Value("first", Lists.newArrayList(1, 2, 3, 4, 5)),
                          table.get("1_2"));
      Assert.assertEquals(new KeyStructValueTableDefinition.KeyValue.Value("two",
                                                                           Lists.newArrayList(10, 11, 12, 13, 14)),
                          table.get("2_2"));

      Assert.assertTrue(table.commitTx());
      transactionManager.canCommit(tx, table.getTxChanges());
      transactionManager.commit(tx);
      table.postTxCommit();

      // Make sure Hive also sees those values
      result = exploreClient.submit("select * from my_table").get();
      Assert.assertEquals("1", result.next().getColumns().get(0).toString());
      Assert.assertEquals("1_2", result.next().getColumns().get(0).toString());
      Assert.assertEquals("2", result.next().getColumns().get(0).toString());
      Assert.assertEquals("2_2", result.next().getColumns().get(0).toString());
      Assert.assertFalse(result.hasNext());
      result.close();
    } finally {
      datasetFramework.deleteInstance("my_table");
    }
  }

  @Test
  public void writeIntoOtherDatasetTest() throws Exception {

    datasetFramework.addModule("keyExtendedStructValueTable",
                               new KeyExtendedStructValueTableDefinition.KeyExtendedStructValueTableModule());
    datasetFramework.addInstance("keyExtendedStructValueTable", "extended_table", DatasetProperties.EMPTY);
    initKeyValueTable("my_table");
    try {
      // Accessing dataset instance to perform data operations
      KeyExtendedStructValueTableDefinition.KeyExtendedStructValueTable table =
        datasetFramework.getDataset("extended_table", DatasetDefinition.NO_ARGUMENTS, null);
      Assert.assertNotNull(table);

      Transaction tx1 = transactionManager.startShort(100);
      table.startTx(tx1);

      KeyExtendedStructValueTableDefinition.KeyExtendedValue value1 =
        new KeyExtendedStructValueTableDefinition.KeyExtendedValue(
          "10",
          new KeyStructValueTableDefinition.KeyValue.Value("ten", Lists.newArrayList(10, 11, 12)),
          20);
      table.put("10", value1);
      Assert.assertEquals(value1, table.get("10"));

      Assert.assertTrue(table.commitTx());
      transactionManager.canCommit(tx1, table.getTxChanges());
      transactionManager.commit(tx1);
      table.postTxCommit();

      ListenableFuture<ExploreExecutionResult> future =
        exploreClient.submit("insert into table my_table select key,value from extended_table");
      ExploreExecutionResult result = future.get();
      result.close();

      result = exploreClient.submit("select * from my_table").get();
      Assert.assertEquals("1", result.next().getColumns().get(0).toString());
      Assert.assertEquals("10_2", result.next().getColumns().get(0).toString());
      Assert.assertEquals("2", result.next().getColumns().get(0).toString());
      Assert.assertFalse(result.hasNext());
      result.close();

      // Test insert overwrite
      result = exploreClient.submit("insert overwrite table my_table select key,value from extended_table").get();
      result.close();
      result = exploreClient.submit("select * from my_table").get();
      result.hasNext();

    } finally {
      datasetFramework.deleteInstance("my_table");
      datasetFramework.deleteInstance("extended_table");
      datasetFramework.deleteModule("keyExtendedStructValueTable");
    }
  }

  // TODO test write from native table to dataset,
  // TODO test insert overwrite table: overwrite is the same as into
  // TODO test trying to write with incompatible types
}
