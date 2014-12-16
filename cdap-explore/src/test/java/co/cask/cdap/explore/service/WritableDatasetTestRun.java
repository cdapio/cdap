/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.explore.service.datasets.KeyExtendedStructValueTableDefinition;
import co.cask.cdap.explore.service.datasets.KeyStructValueTableDefinition;
import co.cask.cdap.explore.service.datasets.KeyValueTableDefinition;
import co.cask.cdap.explore.service.datasets.WritableKeyStructValueTableDefinition;
import co.cask.cdap.test.XSlowTests;
import co.cask.tephra.Transaction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.net.URL;

/**
 *
 */
@Category(XSlowTests.class)
public class WritableDatasetTestRun extends BaseHiveExploreServiceTest {
  @BeforeClass
  public static void start() throws Exception {
    startServices();
    datasetFramework.addModule("keyStructValue", new KeyStructValueTableDefinition.KeyStructValueTableModule());
  }

  private static void initKeyValueTable(String tableName, boolean addData) throws Exception {
    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", tableName, DatasetProperties.EMPTY);
    if (!addData) {
      return;
    }

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
    try {
      initKeyValueTable("my_table", true);
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
    try {
      initKeyValueTable("my_table", true);
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

  @Test
  public void writeIntoNonScannableDataset() throws Exception {

    datasetFramework.addModule("keyExtendedStructValueTable",
                               new KeyExtendedStructValueTableDefinition.KeyExtendedStructValueTableModule());
    datasetFramework.addInstance("keyExtendedStructValueTable", "extended_table", DatasetProperties.EMPTY);

    datasetFramework.addModule("writableKeyStructValueTable",
                               new WritableKeyStructValueTableDefinition.KeyStructValueTableModule());
    datasetFramework.addInstance("writableKeyStructValueTable", "writable_table", DatasetProperties.EMPTY);
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
        exploreClient.submit("insert into table writable_table select key,value from extended_table");
      ExploreExecutionResult result = future.get();
      result.close();

      KeyStructValueTableDefinition.KeyStructValueTable table2 =
        datasetFramework.getDataset("writable_table", DatasetDefinition.NO_ARGUMENTS, null);
      Assert.assertNotNull(table);
      Transaction tx = transactionManager.startShort(100);
      table2.startTx(tx);

      Assert.assertEquals(new KeyStructValueTableDefinition.KeyValue.Value("ten", Lists.newArrayList(10, 11, 12)),
                          table2.get("10_2"));

      Assert.assertTrue(table.commitTx());
      transactionManager.canCommit(tx, table.getTxChanges());
      transactionManager.commit(tx);
      table.postTxCommit();

    } finally {
      datasetFramework.deleteInstance("writable_table");
      datasetFramework.deleteInstance("extended_table");
      datasetFramework.deleteModule("writableKeyStructValueTable");
      datasetFramework.deleteModule("keyExtendedStructValueTable");
    }
  }

  @Test
  public void multipleInsertsTest() throws Exception {
    try {
      initKeyValueTable("my_table", true);
      initKeyValueTable("my_table_1", false);
      initKeyValueTable("my_table_2", false);
      initKeyValueTable("my_table_3", false);
      ListenableFuture<ExploreExecutionResult> future =
        exploreClient.submit("from my_table insert into table my_table_1 select * where key='1'" +
                               "insert into table my_table_2 select * where key='2'" +
                               "insert into table my_table_3 select *");
      ExploreExecutionResult result = future.get();
      result.close();

      result = exploreClient.submit("select * from my_table_2").get();
      Assert.assertEquals("2_2", result.next().getColumns().get(0).toString());
      Assert.assertFalse(result.hasNext());
      result.close();

      result = exploreClient.submit("select * from my_table_1").get();
      Assert.assertEquals("1_2", result.next().getColumns().get(0).toString());
      Assert.assertFalse(result.hasNext());
      result.close();

      result = exploreClient.submit("select * from my_table_3").get();
      Assert.assertEquals("1_2", result.next().getColumns().get(0).toString());
      Assert.assertEquals("2_2", result.next().getColumns().get(0).toString());
      Assert.assertFalse(result.hasNext());
      result.close();
    } finally {
      datasetFramework.deleteInstance("my_table");
      datasetFramework.deleteInstance("my_table_1");
      datasetFramework.deleteInstance("my_table_2");
      datasetFramework.deleteInstance("my_table_3");
    }
  }

  @Test
  public void writeFromNativeTableIntoDatasetTest() throws Exception {

    datasetFramework.addModule("kvTable", new KeyValueTableDefinition.KeyValueTableModule());
    datasetFramework.addInstance("kvTable", "simple_table", DatasetProperties.EMPTY);
    try {
      URL loadFileUrl = getClass().getResource("/test_table.dat");
      Assert.assertNotNull(loadFileUrl);

      exploreClient.submit("create table test (first INT, second STRING) ROW FORMAT " +
                             "DELIMITED FIELDS TERMINATED BY '\\t'").get().close();
      exploreClient.submit("LOAD DATA LOCAL INPATH '" + new File(loadFileUrl.toURI()).getAbsolutePath() +
                             "' INTO TABLE test").get().close();

      exploreClient.submit("insert into table simple_table select * from test").get().close();

      ExploreExecutionResult result = exploreClient.submit("select * from simple_table").get();
      Assert.assertEquals(ImmutableList.of(1, "one"), result.next().getColumns());
      Assert.assertEquals(ImmutableList.of(2, "two"), result.next().getColumns());
      Assert.assertEquals(ImmutableList.of(3, "three"), result.next().getColumns());
      Assert.assertEquals(ImmutableList.of(4, "four"), result.next().getColumns());
      Assert.assertEquals(ImmutableList.of(5, "five"), result.next().getColumns());
      Assert.assertFalse(result.hasNext());
      result.close();

    } finally {
      exploreClient.submit("drop table if exists test").get().close();
      datasetFramework.deleteInstance("simple_table");
      datasetFramework.deleteModule("kvTable");
    }
  }

  @Test
  public void writeFromDatasetIntoNativeTableTest() throws Exception {

    datasetFramework.addModule("kvTable", new KeyValueTableDefinition.KeyValueTableModule());
    datasetFramework.addInstance("kvTable", "simple_table", DatasetProperties.EMPTY);
    try {
      exploreClient.submit("create table test (first INT, second STRING) ROW FORMAT " +
                             "DELIMITED FIELDS TERMINATED BY '\\t'").get().close();

      // Accessing dataset instance to perform data operations
      KeyValueTableDefinition.KeyValueTable table =
        datasetFramework.getDataset("simple_table", DatasetDefinition.NO_ARGUMENTS, null);
      Assert.assertNotNull(table);

      Transaction tx1 = transactionManager.startShort(100);
      table.startTx(tx1);

      table.put(10, "ten");
      Assert.assertEquals("ten", table.get(10));

      Assert.assertTrue(table.commitTx());
      transactionManager.canCommit(tx1, table.getTxChanges());
      transactionManager.commit(tx1);
      table.postTxCommit();

      exploreClient.submit("insert into table test select * from simple_table").get().close();

      ExploreExecutionResult result = exploreClient.submit("select * from test").get();
      Assert.assertEquals(ImmutableList.of(10, "ten"), result.next().getColumns());
      Assert.assertFalse(result.hasNext());
      result.close();

    } finally {
      exploreClient.submit("drop table if exists test").get().close();
      datasetFramework.deleteInstance("simple_table");
      datasetFramework.deleteModule("kvTable");
    }
  }

  // TODO test insert overwrite table: overwrite is the same as into
  // TODO test trying to write with incompatible types
}
