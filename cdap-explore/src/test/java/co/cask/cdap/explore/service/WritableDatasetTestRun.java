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
import co.cask.cdap.proto.Id;
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

  private static final Id.DatasetModule keyExtendedStructValueTable =
    Id.DatasetModule.from(NAMESPACE_ID, "keyExtendedStructValueTable");
  private static final Id.DatasetModule kvTable = Id.DatasetModule.from(NAMESPACE_ID, "kvTable");
  private static final Id.DatasetModule writableKeyStructValueTable =
    Id.DatasetModule.from(NAMESPACE_ID, "writableKeyStructValueTable");
  private static final Id.DatasetInstance extendedTable = Id.DatasetInstance.from(NAMESPACE_ID, "extended_table");
  private static final Id.DatasetInstance simpleTable = Id.DatasetInstance.from(NAMESPACE_ID, "simple_table");

  @BeforeClass
  public static void start() throws Exception {
    initialize();
    datasetFramework.addModule(KEY_STRUCT_VALUE, new KeyStructValueTableDefinition.KeyStructValueTableModule());
  }

  private static void initKeyValueTable(Id.DatasetInstance datasetInstanceId, boolean addData) throws Exception {
    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", datasetInstanceId, DatasetProperties.EMPTY);
    if (!addData) {
      return;
    }

    // Accessing dataset instance to perform data operations
    KeyStructValueTableDefinition.KeyStructValueTable table =
      datasetFramework.getDataset(datasetInstanceId, DatasetDefinition.NO_ARGUMENTS, null);
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
    datasetFramework.deleteModule(KEY_STRUCT_VALUE);
  }

  @Test
  public void writeIntoItselfTest() throws Exception {
    try {
      initKeyValueTable(MY_TABLE, true);
      ListenableFuture<ExploreExecutionResult> future =
        exploreClient.submit(NAMESPACE_ID, "insert into table my_table select * from my_table");
      ExploreExecutionResult result = future.get();
      result.close();

      // Assert the values have been inserted into the dataset
      KeyStructValueTableDefinition.KeyStructValueTable table =
        datasetFramework.getDataset(MY_TABLE, DatasetDefinition.NO_ARGUMENTS, null);
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
      result = exploreClient.submit(NAMESPACE_ID, "select * from my_table").get();
      Assert.assertEquals("1", result.next().getColumns().get(0).toString());
      Assert.assertEquals("1_2", result.next().getColumns().get(0).toString());
      Assert.assertEquals("2", result.next().getColumns().get(0).toString());
      Assert.assertEquals("2_2", result.next().getColumns().get(0).toString());
      Assert.assertFalse(result.hasNext());
      result.close();
    } finally {
      datasetFramework.deleteInstance(MY_TABLE);
    }
  }

  @Test
  public void testTablesWithSpecialChars() throws Exception {
    // '.' are replaced with "_" in hive, so create a dataset with . in name.
    Id.DatasetInstance myTable1 = Id.DatasetInstance.from(NAMESPACE_ID, "dot.table");
    // '_' are replaced with "_" in hive, so create a dataset with . in name.
    Id.DatasetInstance myTable2 = Id.DatasetInstance.from(NAMESPACE_ID, "hyphen-table");
    try {
      initKeyValueTable(myTable1, true);
      initKeyValueTable(myTable2, true);

      ExploreExecutionResult result = exploreClient.submit(NAMESPACE_ID, "select * from dot_table").get();

      Assert.assertEquals("1", result.next().getColumns().get(0).toString());
      result.close();

      result = exploreClient.submit(NAMESPACE_ID, "select * from hyphen_table").get();
      Assert.assertEquals("1", result.next().getColumns().get(0).toString());
      result.close();

    } finally {
      datasetFramework.deleteInstance(myTable1);
      datasetFramework.deleteInstance(myTable2);
    }
  }

  @Test
  public void writeIntoOtherDatasetTest() throws Exception {

    datasetFramework.addModule(keyExtendedStructValueTable,
                               new KeyExtendedStructValueTableDefinition.KeyExtendedStructValueTableModule());
    datasetFramework.addInstance("keyExtendedStructValueTable", extendedTable, DatasetProperties.EMPTY);
    try {
      initKeyValueTable(MY_TABLE, true);
      // Accessing dataset instance to perform data operations
      KeyExtendedStructValueTableDefinition.KeyExtendedStructValueTable table =
        datasetFramework.getDataset(extendedTable, DatasetDefinition.NO_ARGUMENTS, null);
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
        exploreClient.submit(NAMESPACE_ID, "insert into table my_table select key,value from extended_table");
      ExploreExecutionResult result = future.get();
      result.close();

      result = exploreClient.submit(NAMESPACE_ID, "select * from my_table").get();
      Assert.assertEquals("1", result.next().getColumns().get(0).toString());
      Assert.assertEquals("10_2", result.next().getColumns().get(0).toString());
      Assert.assertEquals("2", result.next().getColumns().get(0).toString());
      Assert.assertFalse(result.hasNext());
      result.close();

      // Test insert overwrite
      result = exploreClient.submit(NAMESPACE_ID,
                                    "insert overwrite table my_table select key,value from extended_table").get();
      result.close();
      result = exploreClient.submit(NAMESPACE_ID, "select * from my_table").get();
      result.hasNext();

    } finally {
      datasetFramework.deleteInstance(MY_TABLE);
      datasetFramework.deleteInstance(extendedTable);
      datasetFramework.deleteModule(keyExtendedStructValueTable);
    }
  }

  @Test
  public void writeIntoNonScannableDataset() throws Exception {
    Id.DatasetInstance writableTable = Id.DatasetInstance.from(NAMESPACE_ID, "writable_table");
    datasetFramework.addModule(keyExtendedStructValueTable,
                               new KeyExtendedStructValueTableDefinition.KeyExtendedStructValueTableModule());
    datasetFramework.addInstance("keyExtendedStructValueTable", extendedTable, DatasetProperties.EMPTY);

    datasetFramework.addModule(writableKeyStructValueTable,
                               new WritableKeyStructValueTableDefinition.KeyStructValueTableModule());
    datasetFramework.addInstance("writableKeyStructValueTable", writableTable, DatasetProperties.EMPTY);
    try {
      // Accessing dataset instance to perform data operations
      KeyExtendedStructValueTableDefinition.KeyExtendedStructValueTable table =
        datasetFramework.getDataset(extendedTable, DatasetDefinition.NO_ARGUMENTS, null);
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
        exploreClient.submit(NAMESPACE_ID, "insert into table writable_table select key,value from extended_table");
      ExploreExecutionResult result = future.get();
      result.close();

      KeyStructValueTableDefinition.KeyStructValueTable table2 =
        datasetFramework.getDataset(writableTable, DatasetDefinition.NO_ARGUMENTS, null);
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
      datasetFramework.deleteInstance(writableTable);
      datasetFramework.deleteInstance(extendedTable);
      datasetFramework.deleteModule(writableKeyStructValueTable);
      datasetFramework.deleteModule(keyExtendedStructValueTable);
    }
  }

  @Test
  public void multipleInsertsTest() throws Exception {
    Id.DatasetInstance myTable1 = Id.DatasetInstance.from(NAMESPACE_ID, "my_table_1");
    Id.DatasetInstance myTable2 = Id.DatasetInstance.from(NAMESPACE_ID, "my_table_2");
    Id.DatasetInstance myTable3 = Id.DatasetInstance.from(NAMESPACE_ID, "my_table_3");
    try {
      initKeyValueTable(MY_TABLE, true);
      initKeyValueTable(myTable1, false);
      initKeyValueTable(myTable2, false);
      initKeyValueTable(myTable3, false);
      ListenableFuture<ExploreExecutionResult> future =
        exploreClient.submit(NAMESPACE_ID,
                             "from my_table insert into table my_table_1 select * where key='1'" +
                               "insert into table my_table_2 select * where key='2'" +
                               "insert into table my_table_3 select *");
      ExploreExecutionResult result = future.get();
      result.close();

      result = exploreClient.submit(NAMESPACE_ID, "select * from my_table_2").get();
      Assert.assertEquals("2_2", result.next().getColumns().get(0).toString());
      Assert.assertFalse(result.hasNext());
      result.close();

      result = exploreClient.submit(NAMESPACE_ID, "select * from my_table_1").get();
      Assert.assertEquals("1_2", result.next().getColumns().get(0).toString());
      Assert.assertFalse(result.hasNext());
      result.close();

      result = exploreClient.submit(NAMESPACE_ID, "select * from my_table_3").get();
      Assert.assertEquals("1_2", result.next().getColumns().get(0).toString());
      Assert.assertEquals("2_2", result.next().getColumns().get(0).toString());
      Assert.assertFalse(result.hasNext());
      result.close();
    } finally {
      datasetFramework.deleteInstance(MY_TABLE);
      datasetFramework.deleteInstance(myTable1);
      datasetFramework.deleteInstance(myTable2);
      datasetFramework.deleteInstance(myTable3);
    }
  }

  @Test
  public void writeFromNativeTableIntoDatasetTest() throws Exception {

    datasetFramework.addModule(kvTable, new KeyValueTableDefinition.KeyValueTableModule());
    datasetFramework.addInstance("kvTable", simpleTable, DatasetProperties.EMPTY);
    try {
      URL loadFileUrl = getClass().getResource("/test_table.dat");
      Assert.assertNotNull(loadFileUrl);

      exploreClient.submit(NAMESPACE_ID,
                           "create table test (first INT, second STRING) ROW FORMAT " +
                             "DELIMITED FIELDS TERMINATED BY '\\t'").get().close();
      exploreClient.submit(NAMESPACE_ID,
                           "LOAD DATA LOCAL INPATH '" + new File(loadFileUrl.toURI()).getAbsolutePath() +
                             "' INTO TABLE test").get().close();

      exploreClient.submit(NAMESPACE_ID, "insert into table simple_table select * from test").get().close();

      ExploreExecutionResult result = exploreClient.submit(NAMESPACE_ID, "select * from simple_table").get();
      Assert.assertEquals(ImmutableList.of(1, "one"), result.next().getColumns());
      Assert.assertEquals(ImmutableList.of(2, "two"), result.next().getColumns());
      Assert.assertEquals(ImmutableList.of(3, "three"), result.next().getColumns());
      Assert.assertEquals(ImmutableList.of(4, "four"), result.next().getColumns());
      Assert.assertEquals(ImmutableList.of(5, "five"), result.next().getColumns());
      Assert.assertFalse(result.hasNext());
      result.close();

    } finally {
      exploreClient.submit(NAMESPACE_ID, "drop table if exists test").get().close();
      datasetFramework.deleteInstance(simpleTable);
      datasetFramework.deleteModule(kvTable);
    }
  }

  @Test
  public void writeFromDatasetIntoNativeTableTest() throws Exception {

    datasetFramework.addModule(kvTable, new KeyValueTableDefinition.KeyValueTableModule());
    datasetFramework.addInstance("kvTable", simpleTable, DatasetProperties.EMPTY);
    try {
      exploreClient.submit(NAMESPACE_ID, "create table test (first INT, second STRING) ROW FORMAT " +
                             "DELIMITED FIELDS TERMINATED BY '\\t'").get().close();

      // Accessing dataset instance to perform data operations
      KeyValueTableDefinition.KeyValueTable table =
        datasetFramework.getDataset(simpleTable, DatasetDefinition.NO_ARGUMENTS, null);
      Assert.assertNotNull(table);

      Transaction tx1 = transactionManager.startShort(100);
      table.startTx(tx1);

      table.put(10, "ten");
      Assert.assertEquals("ten", table.get(10));

      Assert.assertTrue(table.commitTx());
      transactionManager.canCommit(tx1, table.getTxChanges());
      transactionManager.commit(tx1);
      table.postTxCommit();

      exploreClient.submit(NAMESPACE_ID, "insert into table test select * from simple_table").get().close();

      ExploreExecutionResult result = exploreClient.submit(NAMESPACE_ID, "select * from test").get();
      Assert.assertEquals(ImmutableList.of(10, "ten"), result.next().getColumns());
      Assert.assertFalse(result.hasNext());
      result.close();

    } finally {
      exploreClient.submit(NAMESPACE_ID, "drop table if exists test").get().close();
      datasetFramework.deleteInstance(simpleTable);
      datasetFramework.deleteModule(kvTable);
    }
  }

  // TODO test insert overwrite table: overwrite is the same as into
  // TODO test trying to write with incompatible types
}
