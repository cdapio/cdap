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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

/**
 * Tests Hive13ExploreService.
 */
@Category(SlowTests.class)
public class HiveExploreServiceTableTestRun extends BaseHiveExploreServiceTest {

  @BeforeClass
  public static void start() throws Exception {
    startServices();

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("table", "my_table", DatasetProperties.EMPTY);

    // Accessing dataset instance to perform data operations
    Table table = datasetFramework.getDataset("my_table", DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);

    Transaction tx1 = transactionManager.startShort(100);
    // normally this is handled by the platform... but we have to do it manually here since Table interface
    // is not transaction aware
    TransactionAware txTable = (TransactionAware) table;
    txTable.startTx(tx1);

    table.put(Bytes.toBytes("userX"), Bytes.toBytes("age"), Bytes.toBytes(36));
    table.put(Bytes.toBytes("userX"), Bytes.toBytes("gender"), Bytes.toBytes("male"));
    table.put(Bytes.toBytes("userX"), Bytes.toBytes("name"), Bytes.toBytes("Jack"));
    table.put(Bytes.toBytes("userY"), Bytes.toBytes("age"), Bytes.toBytes(27));
    table.put(Bytes.toBytes("userY"), Bytes.toBytes("gender"), Bytes.toBytes("female"));
    table.put(Bytes.toBytes("userY"), Bytes.toBytes("name"), Bytes.toBytes("Jill"));

    Assert.assertTrue(txTable.commitTx());

    transactionManager.canCommit(tx1, txTable.getTxChanges());
    transactionManager.commit(tx1);

    txTable.postTxCommit();
  }

  @AfterClass
  public static void stop() throws Exception {
    datasetFramework.deleteInstance("my_table");
  }

  @Test
  public void testDefaultSchema() throws Exception {
    runCommand("describe my_table",
               true,
               Lists.newArrayList(
                 new ColumnDesc("col_name", "STRING", 1, "from deserializer"),
                 new ColumnDesc("data_type", "STRING", 2, "from deserializer"),
                 new ColumnDesc("comment", "STRING", 3, "from deserializer")
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("row", "binary", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("columns", "map<binary,binary>", "from deserializer"))
               )
    );
  }

  @Test
  public void testSelectStar() throws Exception {
    ExploreExecutionResult results = exploreClient.submit("select * from my_table").get();
    // check schema
    List<ColumnDesc> expectedSchema = Lists.newArrayList(
      new ColumnDesc("my_table.row", "BINARY", 1, null),
      new ColumnDesc("my_table.columns", "map<binary,binary>", 2, null)
    );
    Assert.assertEquals(expectedSchema, results.getResultSchema());
    // check each result, without checking timestamp since that changes for each test
    // first result
    List<Object> columns = results.next().getColumns();
    Assert.assertEquals("userX", Bytes.toString((byte[]) columns.get(0)));
    // second result
    columns = results.next().getColumns();
    Assert.assertEquals("userY", Bytes.toString((byte[]) columns.get(0)));
    // should not be any more
    Assert.assertFalse(results.hasNext());
  }

  @Test
  public void testSimpleSelect() throws Exception {
    runCommand("select row from my_table",
               true,
               Lists.newArrayList(new ColumnDesc("row", "BINARY", 1, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList(Bytes.toBytes("userX"))),
                 new QueryResult(Lists.<Object>newArrayList(Bytes.toBytes("userY"))))
    );
  }
}
