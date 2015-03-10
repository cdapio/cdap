/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.lib.ObjectMappedTableProperties;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.explore.service.datasets.Record;
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
 * Tests exploration of object mapped tables.
 */
@Category(SlowTests.class)
public class HiveExploreObjectMappedTableTestRun extends BaseHiveExploreServiceTest {
  private static Record record1;
  private static Record record2;

  @BeforeClass
  public static void start() throws Exception {
    initialize();

    datasetFramework.addInstance(ObjectMappedTable.class.getName(), MY_TABLE, ObjectMappedTableProperties.builder()
      .setType(Record.class)
      .setRowKeyExploreName("row_key")
      .setRowKeyExploreType(Schema.Type.STRING)
      .build());

    // Accessing dataset instance to perform data operations
    ObjectMappedTable<Record> table = datasetFramework.getDataset(MY_TABLE, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);

    TransactionAware txTable = (TransactionAware) table;
    Transaction tx1 = transactionManager.startShort(100);
    txTable.startTx(tx1);

    record1 = new Record(123, 1234567890L, 3.14159f, 3.1415926535, "foobar", new byte[] { 1, 2, 3 });
    record2 = new Record(0 - 987, 9876543210L, 2.71f, 2.71112384, "hello world", new byte[] { 4, 5, 6 });
    table.write("123", record1);
    table.write("456", record2);

    Assert.assertTrue(txTable.commitTx());

    transactionManager.canCommit(tx1, txTable.getTxChanges());
    transactionManager.commit(tx1);

    txTable.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    txTable.startTx(tx2);
  }

  @AfterClass
  public static void stop() throws Exception {
    datasetFramework.deleteInstance(MY_TABLE);
  }

  @Test
  public void testSchema() throws Exception {
    runCommand(NAMESPACE_ID, "describe " + MY_TABLE_HIVE_NAME,
               true,
               Lists.newArrayList(
                 new ColumnDesc("col_name", "STRING", 1, "from deserializer"),
                 new ColumnDesc("data_type", "STRING", 2, "from deserializer"),
                 new ColumnDesc("comment", "STRING", 3, "from deserializer")
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("row_key", "string", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("bytearrayfield", "binary", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("doublefield", "double", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("floatfield", "float", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("intfield", "int", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("longfield", "bigint", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("stringfield", "string", "from deserializer"))
               )
    );
  }

  @Test
  public void testSelectStar() throws Exception {
    List<ColumnDesc> expectedSchema = Lists.newArrayList(
      new ColumnDesc(MY_TABLE_HIVE_NAME + ".row_key", "STRING", 1, null),
      new ColumnDesc(MY_TABLE_HIVE_NAME + ".bytearrayfield", "BINARY", 2, null),
      new ColumnDesc(MY_TABLE_HIVE_NAME + ".doublefield", "DOUBLE", 3, null),
      new ColumnDesc(MY_TABLE_HIVE_NAME + ".floatfield", "FLOAT", 4, null),
      new ColumnDesc(MY_TABLE_HIVE_NAME + ".intfield", "INT", 5, null),
      new ColumnDesc(MY_TABLE_HIVE_NAME + ".longfield", "BIGINT", 6, null),
      new ColumnDesc(MY_TABLE_HIVE_NAME + ".stringfield", "STRING", 7, null)
    );
    ExploreExecutionResult results = exploreClient.submit(NAMESPACE_ID, "select * from " + MY_TABLE_HIVE_NAME).get();
    // check schema
    Assert.assertEquals(expectedSchema, results.getResultSchema());
    List<Object> columns = results.next().getColumns();
    // check record1
    Assert.assertEquals("123", columns.get(0));
    Assert.assertArrayEquals(record1.byteArrayField, (byte[]) columns.get(1));
    Assert.assertTrue(Math.abs(record1.doubleField - (Double) columns.get(2)) < 0.000001);
    // sigh... why are floats returned as doubles??
    Assert.assertTrue(Math.abs(record1.floatField - (Double) columns.get(3)) < 0.000001);
    Assert.assertEquals(record1.intField, columns.get(4));
    Assert.assertEquals(record1.longField, columns.get(5));
    Assert.assertEquals(record1.stringField, columns.get(6));
    // check record2
    columns = results.next().getColumns();
    Assert.assertEquals("456", columns.get(0));
    Assert.assertArrayEquals(record2.byteArrayField, (byte[]) columns.get(1));
    Assert.assertTrue(Math.abs(record2.doubleField - (Double) columns.get(2)) < 0.000001);
    Assert.assertTrue(Math.abs(record2.floatField - (Double) columns.get(3)) < 0.000001);
    Assert.assertEquals(record2.intField, columns.get(4));
    Assert.assertEquals(record2.longField, columns.get(5));
    Assert.assertEquals(record2.stringField, columns.get(6));
    // should not be any more
    Assert.assertFalse(results.hasNext());
  }

  @Test
  public void testSelect() throws Exception {
    runCommand(NAMESPACE_ID, "select intfield, stringfield from " + MY_TABLE_HIVE_NAME + " where row_key='123'",
               true,
               Lists.newArrayList(new ColumnDesc("intfield", "INT", 1, null),
                                  new ColumnDesc("stringfield", "STRING", 2, null)),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(record1.intField, record1.stringField)))
    );
  }
}
