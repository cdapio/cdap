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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.util.List;

/**
 * Tests exploration of Tables.
 */
@Category(SlowTests.class)
public class HiveExploreTableTestRun extends BaseHiveExploreServiceTest {
  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void start() throws Exception {
    initialize(tmpFolder);

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("bool_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("string_field", Schema.of(Schema.Type.STRING))
    );
    datasetFramework.addInstance(Table.class.getName(), MY_TABLE, DatasetProperties.builder()
      .add(Table.PROPERTY_SCHEMA, schema.toString())
      .add(Table.PROPERTY_SCHEMA_ROW_FIELD, "string_field")
      .build());

    // Accessing dataset instance to perform data operations
    Table table = datasetFramework.getDataset(MY_TABLE, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);

    TransactionAware txTable = (TransactionAware) table;
    Transaction tx1 = transactionManager.startShort(100);
    txTable.startTx(tx1);

    Put put = new Put(Bytes.toBytes("row1"));
    put.add("bool_field", false);
    put.add("int_field", Integer.MAX_VALUE);
    put.add("long_field", Long.MAX_VALUE);
    put.add("float_field", 3.14f);
    put.add("double_field", 3.14);
    put.add("bytes_field", new byte[]{1, 2, 3});
    table.put(put);

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
  public void testNoOpOnMissingSchema() throws Exception {
    Id.DatasetInstance datasetId = Id.DatasetInstance.from(NAMESPACE_ID, "noschema");
    datasetFramework.addInstance(Table.class.getName(), datasetId, DatasetProperties.EMPTY);
    try {
      DatasetSpecification spec = datasetFramework.getDatasetSpec(datasetId);
      Assert.assertEquals(QueryHandle.NO_OP, exploreTableManager.enableDataset(datasetId, spec));
    } finally {
      datasetFramework.deleteInstance(datasetId);
    }
  }

  @Test
  public void testSchema() throws Exception {
    runCommand(NAMESPACE_ID, "describe " + MY_TABLE_NAME,
               true,
               Lists.newArrayList(
                 new ColumnDesc("col_name", "STRING", 1, "from deserializer"),
                 new ColumnDesc("data_type", "STRING", 2, "from deserializer"),
                 new ColumnDesc("comment", "STRING", 3, "from deserializer")
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("bool_field", "boolean", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("int_field", "int", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("long_field", "bigint", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("float_field", "float", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("double_field", "double", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("bytes_field", "binary", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("string_field", "string", "from deserializer"))
               )
    );
  }

  @Test
  public void testSelectStar() throws Exception {
    List<ColumnDesc> expectedSchema = Lists.newArrayList(
      new ColumnDesc(MY_TABLE_NAME + ".bool_field", "BOOLEAN", 1, null),
      new ColumnDesc(MY_TABLE_NAME + ".int_field", "INT", 2, null),
      new ColumnDesc(MY_TABLE_NAME + ".long_field", "BIGINT", 3, null),
      new ColumnDesc(MY_TABLE_NAME + ".float_field", "FLOAT", 4, null),
      new ColumnDesc(MY_TABLE_NAME + ".double_field", "DOUBLE", 5, null),
      new ColumnDesc(MY_TABLE_NAME + ".bytes_field", "BINARY", 6, null),
      new ColumnDesc(MY_TABLE_NAME + ".string_field", "STRING", 7, null)
    );
    ExploreExecutionResult results = exploreClient.submit(NAMESPACE_ID, "select * from " + MY_TABLE_NAME).get();
    // check schema
    Assert.assertEquals(expectedSchema, results.getResultSchema());
    List<Object> columns = results.next().getColumns();
    // check record1
    Assert.assertFalse((Boolean) columns.get(0));
    Assert.assertEquals(Integer.MAX_VALUE, columns.get(1));
    Assert.assertEquals(Long.MAX_VALUE, columns.get(2));
    // why does this come back as a double when it's a float???
    Assert.assertTrue(Math.abs(3.14f - (Double) columns.get(3)) < 0.000001);
    Assert.assertTrue(Math.abs(3.14 - (Double) columns.get(4)) < 0.000001);
    Assert.assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) columns.get(5));
    Assert.assertEquals("row1", columns.get(6));
    // should not be any more
    Assert.assertFalse(results.hasNext());
  }

  @Test
  public void testSelect() throws Exception {
    String command = String.format("select int_field, double_field from %s where string_field='row1'", MY_TABLE_NAME);
    runCommand(NAMESPACE_ID, command,
               true,
               Lists.newArrayList(new ColumnDesc("int_field", "INT", 1, null),
                                  new ColumnDesc("double_field", "DOUBLE", 2, null)),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(Integer.MAX_VALUE, 3.14)))
    );
  }
}
