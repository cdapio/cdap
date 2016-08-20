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
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.test.SlowTests;
import com.google.common.collect.Lists;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
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
  }

  private static final Schema SCHEMA =
    Schema.recordOf("record",
                    Schema.Field.of("bool_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                    Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING))
  );

  @Before
  public void before() throws Exception {
    datasetFramework.addInstance(Table.class.getName(), MY_TABLE, DatasetProperties.builder()
      .add(Table.PROPERTY_SCHEMA, SCHEMA.toString())
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
    put.add("bytes_field", new byte[]{'A', 'B', 'C'});
    table.put(put);

    Assert.assertTrue(txTable.commitTx());

    transactionManager.canCommit(tx1, txTable.getTxChanges());
    transactionManager.commit(tx1);

    txTable.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    txTable.startTx(tx2);
  }

  @After
  public void after() throws Exception {
    datasetFramework.deleteInstance(MY_TABLE);
  }

  @Test
  public void testNoOpOnMissingSchema() throws Exception {
    DatasetId datasetId = new DatasetId(NAMESPACE_ID.getId(), "noschema");
    datasetFramework.addInstance(Table.class.getName(), datasetId.toId(), DatasetProperties.EMPTY);
    try {
      DatasetSpecification spec = datasetFramework.getDatasetSpec(datasetId.toId());
      Assert.assertEquals(QueryHandle.NO_OP, exploreTableManager.enableDataset(datasetId, spec));
    } finally {
      datasetFramework.deleteInstance(datasetId.toId());
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
    // check SCHEMA
    Assert.assertEquals(expectedSchema, results.getResultSchema());
    List<Object> columns = results.next().getColumns();
    // check record1
    Assert.assertFalse((Boolean) columns.get(0));
    Assert.assertEquals(Integer.MAX_VALUE, columns.get(1));
    Assert.assertEquals(Long.MAX_VALUE, columns.get(2));
    // why does this come back as a double when it's a float???
    Assert.assertTrue(Math.abs(3.14f - (Double) columns.get(3)) < 0.000001);
    Assert.assertTrue(Math.abs(3.14 - (Double) columns.get(4)) < 0.000001);
    Assert.assertArrayEquals(new byte[]{'A', 'B', 'C'}, (byte[]) columns.get(5));
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

  @Test
  public void testUpdateNoSchemaThenWithSchema() throws Exception {
    // make sure we can get the table info
    exploreService.getTableInfo(NAMESPACE_ID.getId(), MY_TABLE_NAME);
    // update the properties to be not explorable
    datasetFramework.updateInstance(MY_TABLE, DatasetProperties.EMPTY);
    // validate the new table SCHEMA
    try {
      exploreService.getTableInfo(NAMESPACE_ID.getId(), MY_TABLE_NAME);
      Assert.fail("Expected TableNotFoundException");
    } catch (TableNotFoundException e) {
      // expected
    }

    // update the properties again, to be explorable with the same schema as before
    datasetFramework.updateInstance(MY_TABLE, DatasetProperties.builder()
      .add(Table.PROPERTY_SCHEMA, SCHEMA.toString())
      .add(Table.PROPERTY_SCHEMA_ROW_FIELD, "string_field")
      .build());
    // validate explore works after update
    testSchema();
    testSelectStar();
  }

  @Test
  public void testUpdateSchema() throws Exception {
    Schema newSchema = Schema.recordOf(
      "record",
      Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("new_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("string_field", Schema.of(Schema.Type.STRING))
    );
    datasetFramework.updateInstance(MY_TABLE, DatasetProperties.builder()
      .add(Table.PROPERTY_SCHEMA, newSchema.toString())
      .add(Table.PROPERTY_SCHEMA_ROW_FIELD, "string_field")
      .build());

    // validate the new table SCHEMA
    runCommand(NAMESPACE_ID, "describe " + MY_TABLE_NAME,
               true,
               Lists.newArrayList(
                 new ColumnDesc("col_name", "STRING", 1, "from deserializer"),
                 new ColumnDesc("data_type", "STRING", 2, "from deserializer"),
                 new ColumnDesc("comment", "STRING", 3, "from deserializer")
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("int_field", "int", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("long_field", "bigint", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("float_field", "float", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("double_field", "binary", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("bytes_field", "string", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("new_field", "string", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("string_field", "string", "from deserializer"))
               )
    );

    // validate that select * returns the new SCHEMA
    List<ColumnDesc> expectedSchema = Lists.newArrayList(
      new ColumnDesc(MY_TABLE_NAME + ".int_field", "INT", 1, null),
      new ColumnDesc(MY_TABLE_NAME + ".long_field", "BIGINT", 2, null),
      new ColumnDesc(MY_TABLE_NAME + ".float_field", "FLOAT", 3, null),
      new ColumnDesc(MY_TABLE_NAME + ".double_field", "BINARY", 4, null),
      new ColumnDesc(MY_TABLE_NAME + ".bytes_field", "STRING", 5, null),
      new ColumnDesc(MY_TABLE_NAME + ".new_field", "STRING", 6, null),
      new ColumnDesc(MY_TABLE_NAME + ".string_field", "STRING", 7, null)
    );
    ExploreExecutionResult results = exploreClient.submit(NAMESPACE_ID, "select * from " + MY_TABLE_NAME).get();
    // check SCHEMA
    Assert.assertEquals(expectedSchema, results.getResultSchema());
    List<Object> columns = results.next().getColumns();
    // check record1
    Assert.assertEquals(Integer.MAX_VALUE, columns.get(0));
    Assert.assertEquals(Long.MAX_VALUE, columns.get(1));
    // why does this come back as a double when it's a float???
    Assert.assertTrue(Math.abs(3.14f - (Double) columns.get(2)) < 0.000001);
    Assert.assertArrayEquals(Bytes.toBytes(3.14D), (byte[]) columns.get(3));
    Assert.assertEquals("ABC", columns.get(4));
    Assert.assertNull(columns.get(5));
    Assert.assertEquals("row1", columns.get(6));
    // should not be any more
    Assert.assertFalse(results.hasNext());
  }

  @Test
  public void testInsert() throws Exception {
    Id.DatasetInstance otherTable = Id.DatasetInstance.from(NAMESPACE_ID, "othertable");

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("value", Schema.of(Schema.Type.INT)),
      Schema.Field.of("id", Schema.of(Schema.Type.STRING))
    );
    datasetFramework.addInstance(Table.class.getName(), otherTable, DatasetProperties.builder()
      .add(Table.PROPERTY_SCHEMA, schema.toString())
      .add(Table.PROPERTY_SCHEMA_ROW_FIELD, "id")
      .build());
    try {
      String command = String.format("insert into %s select int_field, string_field from %s",
                                     getDatasetHiveName(otherTable), MY_TABLE_NAME);
      ExploreExecutionResult result = exploreClient.submit(NAMESPACE_ID, command).get();
      Assert.assertEquals(QueryStatus.OpStatus.FINISHED, result.getStatus().getStatus());

      command = String.format("select id, value from %s", getDatasetHiveName(otherTable));
      runCommand(NAMESPACE_ID, command,
        true,
        Lists.newArrayList(new ColumnDesc("id", "STRING", 1, null),
                           new ColumnDesc("value", "INT", 2, null)),
        Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList("row1", Integer.MAX_VALUE)))
      );
    } finally {
      datasetFramework.deleteInstance(otherTable);
    }
  }

  @Test
  public void testInsertFromJoin() throws Exception {
    Id.DatasetInstance userTableID = Id.DatasetInstance.from(NAMESPACE_ID, "users");
    Id.DatasetInstance purchaseTableID = Id.DatasetInstance.from(NAMESPACE_ID, "purchases");
    Id.DatasetInstance expandedTableID = Id.DatasetInstance.from(NAMESPACE_ID, "expanded");

    Schema userSchema = Schema.recordOf(
      "user",
      Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("email", Schema.of(Schema.Type.STRING))
    );
    Schema purchaseSchema = Schema.recordOf(
      "purchase",
      Schema.Field.of("purchaseid", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("itemid", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("userid", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("ct", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE))
    );
    Schema expandedSchema = Schema.recordOf(
      "expandedPurchase",
      Schema.Field.of("purchaseid", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("itemid", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("userid", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("ct", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("username", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("email", Schema.of(Schema.Type.STRING))
    );

    datasetFramework.addInstance(Table.class.getName(), userTableID, DatasetProperties.builder()
      .add(Table.PROPERTY_SCHEMA, userSchema.toString())
      .add(Table.PROPERTY_SCHEMA_ROW_FIELD, "id")
      .build());
    datasetFramework.addInstance(Table.class.getName(), purchaseTableID, DatasetProperties.builder()
      .add(Table.PROPERTY_SCHEMA, purchaseSchema.toString())
      .add(Table.PROPERTY_SCHEMA_ROW_FIELD, "purchaseid")
      .build());
    datasetFramework.addInstance(Table.class.getName(), expandedTableID, DatasetProperties.builder()
      .add(Table.PROPERTY_SCHEMA, expandedSchema.toString())
      .add(Table.PROPERTY_SCHEMA_ROW_FIELD, "purchaseid")
      .build());

    Table userTable = datasetFramework.getDataset(userTableID, DatasetDefinition.NO_ARGUMENTS, null);
    Table purchaseTable = datasetFramework.getDataset(purchaseTableID, DatasetDefinition.NO_ARGUMENTS, null);

    TransactionAware txUserTable = (TransactionAware) userTable;
    TransactionAware txPurchaseTable = (TransactionAware) purchaseTable;
    Transaction tx1 = transactionManager.startShort(100);

    txUserTable.startTx(tx1);
    txPurchaseTable.startTx(tx1);

    Put put = new Put(Bytes.toBytes("samuel"));
    put.add("name", "Samuel Jackson");
    put.add("email", "sjackson@gmail.com");
    userTable.put(put);

    put = new Put(Bytes.toBytes(1L));
    put.add("userid", "samuel");
    put.add("itemid", "scotch");
    put.add("ct", 1);
    put.add("price", 56.99d);
    purchaseTable.put(put);

    txUserTable.commitTx();
    txPurchaseTable.commitTx();

    List<byte[]> changes = new ArrayList<>();
    changes.addAll(txUserTable.getTxChanges());
    changes.addAll(txPurchaseTable.getTxChanges());
    transactionManager.canCommit(tx1, changes);
    transactionManager.commit(tx1);

    txUserTable.postTxCommit();
    txPurchaseTable.postTxCommit();

    try {
      String command = String.format(
        "insert into table %s select P.purchaseid, P.itemid, P.userid, P.ct, P.price, U.name, U.email from " +
          "%s P join %s U on (P.userid = U.id)",
        getDatasetHiveName(expandedTableID), getDatasetHiveName(purchaseTableID), getDatasetHiveName(userTableID));
      ExploreExecutionResult result = exploreClient.submit(NAMESPACE_ID, command).get();
      Assert.assertEquals(QueryStatus.OpStatus.FINISHED, result.getStatus().getStatus());

      command = String.format("select purchaseid, itemid, userid, ct, price, username, email from %s",
                              getDatasetHiveName(expandedTableID));
      runCommand(NAMESPACE_ID, command,
        true,
        Lists.newArrayList(new ColumnDesc("purchaseid", "BIGINT", 1, null),
                           new ColumnDesc("itemid", "STRING", 2, null),
                           new ColumnDesc("userid", "STRING", 3, null),
                           new ColumnDesc("ct", "INT", 4, null),
                           new ColumnDesc("price", "DOUBLE", 5, null),
                           new ColumnDesc("username", "STRING", 6, null),
                           new ColumnDesc("email", "STRING", 7, null)),
        Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(
          1L, "scotch", "samuel", 1, 56.99d, "Samuel Jackson", "sjackson@gmail.com")))
      );
    } finally {
      datasetFramework.deleteInstance(userTableID);
      datasetFramework.deleteInstance(purchaseTableID);
      datasetFramework.deleteInstance(expandedTableID);
    }
  }
}
