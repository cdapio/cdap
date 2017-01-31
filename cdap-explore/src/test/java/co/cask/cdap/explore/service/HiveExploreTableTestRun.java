/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.ExploreProperties;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.test.SlowTests;
import com.google.common.collect.Lists;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

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

  private static final Schema NEW_SCHEMA =
    Schema.recordOf("record",
                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                    Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("new_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("string_field", Schema.of(Schema.Type.STRING))
    );

  private DatasetProperties setupTableProperties(@Nullable String dbName, @Nullable String tableName,
                                                 @Nullable Schema schema) {
    TableProperties.Builder props = TableProperties.builder();
    if (schema != null) {
      TableProperties.setSchema(props, schema);
      TableProperties.setRowFieldName(props, "string_field");
    }
    if (dbName != null) {
      ExploreProperties.setExploreDatabaseName(props, dbName);
    }
    if (tableName != null) {
      ExploreProperties.setExploreTableName(props, tableName);
    }
    return props.build();
  }

  private void setupTable(@Nullable String dbName, @Nullable String tableName) throws Exception {

    if (dbName != null) {
      runCommand(NAMESPACE_ID, "create database if not exists " + dbName, false, null, null);
    }
    datasetFramework.addInstance(Table.class.getName(), MY_TABLE, setupTableProperties(dbName, tableName, SCHEMA));

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

  @Test
  public void testNoOpOnMissingSchema() throws Exception {
    DatasetId datasetId = NAMESPACE_ID.dataset("noschema");
    datasetFramework.addInstance(Table.class.getName(), datasetId, DatasetProperties.EMPTY);
    try {
      DatasetSpecification spec = datasetFramework.getDatasetSpec(datasetId);
      Assert.assertEquals(QueryHandle.NO_OP, exploreTableManager.enableDataset(datasetId, spec, false));
    } finally {
      datasetFramework.deleteInstance(datasetId);
    }
  }

  private void testSchema(String tableToQuery, Schema schema) throws Exception {

    runCommand(NAMESPACE_ID, "describe " + tableToQuery,
               true,
               Lists.newArrayList(
                 new ColumnDesc("col_name", "STRING", 1, "from deserializer"),
                 new ColumnDesc("data_type", "STRING", 2, "from deserializer"),
                 new ColumnDesc("comment", "STRING", 3, "from deserializer")
               ),
               schema.equals(SCHEMA) ? Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("bool_field", "boolean", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("int_field", "int", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("long_field", "bigint", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("float_field", "float", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("double_field", "double", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("bytes_field", "binary", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("string_field", "string", "from deserializer"))
               ) : Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("int_field", "int", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("long_field", "bigint", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("float_field", "float", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("double_field", "binary", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("bytes_field", "string", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("new_field", "string", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("string_field", "string", "from deserializer"))
               ));
  }

  private void testSelectStar(String tableToQuery, String tableInSchema, Schema schema) throws Exception {

    List<ColumnDesc> expectedSchema = schema.equals(SCHEMA)
      ? Lists.newArrayList(new ColumnDesc(tableInSchema + ".bool_field", "BOOLEAN", 1, null),
                           new ColumnDesc(tableInSchema + ".int_field", "INT", 2, null),
                           new ColumnDesc(tableInSchema + ".long_field", "BIGINT", 3, null),
                           new ColumnDesc(tableInSchema + ".float_field", "FLOAT", 4, null),
                           new ColumnDesc(tableInSchema + ".double_field", "DOUBLE", 5, null),
                           new ColumnDesc(tableInSchema + ".bytes_field", "BINARY", 6, null),
                           new ColumnDesc(tableInSchema + ".string_field", "STRING", 7, null))
      : Lists.newArrayList(new ColumnDesc(tableInSchema + ".int_field", "INT", 1, null),
                           new ColumnDesc(tableInSchema + ".long_field", "BIGINT", 2, null),
                           new ColumnDesc(tableInSchema + ".float_field", "FLOAT", 3, null),
                           new ColumnDesc(tableInSchema + ".double_field", "BINARY", 4, null),
                           new ColumnDesc(tableInSchema + ".bytes_field", "STRING", 5, null),
                           new ColumnDesc(tableInSchema + ".new_field", "STRING", 6, null),
                           new ColumnDesc(tableInSchema + ".string_field", "STRING", 7, null));

    ExploreExecutionResult results = exploreClient.submit(NAMESPACE_ID, "select * from " + tableToQuery).get();
    // check SCHEMA
    Assert.assertEquals(expectedSchema, results.getResultSchema());
    List<Object> columns = results.next().getColumns();
    // check record1, account for the variability between SCHEMA and NEW_SCHEMA
    int index = 0;
    if (schema.equals(SCHEMA)) {
      Assert.assertFalse((Boolean) columns.get(index++));
    }
    Assert.assertEquals(Integer.MAX_VALUE, columns.get(index++));
    Assert.assertEquals(Long.MAX_VALUE, columns.get(index++));
    // why does this come back as a double when it's a float???
    Assert.assertTrue(Math.abs(3.14f - (Double) columns.get(index++)) < 0.000001);
    if (schema.equals(SCHEMA)) {
      Assert.assertTrue(Math.abs(3.14 - (Double) columns.get(index++)) < 0.000001);
      Assert.assertArrayEquals(new byte[]{'A', 'B', 'C'}, (byte[]) columns.get(index++));
    } else {
      Assert.assertArrayEquals(Bytes.toBytes(3.14D), (byte[]) columns.get(index++));
      Assert.assertEquals("ABC", columns.get(index++));
      Assert.assertNull(columns.get(index++));
    }
    Assert.assertEquals("row1", columns.get(index));

    // should not be any more
    Assert.assertFalse(results.hasNext());
  }

  private void testSelect(String tableToQuery, Schema schema) throws Exception {

    String command = String.format("select int_field, double_field from %s where string_field='row1'", tableToQuery);
    runCommand(NAMESPACE_ID, command,
               true,
               Lists.newArrayList(new ColumnDesc("int_field", "INT", 1, null),
                                  new ColumnDesc("double_field", schema.equals(SCHEMA) ? "DOUBLE" : "BINARY", 2, null)),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(
                 Integer.MAX_VALUE,
                 schema.equals(SCHEMA) ? 3.14 : Bytes.toBytes(3.14D))))
    );
  }

  @Test
  public void testUpdateNoSchemaThenWithSchemaThenOtherSchema() throws Exception {
    testUpdateNoSchemaThenWithSchemaThenOtherSchema(null, null);
    testUpdateNoSchemaThenWithSchemaThenOtherSchema(null, "mytt");
    testUpdateNoSchemaThenWithSchemaThenOtherSchema("yourdb", null);
    testUpdateNoSchemaThenWithSchemaThenOtherSchema("mydb", "yourtt");
  }

  private void testUpdateNoSchemaThenWithSchemaThenOtherSchema(@Nullable String dbName,
                                                               @Nullable String tableName) throws Exception {
    // create a table with SCHEMA and populate it with some data
    setupTable(dbName, tableName);

    if (tableName == null) {
      tableName = MY_TABLE_NAME;
    }
    String tableToQuery = tableName;
    if (dbName != null) {
      tableToQuery = dbName + "." + tableToQuery;
    }

    try {
      // validate the schema in explore, run sand validate some queries
      testSchema(tableToQuery, SCHEMA);
      testSelect(tableToQuery, SCHEMA);
      testSelectStar(tableToQuery, tableName, SCHEMA);

      if (tableName == null) {
        tableName = MY_TABLE_NAME;
      }
      // make sure we can get the table info
      exploreService.getTableInfo(NAMESPACE_ID.getNamespace(), dbName, tableName);
      // update the properties to be not explorable
      datasetFramework.updateInstance(MY_TABLE, setupTableProperties(dbName, tableName, null));
      // validate the table is not explorable
      try {
        exploreService.getTableInfo(NAMESPACE_ID.getNamespace(), dbName, tableName);
        Assert.fail("Expected TableNotFoundException");
      } catch (TableNotFoundException e) {
        // expected
      }

      // update the properties again, to be explorable with the same schema as before
      datasetFramework.updateInstance(MY_TABLE, setupTableProperties(dbName, tableName, SCHEMA));
      // validate explore works after update
      testSchema(tableToQuery, SCHEMA);
      testSelect(tableToQuery, SCHEMA);
      testSelectStar(tableToQuery, tableName, SCHEMA);

      // update the properties again, this time with a different schema
      datasetFramework.updateInstance(MY_TABLE, setupTableProperties(dbName, tableName, NEW_SCHEMA));
      // validate explore reflects the update
      testSchema(tableToQuery, NEW_SCHEMA);
      testSelect(tableToQuery, NEW_SCHEMA);
      testSelectStar(tableToQuery, tableName, NEW_SCHEMA);

    } finally {
      datasetFramework.deleteInstance(MY_TABLE);
      if (dbName != null) {
        runCommand(NAMESPACE_ID, "drop database if exists " + dbName + "cascade", false, null, null);
      }
    }
  }

  @Test
  public void testUpdateWithDifferentDBOrTableName() throws Exception {
    setupTable(null, null);
    try {
      datasetFramework.updateInstance(MY_TABLE, setupTableProperties(null, "xxtab", SCHEMA));
      validateDBOrTableChange(null, null, null, "xxtab");

      runCommand(NAMESPACE_ID, "create database if not exists yydb", false, null, null);
      datasetFramework.updateInstance(MY_TABLE, setupTableProperties("yydb", null, SCHEMA));
      validateDBOrTableChange(null, "xxtab", "yydb", null);

      runCommand(NAMESPACE_ID, "create database if not exists xxdb", false, null, null);
      datasetFramework.updateInstance(MY_TABLE, setupTableProperties("xxdb", null, SCHEMA));
      validateDBOrTableChange("yydb", null, "xxdb", null);

      datasetFramework.updateInstance(MY_TABLE, setupTableProperties("xxdb", "yytab", SCHEMA));
      validateDBOrTableChange("xxdb", null, "xxdb", "yytab");

      datasetFramework.updateInstance(MY_TABLE, setupTableProperties(null, null, SCHEMA));
      validateDBOrTableChange("xxdb", "yytab", null, null);
    } finally {
      datasetFramework.deleteInstance(MY_TABLE);
      runCommand(NAMESPACE_ID, "drop database if exists xxdb cascade", false, null, null);
      runCommand(NAMESPACE_ID, "drop database if exists yydb cascade", false, null, null);
    }
  }

  private void validateDBOrTableChange(@Nullable String oldDatabase, @Nullable String oldTable,
                                       @Nullable String newDatabase, @Nullable String newTable)
    throws ExploreException, TableNotFoundException {

    oldTable = oldTable != null ? oldTable : MY_TABLE_NAME;
    newTable = newTable != null ? newTable : MY_TABLE_NAME;
    try {
      exploreService.getTableInfo(NAMESPACE_ID.getNamespace(), oldDatabase, oldTable);
      Assert.fail("table should not exist: " + oldDatabase + "." + oldTable);
    } catch (TableNotFoundException e) {
      // expected - the old table should not exist
    }
    exploreService.getTableInfo(NAMESPACE_ID.getNamespace(), newDatabase, newTable);
  }


  @Test
  public void testInsert() throws Exception {
    setupTable(null, null);
    DatasetId otherTable = NAMESPACE_ID.dataset("othertable");

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("value", Schema.of(Schema.Type.INT)),
      Schema.Field.of("id", Schema.of(Schema.Type.STRING))
    );
    datasetFramework.addInstance(Table.class.getName(), otherTable, TableProperties.builder()
      .setSchema(schema)
      .setRowFieldName("id")
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
      datasetFramework.deleteInstance(MY_TABLE);
      datasetFramework.deleteInstance(otherTable);
    }
  }

  @Test
  public void testInsertFromJoin() throws Exception {
    DatasetId userTableID = NAMESPACE_ID.dataset("users");
    DatasetId purchaseTableID = NAMESPACE_ID.dataset("purchases");
    DatasetId expandedTableID = NAMESPACE_ID.dataset("expanded");

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

    datasetFramework.addInstance(Table.class.getName(), userTableID, TableProperties.builder()
      .setSchema(userSchema)
      .setRowFieldName("id")
      .build());
    datasetFramework.addInstance(Table.class.getName(), purchaseTableID,  TableProperties.builder()
      .setSchema(purchaseSchema)
      .setRowFieldName("purchaseid")
      .build());
    datasetFramework.addInstance(Table.class.getName(), expandedTableID, TableProperties.builder()
      .setSchema(expandedSchema)
      .setRowFieldName("purchaseid")
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
