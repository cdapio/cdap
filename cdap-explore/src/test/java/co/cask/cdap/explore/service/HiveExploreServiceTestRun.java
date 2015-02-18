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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.explore.jdbc.ExploreDriver;
import co.cask.cdap.explore.service.datasets.KeyStructValueTableDefinition;
import co.cask.cdap.explore.service.datasets.NotRecordScannableTableDefinition;
import co.cask.cdap.hive.datasets.DatasetInputFormat;
import co.cask.cdap.hive.datasets.DatasetSerDe;
import co.cask.cdap.hive.datasets.DatasetStorageHandler;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryInfo;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.TableInfo;
import co.cask.cdap.proto.TableNameInfo;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.Transaction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import static co.cask.cdap.explore.service.datasets.KeyStructValueTableDefinition.KeyValue;

/**
 * Tests Hive13ExploreService.
 */
@Category(SlowTests.class)
public class HiveExploreServiceTestRun extends BaseHiveExploreServiceTest {
  @BeforeClass
  public static void start() throws Exception {
    startServices();

    datasetFramework.addModule(KEY_STRUCT_VALUE, new KeyStructValueTableDefinition.KeyStructValueTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", MY_TABLE, DatasetProperties.EMPTY);

    // Accessing dataset instance to perform data operations
    KeyStructValueTableDefinition.KeyStructValueTable table =
      datasetFramework.getDataset(MY_TABLE, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);

    Transaction tx1 = transactionManager.startShort(100);
    table.startTx(tx1);

    KeyValue.Value value1 = new KeyValue.Value("first", Lists.newArrayList(1, 2, 3, 4, 5));
    KeyValue.Value value2 = new KeyValue.Value("two", Lists.newArrayList(10, 11, 12, 13, 14));
    table.put("1", value1);
    table.put("2", value2);
    Assert.assertEquals(value1, table.get("1"));

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1, table.getTxChanges());
    transactionManager.commit(tx1);

    table.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    table.startTx(tx2);

    Assert.assertEquals(value1, table.get("1"));
  }

  @AfterClass
  public static void stop() throws Exception {
    datasetFramework.deleteInstance(MY_TABLE);
    datasetFramework.deleteModule(KEY_STRUCT_VALUE);
  }

  @Test
  public void testDeployNotRecordScannable() throws Exception {
    // Try to deploy a dataset that is not record scannable, when explore is enabled.
    // This should be processed with no exception being thrown
    Id.DatasetModule module2 = Id.DatasetModule.from(NAMESPACE_ID, "module2");
    Id.DatasetInstance myTableNotRecordScannable = Id.DatasetInstance.from(NAMESPACE_ID,
                                                                           "my_table_not_record_scannable");
    datasetFramework.addModule(module2, new NotRecordScannableTableDefinition.NotRecordScannableTableModule());
    datasetFramework.addInstance("NotRecordScannableTableDef", myTableNotRecordScannable,
                                 DatasetProperties.EMPTY);

    datasetFramework.deleteInstance(myTableNotRecordScannable);
    datasetFramework.deleteModule(module2);
  }

  @Test
  public void testTable() throws Exception {
    KeyStructValueTableDefinition.KeyStructValueTable table =
      datasetFramework.getDataset(MY_TABLE, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);
    Transaction tx = transactionManager.startShort(100);
    table.startTx(tx);
    Assert.assertEquals(new KeyValue.Value("first", Lists.newArrayList(1, 2, 3, 4, 5)), table.get("1"));
    transactionManager.abort(tx);
  }

  @Test
  public void getUserTables() throws Exception {
    exploreClient.submit("create table test (first INT, second STRING) " +
                           "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'").get();
    List<TableNameInfo> tables = exploreService.getTables(null);
    Assert.assertEquals(ImmutableList.of(new TableNameInfo("default", "my_table"),
                                         new TableNameInfo("default", "test")),
                        tables);

    tables = exploreService.getTables("default");
    Assert.assertEquals(ImmutableList.of(new TableNameInfo("default", "my_table"),
                                         new TableNameInfo("default", "test")),
                        tables);

    tables = exploreService.getTables("foobar");
    Assert.assertEquals(ImmutableList.of(), tables);

    exploreClient.submit("drop table if exists test").get();
  }

  @Test
  public void testHiveIntegration() throws Exception {
    runCommand("show tables",
               true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList("my_table"))));

    runCommand("describe my_table",
               true,
               Lists.newArrayList(
                 new ColumnDesc("col_name", "STRING", 1, "from deserializer"),
                 new ColumnDesc("data_type", "STRING", 2, "from deserializer"),
                 new ColumnDesc("comment", "STRING", 3, "from deserializer")
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("key", "string", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("value", "struct<name:string,ints:array<int>>",
                                                            "from deserializer"))
               )
    );

    runCommand("select key, value from my_table",
               true,
               Lists.newArrayList(new ColumnDesc("key", "STRING", 1, null),
                                  new ColumnDesc("value", "struct<name:string,ints:array<int>>", 2, null)
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("1", "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}")),
                 new QueryResult(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}")))
    );

    runCommand("select key, value from my_table where key = '1'",
               true,
               Lists.newArrayList(
                 new ColumnDesc("key", "STRING", 1, null),
                 new ColumnDesc("value", "struct<name:string,ints:array<int>>", 2, null)
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("1", "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}"))
               )
    );

    runCommand("select * from my_table",
               true,
               Lists.newArrayList(
                 new ColumnDesc("my_table.key", "STRING", 1, null),
                 new ColumnDesc("my_table.value", "struct<name:string,ints:array<int>>", 2, null)
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("1", "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}")),
                 new QueryResult(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}"))
               )
    );

    runCommand("select * from my_table where key = '2'",
               true,
               Lists.newArrayList(
                 new ColumnDesc("my_table.key", "STRING", 1, null),
                 new ColumnDesc("my_table.value", "struct<name:string,ints:array<int>>", 2, null)
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}"))
               )
    );
  }

  @Test
  public void queriesListTest() throws Exception {
    ListenableFuture<ExploreExecutionResult> future;
    ExploreExecutionResult results;
    List<QueryInfo> queries;

    future = exploreClient.submit("show tables");
    future.get();

    future = exploreClient.submit("select * from my_table");
    results = future.get();

    queries = exploreService.getQueries();
    Assert.assertEquals(2, queries.size());
    Assert.assertEquals("select * from my_table", queries.get(0).getStatement());
    Assert.assertEquals("FINISHED", queries.get(0).getStatus().toString());
    Assert.assertTrue(queries.get(0).isHasResults());
    Assert.assertTrue(queries.get(0).isActive());
    Assert.assertEquals("show tables", queries.get(1).getStatement());
    Assert.assertEquals("FINISHED", queries.get(1).getStatus().toString());
    Assert.assertTrue(queries.get(1).isHasResults());
    Assert.assertTrue(queries.get(1).isActive());

    // Make the last query inactive
    while (results.hasNext()) {
      results.next();
    }
    queries = exploreService.getQueries();
    Assert.assertEquals(2, queries.size());
    Assert.assertEquals("select * from my_table", queries.get(0).getStatement());
    Assert.assertEquals("FINISHED", queries.get(0).getStatus().toString());
    Assert.assertTrue(queries.get(0).isHasResults());
    Assert.assertFalse(queries.get(0).isActive());
    Assert.assertEquals("show tables", queries.get(1).getStatement());
    Assert.assertEquals("FINISHED", queries.get(1).getStatus().toString());
    Assert.assertTrue(queries.get(1).isHasResults());
    Assert.assertTrue(queries.get(1).isActive());

    // Close last query
    results.close();
    queries = exploreService.getQueries();
    Assert.assertEquals(1, queries.size());
    Assert.assertEquals("show tables", queries.get(0).getStatement());

    // Make sure queries are reverse ordered by timestamp
    exploreClient.submit("show tables").get();
    exploreClient.submit("show tables").get();
    exploreClient.submit("show tables").get();
    exploreClient.submit("show tables").get();

    queries = exploreService.getQueries();
    List<Long> timestamps = Lists.newArrayList();
    Assert.assertEquals(5, queries.size());
    for (QueryInfo queryInfo : queries) {
      Assert.assertNotNull(queryInfo.getStatement());
      Assert.assertNotNull(queryInfo.getQueryHandle());
      Assert.assertTrue(queryInfo.isActive());
      Assert.assertEquals("FINISHED", queryInfo.getStatus().toString());
      Assert.assertEquals("show tables", queryInfo.getStatement());
      timestamps.add(queryInfo.getTimestamp());
    }

    // verify the ordering
    Assert.assertTrue(Ordering.natural().reverse().isOrdered(timestamps));
  }

  @Test
  public void previewResultsTest() throws Exception {
    Id.DatasetInstance myTable2 = Id.DatasetInstance.from(NAMESPACE_ID, "my_table_2");
    Id.DatasetInstance myTable3 = Id.DatasetInstance.from(NAMESPACE_ID, "my_table_3");
    Id.DatasetInstance myTable4 = Id.DatasetInstance.from(NAMESPACE_ID, "my_table_4");
    Id.DatasetInstance myTable5 = Id.DatasetInstance.from(NAMESPACE_ID, "my_table_5");
    Id.DatasetInstance myTable6 = Id.DatasetInstance.from(NAMESPACE_ID, "my_table_6");
    datasetFramework.addInstance("keyStructValueTable", myTable2, DatasetProperties.EMPTY);
    datasetFramework.addInstance("keyStructValueTable", myTable3, DatasetProperties.EMPTY);
    datasetFramework.addInstance("keyStructValueTable", myTable4, DatasetProperties.EMPTY);
    datasetFramework.addInstance("keyStructValueTable", myTable5, DatasetProperties.EMPTY);
    datasetFramework.addInstance("keyStructValueTable", myTable6, DatasetProperties.EMPTY);

    try {
      QueryHandle handle = exploreService.execute("show tables");
      QueryStatus status = waitForCompletionStatus(handle, 200, TimeUnit.MILLISECONDS, 50);
      Assert.assertEquals(QueryStatus.OpStatus.FINISHED, status.getStatus());

      List<QueryResult> firstPreview = exploreService.previewResults(handle);
      Assert.assertEquals(ImmutableList.of(
        new QueryResult(ImmutableList.<Object>of("my_table")),
        new QueryResult(ImmutableList.<Object>of("my_table_2")),
        new QueryResult(ImmutableList.<Object>of("my_table_3")),
        new QueryResult(ImmutableList.<Object>of("my_table_4")),
        new QueryResult(ImmutableList.<Object>of("my_table_5"))
      ), firstPreview);


      List<QueryResult> endResults = exploreService.nextResults(handle, 100);
      Assert.assertEquals(ImmutableList.of(
        new QueryResult(ImmutableList.<Object>of("my_table_6"))
      ), endResults);

      List<QueryResult> secondPreview = exploreService.previewResults(handle);
      Assert.assertEquals(firstPreview, secondPreview);

      Assert.assertEquals(ImmutableList.of(), exploreService.nextResults(handle, 100));

      try {
        // All results are fetched, query should be inactive now
        exploreService.previewResults(handle);
        Assert.fail("HandleNotFoundException expected - query should be inactive.");
      } catch (HandleNotFoundException e) {
        Assert.assertTrue(e.isInactive());
        // Expected exception
      }

    } finally {
      datasetFramework.deleteInstance(myTable2);
      datasetFramework.deleteInstance(myTable3);
      datasetFramework.deleteInstance(myTable4);
      datasetFramework.deleteInstance(myTable5);
      datasetFramework.deleteInstance(myTable6);
    }
  }

  @Test
  public void getDatasetSchemaTest() throws Exception {
    TableInfo tableInfo = exploreService.getTableInfo(null, "my_table");
    Assert.assertEquals(new TableInfo(
                          "my_table", "default", System.getProperty("user.name"), tableInfo.getCreationTime(), 0, 0,
                          ImmutableList.<TableInfo.ColumnInfo>of(),
                          tableInfo.getParameters(),
                          "EXTERNAL_TABLE",
                          ImmutableList.of(new TableInfo.ColumnInfo("key", "string", null),
                                           new TableInfo.ColumnInfo("value", "struct<name:string,ints:array<int>>",
                                                                    null)),
                          tableInfo.getLocation(), DatasetInputFormat.class.getName(),
                          "org.apache.hadoop.hive.ql.io.HivePassThroughOutputFormat", false, -1,
                          DatasetSerDe.class.getName(),
                          ImmutableMap.of("serialization.format", "1", "explore.dataset.name", "my_table"),
                          true
                        ),
                        tableInfo);
    Assert.assertEquals(DatasetStorageHandler.class.getName(), tableInfo.getParameters().get("storage_handler"));

    tableInfo = exploreService.getTableInfo("default", "my_table");
    Assert.assertEquals(new TableInfo(
                          "my_table", "default", System.getProperty("user.name"), tableInfo.getCreationTime(), 0, 0,
                          ImmutableList.<TableInfo.ColumnInfo>of(),
                          tableInfo.getParameters(),
                          "EXTERNAL_TABLE",
                          ImmutableList.of(new TableInfo.ColumnInfo("key", "string", null),
                                           new TableInfo.ColumnInfo("value", "struct<name:string,ints:array<int>>",
                                                                    null)),
                          tableInfo.getLocation(), DatasetInputFormat.class.getName(),
                          "org.apache.hadoop.hive.ql.io.HivePassThroughOutputFormat", false, -1,
                          DatasetSerDe.class.getName(),
                          ImmutableMap.of("serialization.format", "1", "explore.dataset.name", "my_table"),
                          true
                        ),
                        tableInfo);
    Assert.assertEquals(DatasetStorageHandler.class.getName(), tableInfo.getParameters().get("storage_handler"));

    try {
      exploreService.getTableInfo(null, "foobar");
      Assert.fail("Should throw TableNotFoundException on table foobar");
    } catch (TableNotFoundException e) {
      // Expected
    }

    try {
      exploreService.getTableInfo("foo", "my_table");
      Assert.fail("Should throw TableNotFoundException on table foo.my_table");
    } catch (TableNotFoundException e) {
      // Expected
    }

    // Get info of a Hive table
    exploreClient.submit("create table test (first INT, second STRING) " +
                           "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'").get();
    tableInfo = exploreService.getTableInfo(null, "test");
    Assert.assertEquals(new TableInfo(
                          "test", "default", System.getProperty("user.name"), tableInfo.getCreationTime(), 0, 0,
                          ImmutableList.<TableInfo.ColumnInfo>of(),
                          tableInfo.getParameters(),
                          "MANAGED_TABLE",
                          ImmutableList.of(new TableInfo.ColumnInfo("first", "int", null),
                                           new TableInfo.ColumnInfo("second", "string", null)),
                          tableInfo.getLocation(), "org.apache.hadoop.mapred.TextInputFormat",
                          "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat", false, -1,
                          "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                          ImmutableMap.of("serialization.format", "\t", "field.delim", "\t"),
                          false
                        ),
                        tableInfo);
    exploreClient.submit("drop table if exists test").get();

    // Get info of a partitioned table
    exploreClient.submit("CREATE TABLE page_view(viewTime INT, userid BIGINT, page_url STRING, referrer_url STRING, " +
                           "ip STRING COMMENT \"IP Address of the User\") COMMENT \"This is the page view table\" " +
                           "PARTITIONED BY(dt STRING, country STRING) STORED AS SEQUENCEFILE").get();
    tableInfo = exploreService.getTableInfo(null, "page_view");
    Assert.assertEquals(new TableInfo(
                          "page_view", "default", System.getProperty("user.name"), tableInfo.getCreationTime(), 0, 0,
                          ImmutableList.of(
                            new TableInfo.ColumnInfo("dt", "string", null),
                            new TableInfo.ColumnInfo("country", "string", null)
                          ),
                          tableInfo.getParameters(),
                          "MANAGED_TABLE",
                          ImmutableList.of(
                            new TableInfo.ColumnInfo("viewtime", "int", null),
                            new TableInfo.ColumnInfo("userid", "bigint", null),
                            new TableInfo.ColumnInfo("page_url", "string", null),
                            new TableInfo.ColumnInfo("referrer_url", "string", null),
                            new TableInfo.ColumnInfo("ip", "string", "IP Address of the User"),
                            new TableInfo.ColumnInfo("dt", "string", null),
                            new TableInfo.ColumnInfo("country", "string", null)
                          ),
                          tableInfo.getLocation(),
                          "org.apache.hadoop.mapred.SequenceFileInputFormat",
                          "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat", false, -1,
                          "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                          ImmutableMap.of("serialization.format", "1"),
                          false
                        ),
                        tableInfo);
    Assert.assertEquals("This is the page view table", tableInfo.getParameters().get("comment"));
    exploreClient.submit("drop table if exists page_view").get();
  }

  @Test
  public void exploreDriverTest() throws Exception {
    // Register explore jdbc driver
    Class.forName(ExploreDriver.class.getName());

    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable = new RandomEndpointStrategy(discoveryServiceClient.discover(
      Constants.Service.EXPLORE_HTTP_USER_SERVICE)).pick();

    InetSocketAddress addr = discoverable.getSocketAddress();
    String serviceUrl = String.format("%s%s:%d", Constants.Explore.Jdbc.URL_PREFIX, addr.getHostName(), addr.getPort());

    Connection connection = DriverManager.getConnection(serviceUrl);
    PreparedStatement stmt;
    ResultSet rowSet;

    stmt = connection.prepareStatement("show tables");
    rowSet = stmt.executeQuery();
    Assert.assertTrue(rowSet.next());
    Assert.assertEquals("my_table", rowSet.getString(1));
    stmt.close();

    stmt = connection.prepareStatement("select key, value from my_table");
    rowSet = stmt.executeQuery();
    Assert.assertTrue(rowSet.next());
    Assert.assertEquals(1, rowSet.getInt(1));
    Assert.assertEquals("{\"name\":\"first\",\"ints\":[1,2,3,4,5]}", rowSet.getString(2));
    Assert.assertTrue(rowSet.next());
    Assert.assertEquals(2, rowSet.getInt(1));
    Assert.assertEquals("{\"name\":\"two\",\"ints\":[10,11,12,13,14]}", rowSet.getString(2));
    stmt.close();

    connection.close();
  }

  @Test
  public void testJoin() throws Exception {
    Id.DatasetInstance myTable1 = Id.DatasetInstance.from(NAMESPACE_ID, "my_table_1");
    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", myTable1, DatasetProperties.EMPTY);

    try {
      Transaction tx1 = transactionManager.startShort(100);

      // Accessing dataset instance to perform data operations
      KeyStructValueTableDefinition.KeyStructValueTable table =
        datasetFramework.getDataset(myTable1, DatasetDefinition.NO_ARGUMENTS, null);
      Assert.assertNotNull(table);
      table.startTx(tx1);

      KeyValue.Value value1 = new KeyValue.Value("two", Lists.newArrayList(20, 21, 22, 23, 24));
      KeyValue.Value value2 = new KeyValue.Value("third", Lists.newArrayList(30, 31, 32, 33, 34));
      table.put("2", value1);
      table.put("3", value2);
      Assert.assertEquals(value1, table.get("2"));

      Assert.assertTrue(table.commitTx());

      transactionManager.canCommit(tx1, table.getTxChanges());
      transactionManager.commit(tx1);

      table.postTxCommit();


      runCommand("select my_table.key, my_table.value from " +
                   "my_table " +
                   "join my_table_1 on (my_table.key=my_table_1.key)",
                 true,
                 Lists.newArrayList(new ColumnDesc("my_table.key", "STRING", 1, null),
                                    new ColumnDesc("my_table.value",
                                                   "struct<name:string,ints:array<int>>", 2, null)),
                 Lists.newArrayList(
                   new QueryResult(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}")))
      );

      runCommand("select my_table.key, my_table.value, my_table_1.key, my_table_1.value from " +
                   "my_table " +
                   "right outer join my_table_1 on (my_table.key=my_table_1.key)",
                 true,
                 Lists.newArrayList(new ColumnDesc("my_table.key", "STRING", 1, null),
                                    new ColumnDesc("my_table.value", "struct<name:string,ints:array<int>>", 2, null),
                                    new ColumnDesc("my_table_1.key", "STRING", 3, null),
                                    new ColumnDesc("my_table_1.value",
                                                   "struct<name:string,ints:array<int>>", 4, null)),
                 Lists.newArrayList(
                   new QueryResult(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}",
                                                         "2", "{\"name\":\"two\",\"ints\":[20,21,22,23,24]}")),
                   new QueryResult(Lists.<Object>newArrayList(null, null, "3",
                                                         "{\"name\":\"third\",\"ints\":[30,31,32,33,34]}")))
      );

      runCommand("select my_table.key, my_table.value, my_table_1.key, my_table_1.value from " +
                   "my_table " +
                   "left outer join my_table_1 on (my_table.key=my_table_1.key)",
                 true,
                 Lists.newArrayList(new ColumnDesc("my_table.key", "STRING", 1, null),
                                    new ColumnDesc("my_table.value", "struct<name:string,ints:array<int>>", 2, null),
                                    new ColumnDesc("my_table_1.key", "STRING", 3, null),
                                    new ColumnDesc("my_table_1.value",
                                                   "struct<name:string,ints:array<int>>", 4, null)),
                 Lists.newArrayList(
                   new QueryResult(Lists.<Object>newArrayList("1",
                                                         "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}", null, null)),
                   new QueryResult(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}",
                                                         "2", "{\"name\":\"two\",\"ints\":[20,21,22,23,24]}")))
      );

      runCommand("select my_table.key, my_table.value, my_table_1.key, my_table_1.value from " +
                   "my_table " +
                   "full outer join my_table_1 on (my_table.key=my_table_1.key)",
                 true,
                 Lists.newArrayList(new ColumnDesc("my_table.key", "STRING", 1, null),
                                    new ColumnDesc("my_table.value", "struct<name:string,ints:array<int>>", 2, null),
                                    new ColumnDesc("my_table_1.key", "STRING", 3, null),
                                    new ColumnDesc("my_table_1.value",
                                                   "struct<name:string,ints:array<int>>", 4, null)),
                 Lists.newArrayList(
                   new QueryResult(Lists.<Object>newArrayList("1",
                                                         "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}", null, null)),
                   new QueryResult(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}",
                                                         "2", "{\"name\":\"two\",\"ints\":[20,21,22,23,24]}")),
                   new QueryResult(Lists.<Object>newArrayList(null, null, "3",
                                                         "{\"name\":\"third\",\"ints\":[30,31,32,33,34]}")))
      );
    } finally {
      datasetFramework.deleteInstance(myTable1);
    }
  }

  @Test
  public void testCancel() throws Exception {
    ListenableFuture<ExploreExecutionResult> future = exploreClient.submit("select key, value from my_table");
    future.cancel(true);
    try {
      future.get();
      Assert.fail();
    } catch (CancellationException e) {
      // Expected
    }
  }
}
