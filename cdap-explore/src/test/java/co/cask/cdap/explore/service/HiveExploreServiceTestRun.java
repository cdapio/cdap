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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

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

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void start() throws Exception {
    initialize(tmpFolder);

    waitForCompletionStatus(exploreService.createNamespace(OTHER_NAMESPACE_ID), 200, TimeUnit.MILLISECONDS, 200);

    datasetFramework.addModule(KEY_STRUCT_VALUE, new KeyStructValueTableDefinition.KeyStructValueTableModule());
    datasetFramework.addModule(OTHER_KEY_STRUCT_VALUE, new KeyStructValueTableDefinition.KeyStructValueTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", MY_TABLE, DatasetProperties.EMPTY);
    datasetFramework.addInstance("keyStructValueTable", OTHER_MY_TABLE, DatasetProperties.EMPTY);

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
    datasetFramework.deleteInstance(OTHER_MY_TABLE);
    datasetFramework.deleteModule(KEY_STRUCT_VALUE);
    datasetFramework.deleteModule(OTHER_KEY_STRUCT_VALUE);

    waitForCompletionStatus(exploreService.deleteNamespace(OTHER_NAMESPACE_ID), 200, TimeUnit.MILLISECONDS, 200);
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
    exploreClient.submit(NAMESPACE_ID, "create table test (first INT, second STRING) " +
                           "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'").get();
    List<TableNameInfo> tables = exploreService.getTables(null);
    Assert.assertEquals(ImmutableList.of(new TableNameInfo(NAMESPACE_DATABASE, MY_TABLE_NAME),
                                         new TableNameInfo(NAMESPACE_DATABASE, "test"),
                                         new TableNameInfo(OTHER_NAMESPACE_DATABASE, OTHER_MY_TABLE_NAME)),
                        tables);

    tables = exploreService.getTables(NAMESPACE_ID.getId());
    Assert.assertEquals(ImmutableList.of(new TableNameInfo(NAMESPACE_DATABASE, MY_TABLE_NAME),
                                         new TableNameInfo(NAMESPACE_DATABASE, "test")),
                        tables);

    tables = exploreService.getTables(OTHER_NAMESPACE_ID.getId());
    Assert.assertEquals(ImmutableList.of(new TableNameInfo(OTHER_NAMESPACE_DATABASE, MY_TABLE_NAME)),
                        tables);

    tables = exploreService.getTables("foobar");
    Assert.assertEquals(ImmutableList.of(), tables);

    exploreClient.submit(NAMESPACE_ID, "drop table if exists test").get();
  }

  @Test
  public void testHiveIntegration() throws Exception {
    exploreClient.submit(OTHER_NAMESPACE_ID, "create table test (first INT, second STRING) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'").get();

    runCommand(NAMESPACE_ID, "show tables",
               true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(MY_TABLE_NAME))));

    runCommand(OTHER_NAMESPACE_ID, "show tables",
               true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(OTHER_MY_TABLE_NAME)),
                                  new QueryResult(Lists.<Object>newArrayList("test"))));

    runCommand(NAMESPACE_ID, "describe " + MY_TABLE_NAME,
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

    runCommand(OTHER_NAMESPACE_ID, "describe " + OTHER_MY_TABLE_NAME,
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

    runCommand(NAMESPACE_ID, "select key, value from " + MY_TABLE_NAME,
               true,
               Lists.newArrayList(new ColumnDesc("key", "STRING", 1, null),
                                  new ColumnDesc("value", "struct<name:string,ints:array<int>>", 2, null)
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("1", "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}")),
                 new QueryResult(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}")))
    );

    runCommand(NAMESPACE_ID, String.format("select key, value from %s where key = '1'", MY_TABLE_NAME),
               true,
               Lists.newArrayList(
                 new ColumnDesc("key", "STRING", 1, null),
                 new ColumnDesc("value", "struct<name:string,ints:array<int>>", 2, null)
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("1", "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}"))
               )
    );

    runCommand(NAMESPACE_ID, "select * from " + MY_TABLE_NAME,
               true,
               Lists.newArrayList(
                 new ColumnDesc(MY_TABLE_NAME + ".key", "STRING", 1, null),
                 new ColumnDesc(MY_TABLE_NAME + ".value", "struct<name:string,ints:array<int>>", 2, null)
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("1", "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}")),
                 new QueryResult(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}"))
               )
    );

    runCommand(OTHER_NAMESPACE_ID, "select * from " + OTHER_MY_TABLE_NAME,
               false,
               Lists.newArrayList(
                 new ColumnDesc(OTHER_MY_TABLE_NAME + ".key", "STRING", 1, null),
                 new ColumnDesc(OTHER_MY_TABLE_NAME + ".value", "struct<name:string,ints:array<int>>", 2, null)
               ),
               ImmutableList.<QueryResult>of()
    );

    runCommand(NAMESPACE_ID, String.format("select * from %s where key = '2'", MY_TABLE_NAME),
               true,
               Lists.newArrayList(
                 new ColumnDesc(MY_TABLE_NAME + ".key", "STRING", 1, null),
                 new ColumnDesc(MY_TABLE_NAME + ".value", "struct<name:string,ints:array<int>>", 2, null)
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}"))
               )
    );

    exploreClient.submit(OTHER_NAMESPACE_ID, "drop table if exists test").get();
  }

  @Test
  public void testQueriesList() throws Exception {
    ListenableFuture<ExploreExecutionResult> future;
    ExploreExecutionResult results;
    List<QueryInfo> queries;

    // Use different namespaces than the other tests so that when its run in a suite there isn't a chance of
    // stray queries polluting this test
    Id.Namespace testNamespace1 = Id.Namespace.from("test1");
    Id.Namespace testNamespace2 = Id.Namespace.from("test2");

    exploreClient.addNamespace(testNamespace1).get();
    exploreClient.addNamespace(testNamespace2).get();

    exploreClient.submit(testNamespace1, "create table my_table (first INT, second STRING)").get();

    future = exploreClient.submit(testNamespace1, "show tables");
    future.get();

    future = exploreClient.submit(testNamespace2, "show tables");
    future.get();

    future = exploreClient.submit(testNamespace1, "select * from my_table");
    results = future.get();

    queries = exploreService.getQueries(testNamespace1);
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

    queries = exploreService.getQueries(testNamespace1);
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
    queries = exploreService.getQueries(testNamespace1);
    Assert.assertEquals(1, queries.size());
    Assert.assertEquals("show tables", queries.get(0).getStatement());

    queries = exploreService.getQueries(testNamespace2);
    Assert.assertEquals(1, queries.size());
    Assert.assertEquals("show tables", queries.get(0).getStatement());
    Assert.assertEquals("FINISHED", queries.get(0).getStatus().toString());
    Assert.assertTrue(queries.get(0).isHasResults());
    Assert.assertTrue(queries.get(0).isActive());

    // Make sure queries are reverse ordered by timestamp
    exploreClient.submit(testNamespace1, "show tables").get();
    exploreClient.submit(testNamespace1, "show tables").get();
    exploreClient.submit(testNamespace1, "show tables").get();
    exploreClient.submit(testNamespace1, "show tables").get();

    queries = exploreService.getQueries(testNamespace1);
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

    exploreClient.submit(testNamespace1, "drop table if exists my_table").get();
    exploreClient.removeNamespace(testNamespace1).get();
    exploreClient.removeNamespace(testNamespace2).get();
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
      QueryHandle handle = exploreService.execute(NAMESPACE_ID, "show tables");
      QueryStatus status = waitForCompletionStatus(handle, 200, TimeUnit.MILLISECONDS, 50);
      Assert.assertEquals(QueryStatus.OpStatus.FINISHED, status.getStatus());

      List<QueryResult> firstPreview = exploreService.previewResults(handle);
      Assert.assertEquals(ImmutableList.of(
        new QueryResult(ImmutableList.<Object>of(MY_TABLE_NAME)),
        new QueryResult(ImmutableList.<Object>of(getDatasetHiveName(myTable2))),
        new QueryResult(ImmutableList.<Object>of(getDatasetHiveName(myTable3))),
        new QueryResult(ImmutableList.<Object>of(getDatasetHiveName(myTable4))),
        new QueryResult(ImmutableList.<Object>of(getDatasetHiveName(myTable5)))
      ), firstPreview);


      // test that preview results do not change even when the query cursor is updated by nextResults
      List<QueryResult> endResults = exploreService.nextResults(handle, 100);
      Assert.assertEquals(ImmutableList.of(
        new QueryResult(ImmutableList.<Object>of(getDatasetHiveName(myTable6)))
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

      // now test preview on a query that doesn't return any results
      handle = exploreService.execute(NAMESPACE_ID, "select * from " + getDatasetHiveName(myTable2));
      status = waitForCompletionStatus(handle, 200, TimeUnit.MILLISECONDS, 50);
      Assert.assertEquals(QueryStatus.OpStatus.FINISHED, status.getStatus());
      Assert.assertTrue(exploreService.previewResults(handle).isEmpty());
      // calling preview again should return the same thing. it should not throw an exception
      Assert.assertTrue(exploreService.previewResults(handle).isEmpty());
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
    TableInfo tableInfo = exploreService.getTableInfo(NAMESPACE_ID.getId(), MY_TABLE_NAME);
    Assert.assertEquals(new TableInfo(
                          MY_TABLE_NAME, NAMESPACE_DATABASE, System.getProperty("user.name"),
                          tableInfo.getCreationTime(), 0, 0,
                          ImmutableList.<TableInfo.ColumnInfo>of(),
                          tableInfo.getParameters(),
                          "EXTERNAL_TABLE",
                          ImmutableList.of(new TableInfo.ColumnInfo("key", "string", null),
                                           new TableInfo.ColumnInfo("value", "struct<name:string,ints:array<int>>",
                                                                    null)),
                          tableInfo.getLocation(), null, null, false, -1,
                          DatasetSerDe.class.getName(),
                          ImmutableMap.of("serialization.format", "1",
                                          Constants.Explore.DATASET_NAME, MY_TABLE.getId(),
                                          Constants.Explore.DATASET_NAMESPACE, NAMESPACE_ID.getId()),
                          true
                        ),
                        tableInfo);
    Assert.assertEquals(DatasetStorageHandler.class.getName(), tableInfo.getParameters().get("storage_handler"));

    tableInfo = exploreService.getTableInfo(NAMESPACE_ID.getId(), MY_TABLE_NAME);
    Assert.assertEquals(new TableInfo(
                          MY_TABLE_NAME, NAMESPACE_DATABASE, System.getProperty("user.name"),
                          tableInfo.getCreationTime(), 0, 0,
                          ImmutableList.<TableInfo.ColumnInfo>of(),
                          tableInfo.getParameters(),
                          "EXTERNAL_TABLE",
                          ImmutableList.of(new TableInfo.ColumnInfo("key", "string", null),
                                           new TableInfo.ColumnInfo("value", "struct<name:string,ints:array<int>>",
                                                                    null)),
                          tableInfo.getLocation(), null, null, false, -1,
                          DatasetSerDe.class.getName(),
                          ImmutableMap.of("serialization.format", "1",
                                          Constants.Explore.DATASET_NAME, MY_TABLE.getId(),
                                          Constants.Explore.DATASET_NAMESPACE, NAMESPACE_ID.getId()),
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
      exploreService.getTableInfo("foo", MY_TABLE_NAME);
      Assert.fail("Should throw TableNotFoundException on table foo.my_table");
    } catch (TableNotFoundException e) {
      // Expected
    }

    // Get info of a Hive table
    exploreClient.submit(NAMESPACE_ID, "create table test (first INT, second STRING) " +
                           "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'").get();
    tableInfo = exploreService.getTableInfo(NAMESPACE_ID.getId(), "test");
    Assert.assertEquals(new TableInfo(
                          "test", NAMESPACE_DATABASE, System.getProperty("user.name"),
                          tableInfo.getCreationTime(), 0, 0,
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
    exploreClient.submit(NAMESPACE_ID, "drop table if exists test").get();

    // Get info of a partitioned table
    exploreClient.submit(NAMESPACE_ID,
                         "CREATE TABLE page_view(viewTime INT, userid BIGINT, page_url STRING, referrer_url STRING, " +
                           "ip STRING COMMENT \"IP Address of the User\") COMMENT \"This is the page view table\" " +
                           "PARTITIONED BY(dt STRING, country STRING) STORED AS SEQUENCEFILE").get();
    tableInfo = exploreService.getTableInfo(NAMESPACE_ID.getId(), "page_view");
    Assert.assertEquals(new TableInfo(
                          "page_view", NAMESPACE_DATABASE,
                          System.getProperty("user.name"), tableInfo.getCreationTime(), 0, 0,
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
    exploreClient.submit(NAMESPACE_ID, "drop table if exists page_view").get();
  }

  @Test
  public void exploreDriverTest() throws Exception {
    // Register explore jdbc driver
    Class.forName(ExploreDriver.class.getName());

    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable = new RandomEndpointStrategy(discoveryServiceClient.discover(
      Constants.Service.EXPLORE_HTTP_USER_SERVICE)).pick();

    InetSocketAddress addr = discoverable.getSocketAddress();
    String serviceUrl = String.format("%s%s:%d?namespace=%s",
                                      Constants.Explore.Jdbc.URL_PREFIX, addr.getHostName(), addr.getPort(),
                                      NAMESPACE_ID.getId());

    Connection connection = DriverManager.getConnection(serviceUrl);
    PreparedStatement stmt;
    ResultSet rowSet;

    stmt = connection.prepareStatement("show tables");
    rowSet = stmt.executeQuery();
    Assert.assertTrue(rowSet.next());
    Assert.assertEquals(MY_TABLE_NAME, rowSet.getString(1));
    stmt.close();

    stmt = connection.prepareStatement("select key, value from " + MY_TABLE_NAME);
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
    String myTable1Name = getDatasetHiveName(myTable1);
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

      String query = String.format("select %s.key, %s.value from %s join %s on (%s.key=%s.key)",
                                   MY_TABLE_NAME, MY_TABLE_NAME,
                                   MY_TABLE_NAME, myTable1Name, MY_TABLE_NAME, myTable1Name);
      runCommand(NAMESPACE_ID, query,
                 true,
                 Lists.newArrayList(new ColumnDesc(MY_TABLE_NAME + ".key", "STRING", 1, null),
                                    new ColumnDesc(MY_TABLE_NAME + ".value",
                                                   "struct<name:string,ints:array<int>>", 2, null)),
                 Lists.newArrayList(
                   new QueryResult(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}")))
      );

      query = String.format("select %s.key, %s.value, %s.key, %s.value " +
                              "from %s right outer join %s on (%s.key=%s.key)",
                            MY_TABLE_NAME, MY_TABLE_NAME, myTable1Name, myTable1Name,
                            MY_TABLE_NAME, myTable1Name, MY_TABLE_NAME, myTable1Name);
      runCommand(NAMESPACE_ID, query,
                 true,
                 Lists.newArrayList(new ColumnDesc(MY_TABLE_NAME + ".key", "STRING", 1, null),
                                    new ColumnDesc(MY_TABLE_NAME + ".value",
                                                   "struct<name:string,ints:array<int>>", 2, null),
                                    new ColumnDesc(myTable1Name + ".key", "STRING", 3, null),
                                    new ColumnDesc(myTable1Name + ".value",
                                                   "struct<name:string,ints:array<int>>", 4, null)),
                 Lists.newArrayList(
                   new QueryResult(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}",
                                                         "2", "{\"name\":\"two\",\"ints\":[20,21,22,23,24]}")),
                   new QueryResult(Lists.<Object>newArrayList(null, null, "3",
                                                         "{\"name\":\"third\",\"ints\":[30,31,32,33,34]}")))
      );

      query = String.format("select %s.key, %s.value, %s.key, %s.value from %s " +
                              "left outer join %s on (%s.key=%s.key)",
                            MY_TABLE_NAME, MY_TABLE_NAME, myTable1Name, myTable1Name,
                            MY_TABLE_NAME, myTable1Name, MY_TABLE_NAME, myTable1Name);
      runCommand(NAMESPACE_ID, query,
                 true,
                 Lists.newArrayList(new ColumnDesc(MY_TABLE_NAME + ".key", "STRING", 1, null),
                                    new ColumnDesc(MY_TABLE_NAME + ".value",
                                                   "struct<name:string,ints:array<int>>", 2, null),
                                    new ColumnDesc(myTable1Name + ".key", "STRING", 3, null),
                                    new ColumnDesc(myTable1Name + ".value",
                                                   "struct<name:string,ints:array<int>>", 4, null)),
                 Lists.newArrayList(
                   new QueryResult(Lists.<Object>newArrayList("1",
                                                         "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}", null, null)),
                   new QueryResult(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}",
                                                         "2", "{\"name\":\"two\",\"ints\":[20,21,22,23,24]}")))
      );

      query = String.format("select %s.key, %s.value, %s.key, %s.value from %s " +
                              "full outer join %s on (%s.key=%s.key)",
                            MY_TABLE_NAME, MY_TABLE_NAME, myTable1Name, myTable1Name,
                            MY_TABLE_NAME, myTable1Name, MY_TABLE_NAME, myTable1Name);
      runCommand(NAMESPACE_ID, query,
                 true,
                 Lists.newArrayList(new ColumnDesc(MY_TABLE_NAME + ".key", "STRING", 1, null),
                                    new ColumnDesc(MY_TABLE_NAME + ".value",
                                                   "struct<name:string,ints:array<int>>", 2, null),
                                    new ColumnDesc(myTable1Name + ".key", "STRING", 3, null),
                                    new ColumnDesc(myTable1Name + ".value",
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
    ListenableFuture<ExploreExecutionResult> future = exploreClient.submit(NAMESPACE_ID,
                                                                           "select key, value from " + MY_TABLE_NAME);
    future.cancel(true);
    try {
      future.get();
      Assert.fail();
    } catch (CancellationException e) {
      // Expected
    }
  }

  @Test
  public void testNamespaceCreationDeletion() throws Exception {
    ListenableFuture<ExploreExecutionResult> future = exploreClient.schemas(null, null);
    assertStatementResult(future, true,
                          ImmutableList.of(
                            new ColumnDesc("TABLE_SCHEM", "STRING", 1, "Schema name."),
                            new ColumnDesc("TABLE_CATALOG", "STRING", 2, "Catalog name.")),
                          ImmutableList.of(
                            new QueryResult(Lists.<Object>newArrayList(NAMESPACE_DATABASE, "")),
                            new QueryResult(Lists.<Object>newArrayList(OTHER_NAMESPACE_DATABASE, "")),
                            new QueryResult(Lists.<Object>newArrayList("default", ""))));

    future = exploreClient.addNamespace(Id.Namespace.from("test"));
    future.get();

    future = exploreClient.schemas(null, null);
    assertStatementResult(future, true,
                          ImmutableList.of(
                            new ColumnDesc("TABLE_SCHEM", "STRING", 1, "Schema name."),
                            new ColumnDesc("TABLE_CATALOG", "STRING", 2, "Catalog name.")),
                          ImmutableList.of(
                            new QueryResult(Lists.<Object>newArrayList(NAMESPACE_DATABASE, "")),
                            new QueryResult(Lists.<Object>newArrayList(OTHER_NAMESPACE_DATABASE, "")),
                            new QueryResult(Lists.<Object>newArrayList("cdap_test", "")),
                            new QueryResult(Lists.<Object>newArrayList("default", ""))));

    future = exploreClient.removeNamespace(Id.Namespace.from("test"));
    future.get();

    future = exploreClient.schemas(null, null);
    assertStatementResult(future, true,
                          ImmutableList.of(
                            new ColumnDesc("TABLE_SCHEM", "STRING", 1, "Schema name."),
                            new ColumnDesc("TABLE_CATALOG", "STRING", 2, "Catalog name.")),
                          ImmutableList.of(
                            new QueryResult(Lists.<Object>newArrayList(NAMESPACE_DATABASE, "")),
                            new QueryResult(Lists.<Object>newArrayList(OTHER_NAMESPACE_DATABASE, "")),
                            new QueryResult(Lists.<Object>newArrayList("default", ""))));

  }
}
