/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.explore.service;

import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.http.HttpRequest;
import com.continuuity.common.http.HttpRequests;
import com.continuuity.common.http.ObjectResponse;
import com.continuuity.explore.client.ExploreExecutionResult;
import com.continuuity.explore.jdbc.ExploreDriver;
import com.continuuity.proto.ColumnDesc;
import com.continuuity.proto.DatasetMeta;
import com.continuuity.proto.QueryHandle;
import com.continuuity.proto.QueryResult;
import com.continuuity.proto.QueryStatus;
import com.continuuity.tephra.Transaction;
import com.continuuity.test.SlowTests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.reflect.TypeToken;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import static com.continuuity.explore.service.KeyStructValueTableDefinition.KeyValue;

/**
 * Tests Hive13ExploreService.
 */
@Category(SlowTests.class)
public class HiveExploreServiceTest extends BaseHiveExploreServiceTest {
  @BeforeClass
  public static void start() throws Exception {
    startServices(CConfiguration.create());

    datasetFramework.addModule("keyStructValue", new KeyStructValueTableDefinition.KeyStructValueTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", "my_table", DatasetProperties.EMPTY);

    // Accessing dataset instance to perform data operations
    KeyStructValueTableDefinition.KeyStructValueTable table =
      datasetFramework.getDataset("my_table", DatasetDefinition.NO_ARGUMENTS, null);
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
    datasetFramework.deleteInstance("my_table");
    datasetFramework.deleteModule("keyStructValue");
  }

  @Test
  public void testDeployNotRecordScannable() throws Exception {
    // Try to deploy a dataset that is not record scannable, when explore is enabled.
    // This should be processed with no exception being thrown
    datasetFramework.addModule("module2", new NotRecordScannableTableDefinition.NotRecordScannableTableModule());
    datasetFramework.addInstance("NotRecordScannableTableDef", "my_table_not_record_scannable",
                                 DatasetProperties.EMPTY);

    datasetFramework.deleteInstance("my_table_not_record_scannable");
    datasetFramework.deleteModule("module2");
  }

  @Test
  public void testTable() throws Exception {
    KeyStructValueTableDefinition.KeyStructValueTable table =
      datasetFramework.getDataset("my_table", DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);
    Transaction tx = transactionManager.startShort(100);
    table.startTx(tx);
    Assert.assertEquals(new KeyValue.Value("first", Lists.newArrayList(1, 2, 3, 4, 5)), table.get("1"));
    transactionManager.abort(tx);
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

    List<QueryInfo> result = exploreService.getQueries();
    List<Long> timestamps = Lists.newArrayList();
    Assert.assertTrue(result.size() > 0);
    for (QueryInfo queryInfo : result) {
      Assert.assertNotNull(queryInfo.getStatement());
      Assert.assertNotNull(queryInfo.getQueryHandle());
      Assert.assertFalse(queryInfo.isActive());
      Assert.assertEquals("FINISHED", queryInfo.getStatus().toString());
      timestamps.add(queryInfo.getTimestamp());
    }

    // verify the ordering
    Assert.assertTrue(Ordering.natural().reverse().isOrdered(timestamps));
  }

  @Test
  public void previewResultsTest() throws Exception {
    datasetFramework.addInstance("keyStructValueTable", "my_table_2", DatasetProperties.EMPTY);
    datasetFramework.addInstance("keyStructValueTable", "my_table_3", DatasetProperties.EMPTY);
    datasetFramework.addInstance("keyStructValueTable", "my_table_4", DatasetProperties.EMPTY);
    datasetFramework.addInstance("keyStructValueTable", "my_table_5", DatasetProperties.EMPTY);
    datasetFramework.addInstance("keyStructValueTable", "my_table_6", DatasetProperties.EMPTY);

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
      datasetFramework.deleteInstance("my_table_2");
      datasetFramework.deleteInstance("my_table_3");
      datasetFramework.deleteInstance("my_table_4");
      datasetFramework.deleteInstance("my_table_5");
      datasetFramework.deleteInstance("my_table_6");
    }
  }

  @Test
  public void getDatasetSchemaTest() throws Exception {
    Map<String, String> datasetSchema = exploreClient.datasetSchema("my_table");
    Assert.assertEquals(ImmutableMap.of("key", "string", "value", "struct<name:string,ints:array<int>>"),
                        datasetSchema);
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
  public void getDatasetsTest() throws Exception {

    datasetFramework.addModule("module2", new NotRecordScannableTableDefinition.NotRecordScannableTableModule());
    datasetFramework.addInstance("NotRecordScannableTableDef", "my_table_not_record_scannable",
                                 DatasetProperties.EMPTY);

    ObjectResponse<List<?>> datasets;
    HttpRequest request;
    InetSocketAddress address = datasetManagerEndpointStrategy.pick().getSocketAddress();
    URI baseURI = new URI(String.format("http://%s:%d/", address.getHostName(), address.getPort()));

    request = HttpRequest.get(baseURI.resolve("v2/data/datasets?explorable=true").toURL()).build();
    datasets = ObjectResponse.fromJsonBody(HttpRequests.execute(request),
                                           new TypeToken<List<DatasetSpecification>>() { }.getType());
    Assert.assertEquals(1, datasets.getResponseObject().size());
    Assert.assertEquals("my_table", ((DatasetSpecification) datasets.getResponseObject().get(0)).getName());

    request = HttpRequest.get(baseURI.resolve("v2/data/datasets?explorable=false").toURL()).build();
    datasets = ObjectResponse.fromJsonBody(HttpRequests.execute(request), new TypeToken<List<DatasetSpecification>>() {
    }.getType());
    Assert.assertEquals(1, datasets.getResponseObject().size());
    Assert.assertEquals("my_table_not_record_scannable",
                        ((DatasetSpecification) datasets.getResponseObject().get(0)).getName());

    request = HttpRequest.get(baseURI.resolve("v2/data/datasets?meta=true&explorable=true").toURL()).build();
    datasets = ObjectResponse.fromJsonBody(HttpRequests.execute(request), new TypeToken<List<DatasetMeta>>() {
    }.getType());
    Assert.assertEquals(1, datasets.getResponseObject().size());
    Assert.assertEquals("my_table", ((DatasetMeta) datasets.getResponseObject().get(0)).getSpec().getName());

    request = HttpRequest.get(baseURI.resolve("v2/data/datasets?meta=true&explorable=false").toURL()).build();
    datasets = ObjectResponse.fromJsonBody(HttpRequests.execute(request),
                                           new TypeToken<List<DatasetMeta>>() { }.getType());
    Assert.assertEquals(1, datasets.getResponseObject().size());
    Assert.assertEquals("my_table_not_record_scannable",
                        ((DatasetMeta) datasets.getResponseObject().get(0)).getSpec().getName());

    request = HttpRequest.get(baseURI.resolve("v2/data/datasets?meta=true").toURL()).build();
    datasets = ObjectResponse.fromJsonBody(HttpRequests.execute(request),
                                           new TypeToken<List<DatasetMeta>>() { }.getType());
    Assert.assertEquals(2, datasets.getResponseObject().size());

    datasetFramework.deleteInstance("my_table_not_record_scannable");
    datasetFramework.deleteModule("module2");
  }

  @Test
  public void testJoin() throws Exception {

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", "my_table_1", DatasetProperties.EMPTY);

    try {
      Transaction tx1 = transactionManager.startShort(100);

      // Accessing dataset instance to perform data operations
      KeyStructValueTableDefinition.KeyStructValueTable table =
        datasetFramework.getDataset("my_table_1", DatasetDefinition.NO_ARGUMENTS, null);
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
      datasetFramework.deleteInstance("my_table_1");
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
