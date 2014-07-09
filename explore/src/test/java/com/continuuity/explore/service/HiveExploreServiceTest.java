package com.continuuity.explore.service;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.explore.client.ExploreClientUtil;
import com.continuuity.test.SlowTests;
import com.google.common.collect.Lists;
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
    KeyStructValueTableDefinition.KeyStructValueTable table = datasetFramework.getDataset("my_table", null);
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
    // This should be processed with no exceptionbeing thrown
    datasetFramework.addModule("module2", new NotRecordScannableTableDefinition.NotRecordScannableTableModule());
    datasetFramework.addInstance("NotRecordScannableTableDef", "my_table_not_record_scannable",
                                 DatasetProperties.EMPTY);

    datasetFramework.deleteInstance("my_table_not_record_scannable");
    datasetFramework.deleteModule("module2");
  }

  @Test
  public void testTable() throws Exception {
    KeyStructValueTableDefinition.KeyStructValueTable table = datasetFramework.getDataset("my_table", null);
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
               Lists.newArrayList(new Result(Lists.<Object>newArrayList("my_table"))));

    runCommand("describe my_table",
               true,
               Lists.newArrayList(
                 new ColumnDesc("col_name", "STRING", 1, "from deserializer"),
                 new ColumnDesc("data_type", "STRING", 2, "from deserializer"),
                 new ColumnDesc("comment", "STRING", 3, "from deserializer")
               ),
               Lists.newArrayList(
                 new Result(Lists.<Object>newArrayList("key", "string", "from deserializer")),
                 new Result(Lists.<Object>newArrayList("value", "struct<name:string,ints:array<int>>",
                                                       "from deserializer"))
               )
    );

    runCommand("select key, value from my_table",
               true,
               Lists.newArrayList(new ColumnDesc("key", "STRING", 1, null),
                                  new ColumnDesc("value", "struct<name:string,ints:array<int>>", 2, null)),
               Lists.newArrayList(
                 new Result(Lists.<Object>newArrayList("1", "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}")),
                 new Result(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}")))
    );

    runCommand("select key, value from my_table where key = '1'",
               true,
               Lists.newArrayList(new ColumnDesc("key", "STRING", 1, null),
                                  new ColumnDesc("value", "struct<name:string,ints:array<int>>", 2, null)),
               Lists.newArrayList(
                 new Result(Lists.<Object>newArrayList("1", "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}")))
    );

    runCommand("select * from my_table",
               true,
               Lists.newArrayList(new ColumnDesc("my_table.key", "STRING", 1, null),
                                  new ColumnDesc("my_table.value",
                                                 "struct<name:string,ints:array<int>>", 2, null)),
               Lists.newArrayList(
                 new Result(Lists.<Object>newArrayList("1", "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}")),
                 new Result(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}")))
    );

    runCommand("select * from my_table where key = '2'",
               true,
               Lists.newArrayList(new ColumnDesc("my_table.key", "STRING", 1, null),
                                  new ColumnDesc("my_table.value",
                                                 "struct<name:string,ints:array<int>>", 2, null)),
               Lists.newArrayList(
                 new Result(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}")))
    );
  }

  @Test
  public void exploreDriverTest() throws Exception {
    // Register explore jdbc driver
    Class.forName("com.continuuity.explore.jdbc.ExploreDriver");

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

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", "my_table_1", DatasetProperties.EMPTY);

    try {
      Transaction tx1 = transactionManager.startShort(100);

      // Accessing dataset instance to perform data operations
      KeyStructValueTableDefinition.KeyStructValueTable table = datasetFramework.getDataset("my_table_1", null);
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
                   new Result(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}")))
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
                   new Result(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}",
                                                         "2", "{\"name\":\"two\",\"ints\":[20,21,22,23,24]}")),
                   new Result(Lists.<Object>newArrayList(null, null, "3",
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
                   new Result(Lists.<Object>newArrayList("1",
                                                         "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}", null, null)),
                   new Result(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}",
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
                   new Result(Lists.<Object>newArrayList("1",
                                                         "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}", null, null)),
                   new Result(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}",
                                                         "2", "{\"name\":\"two\",\"ints\":[20,21,22,23,24]}")),
                   new Result(Lists.<Object>newArrayList(null, null, "3",
                                                         "{\"name\":\"third\",\"ints\":[30,31,32,33,34]}")))
      );
    } finally {
      datasetFramework.deleteInstance("my_table_1");
    }
  }

  @Test
  public void testCancel() throws Exception {
    Handle handle = exploreClient.execute("select key, value from my_table");
    exploreClient.cancel(handle);
    Assert.assertEquals(
      Status.OpStatus.CANCELED,
      ExploreClientUtil.waitForCompletionStatus(exploreClient, handle, 200, TimeUnit.MILLISECONDS, 100).getStatus()
    );
    exploreClient.close(handle);
  }
}
