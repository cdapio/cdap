package com.continuuity.explore.service;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.data2.transaction.Transaction;
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

/**
 *
 */
@Category(SlowTests.class)
public class ExploreExtensiveSchemaTableTest extends BaseHiveExploreServiceTest {

  @BeforeClass
  public static void start() throws Exception {
    startServices(CConfiguration.create());

    datasetFramework.addModule("extensiveSchema", new ExtensiveSchemaTableDefinition.ExtensiveSchemaTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("ExtensiveSchemaTable", "my_table", DatasetProperties.EMPTY);

    // Accessing dataset instance to perform data operations
    ExtensiveSchemaTableDefinition.ExtensiveSchemaTable table = datasetFramework.getDataset("my_table", null);
    Assert.assertNotNull(table);

    Transaction tx1 = transactionManager.startShort(100);
    table.startTx(tx1);

    ExtensiveSchemaTableDefinition.ExtensiveSchema value1 =
      new ExtensiveSchemaTableDefinition.ExtensiveSchema("foo", 1, 1.0f, 2.0f, (long) 1, (byte) 1, true, (short) 1,
                                                         new int[] { 1, 2 }, new float[] { 1.0f, 2.0f },
                                                         new double[] { 1.0d, 2.0d }, new long [] { 1, 2 },
                                                         new byte[] { 1, 2}, new boolean[] { true, false },
                                                         new short[] { 1, 2 });
    table.put("1", value1);

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1, table.getTxChanges());
    transactionManager.commit(tx1);

    table.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    table.startTx(tx2);
  }

  @AfterClass
  public static void stop() throws Exception {
    datasetFramework.deleteInstance("my_table");
    datasetFramework.deleteModule("extensiveSchema");
  }

  @Test
  public void test() throws Exception {
    runCommand("show tables",
               true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new Result(Lists.<Object>newArrayList("continuuity_user_my_table"))));

    runCommand("describe continuuity_user_my_table",
               true,
               Lists.newArrayList(
                 new ColumnDesc("col_name", "STRING", 1, "from deserializer"),
                 new ColumnDesc("data_type", "STRING", 2, "from deserializer"),
                 new ColumnDesc("comment", "STRING", 3, "from deserializer")
               ),
               Lists.newArrayList(
                 new Result(Lists.<Object>newArrayList("s", "string", "from deserializer")),
                 new Result(Lists.<Object>newArrayList("i", "int", "from deserializer")),
                 new Result(Lists.<Object>newArrayList("f", "float", "from deserializer")),
                 new Result(Lists.<Object>newArrayList("d", "double", "from deserializer")),
                 new Result(Lists.<Object>newArrayList("l", "bigint", "from deserializer")),
                 new Result(Lists.<Object>newArrayList("b", "tinyint", "from deserializer")),
                 new Result(Lists.<Object>newArrayList("bo", "boolean", "from deserializer")),
                 new Result(Lists.<Object>newArrayList("sh", "smallint", "from deserializer")),
                 // note: hive removes upper cases
                 new Result(Lists.<Object>newArrayList("iarr", "array<int>", "from deserializer")),
                 new Result(Lists.<Object>newArrayList("farr", "array<float>", "from deserializer")),
                 new Result(Lists.<Object>newArrayList("darr", "array<double>", "from deserializer")),
                 new Result(Lists.<Object>newArrayList("larr", "array<bigint>", "from deserializer")),
                 new Result(Lists.<Object>newArrayList("barr", "binary", "from deserializer")),
                 new Result(Lists.<Object>newArrayList("boarr", "array<boolean>", "from deserializer")),
                 new Result(Lists.<Object>newArrayList("sharr", "array<smallint>", "from deserializer"))
               )
    );

    runCommand("select * from continuuity_user_my_table",
               true,
               Lists.newArrayList(new ColumnDesc("continuuity_user_my_table.s", "STRING", 1, null),
                                  new ColumnDesc("continuuity_user_my_table.i", "INT", 2, null),
                                  new ColumnDesc("continuuity_user_my_table.f", "FLOAT", 3, null),
                                  new ColumnDesc("continuuity_user_my_table.d", "DOUBLE", 4, null),
                                  new ColumnDesc("continuuity_user_my_table.l", "BIGINT", 5, null),
                                  new ColumnDesc("continuuity_user_my_table.b", "TINYINT", 6, null),
                                  new ColumnDesc("continuuity_user_my_table.bo", "BOOLEAN", 7, null),
                                  new ColumnDesc("continuuity_user_my_table.sh", "SMALLINT", 8, null),
                                  new ColumnDesc("continuuity_user_my_table.iarr", "array<int>", 9, null),
                                  new ColumnDesc("continuuity_user_my_table.farr", "array<float>", 10, null),
                                  new ColumnDesc("continuuity_user_my_table.darr", "array<double>", 11, null),
                                  new ColumnDesc("continuuity_user_my_table.larr", "array<bigint>", 12, null),
                                  new ColumnDesc("continuuity_user_my_table.barr", "BINARY", 13, null),
                                  new ColumnDesc("continuuity_user_my_table.boarr", "array<boolean>", 14, null),
                                  new ColumnDesc("continuuity_user_my_table.sharr", "array<smallint>", 15, null)
               ),
               Lists.newArrayList(
                 new Result(Lists.<Object>newArrayList("foo", "1.0")))
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
    Assert.assertEquals("continuuity_user_my_table", rowSet.getString(1));
    stmt.close();

    stmt = connection.prepareStatement("select key, value from continuuity_user_my_table");
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
}
