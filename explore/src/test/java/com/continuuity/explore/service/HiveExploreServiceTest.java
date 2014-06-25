package com.continuuity.explore.service;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetServiceModules;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.explore.client.DiscoveryExploreClient;
import com.continuuity.explore.client.ExploreClient;
import com.continuuity.explore.client.ExploreClientUtil;
import com.continuuity.explore.executor.ExploreExecutorService;
import com.continuuity.explore.guice.ExploreRuntimeModule;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.test.SlowTests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.continuuity.explore.service.KeyStructValueTableDefinition.KeyValue;

/**
 * Tests Hive13ExploreService.
 */
@Category(SlowTests.class)
public class HiveExploreServiceTest {
  private static Injector injector;
  private static InMemoryTransactionManager transactionManager;
  private static DatasetFramework datasetFramework;
  private static DatasetService datasetService;
  private static ExploreExecutorService exploreExecutorService;
  private static ExploreClient exploreClient;

  @BeforeClass
  public static void start() throws Exception {
    injector = Guice.createInjector(createInMemoryModules(CConfiguration.create(), new Configuration()));
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    transactionManager.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    exploreExecutorService.startAndWait();

    datasetFramework = injector.getInstance(DatasetFramework.class);
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

    exploreClient = injector.getInstance(DiscoveryExploreClient.class);
    Assert.assertTrue(exploreClient.isAvailable());

  }

  @AfterClass
  public static void stop() throws Exception {
    datasetFramework.deleteInstance("my_table");
    datasetFramework.deleteModule("keyStructValue");

    exploreExecutorService.stopAndWait();
    datasetService.stopAndWait();
    transactionManager.stopAndWait();
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
               Lists.newArrayList(new Result(Lists.<Object>newArrayList("continuuity_user_my_table"))));

    runCommand("describe continuuity_user_my_table",
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

    runCommand("select key, value from continuuity_user_my_table",
        true,
        Lists.newArrayList(new ColumnDesc("key", "STRING", 1, null),
                           new ColumnDesc("value", "struct<name:string,ints:array<int>>", 2, null)),
        Lists.newArrayList(
          new Result(Lists.<Object>newArrayList("1", "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}")),
          new Result(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}")))
    );

    runCommand("select key, value from continuuity_user_my_table where key = '1'",
               true,
               Lists.newArrayList(new ColumnDesc("key", "STRING", 1, null),
                                  new ColumnDesc("value", "struct<name:string,ints:array<int>>", 2, null)),
               Lists.newArrayList(
                 new Result(Lists.<Object>newArrayList("1", "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}")))
    );

    runCommand("select * from continuuity_user_my_table",
               true,
               Lists.newArrayList(new ColumnDesc("continuuity_user_my_table.key", "STRING", 1, null),
                                  new ColumnDesc("continuuity_user_my_table.value",
                                                 "struct<name:string,ints:array<int>>", 2, null)),
               Lists.newArrayList(
                 new Result(Lists.<Object>newArrayList("1", "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}")),
                 new Result(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}")))
    );

    runCommand("select * from continuuity_user_my_table where key = '2'",
               true,
               Lists.newArrayList(new ColumnDesc("continuuity_user_my_table.key", "STRING", 1, null),
                                  new ColumnDesc("continuuity_user_my_table.value",
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

  @Test
  public void testJoin() throws Exception {

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", "my_table_1", DatasetProperties.EMPTY);

    Transaction tx1 = transactionManager.startShort(100);

    // Accessing dataset instance to perform data operations
    KeyStructValueTableDefinition.KeyStructValueTable table = datasetFramework.getDataset("my_table_1", null);
    Assert.assertNotNull(table);
    table.startTx(tx1);

    KeyValue.Value value1 = new KeyValue.Value("two", Lists.newArrayList(10, 11, 12, 13, 14));
    KeyValue.Value value2 = new KeyValue.Value("third", Lists.newArrayList(10, 11, 12, 13, 14));
    table.put("2", value1);
    table.put("3", value2);
    Assert.assertEquals(value1, table.get("2"));

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1, table.getTxChanges());
    transactionManager.commit(tx1);

    table.postTxCommit();


    runCommand("select continuuity_user_my_table.key, continuuity_user_my_table.value from continuuity_user_my_table " +
               "join continuuity_user_my_table_1 on (continuuity_user_my_table.key=continuuity_user_my_table_1.key)",
        true,
        Lists.newArrayList(new ColumnDesc("continuuity_user_my_table.key", "STRING", 1, null),
                           new ColumnDesc("continuuity_user_my_table.value",
                                          "struct<name:string,ints:array<int>>", 2, null)),
        Lists.newArrayList(
            new Result(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}")))
    );

    datasetFramework.deleteInstance("my_table_1");
  }

  @Test
  public void testCancel() throws Exception {
    Handle handle = exploreClient.execute("select key, value from continuuity_user_my_table");
    exploreClient.cancel(handle);
    Assert.assertEquals(
      Status.OpStatus.CANCELED,
      ExploreClientUtil.waitForCompletionStatus(exploreClient, handle, 200, TimeUnit.MILLISECONDS, 100).getStatus()
    );
    exploreClient.close(handle);
  }

  private static void runCommand(String command, boolean expectedHasResult,
                                 List<ColumnDesc> expectedColumnDescs, List<Result> expectedResults) throws Exception {
    Handle handle = exploreClient.execute(command);

    Status status = ExploreClientUtil.waitForCompletionStatus(exploreClient, handle, 200, TimeUnit.MILLISECONDS, 20);
    Assert.assertEquals(Status.OpStatus.FINISHED, status.getStatus());
    Assert.assertEquals(expectedHasResult, status.hasResults());

    Assert.assertEquals(expectedColumnDescs, exploreClient.getResultSchema(handle));
    Assert.assertEquals(expectedResults, trimColumnValues(exploreClient.nextResults(handle, 100)));

    exploreClient.close(handle);
  }

  private static List<Result> trimColumnValues(List<Result> results) {
    List<Result> newResults = Lists.newArrayList();
    for (Result result : results) {
      List<Object> newCols = Lists.newArrayList();
      for (Object obj : result.getColumns()) {
        if (obj instanceof String) {
          newCols.add(((String) obj).trim());
        } else {
          newCols.add(obj);
        }
      }
      newResults.add(new Result(newCols));
    }
    return newResults;
  }

  private static List<Module> createInMemoryModules(CConfiguration configuration, Configuration hConf) {
    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());
    configuration.setBoolean(Constants.Explore.CFG_EXPLORE_ENABLED, true);
    configuration.set(Constants.Explore.CFG_LOCAL_DATA_DIR,
             new File(System.getProperty("java.io.tmpdir"), "hive").getAbsolutePath());

    return ImmutableList.of(
      new ConfigModule(configuration, hConf),
      new IOModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DataSetServiceModules().getInMemoryModule(),
      new DataFabricModules().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new AuthModule(),
      new ExploreRuntimeModule().getInMemoryModules()
    );
  }
}
