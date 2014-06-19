package com.continuuity.com.continuuity.explore.jdbc;

import com.continuuity.api.dataset.DatasetAdmin;
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
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryOrderedTableModule;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.explore.executor.ExploreExecutorService;
import com.continuuity.explore.guice.ExploreRuntimeModule;
import com.continuuity.explore.jdbc.ExploreJDBCUtils;
import com.continuuity.explore.service.KeyStructValueTableDefinition;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;

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

import java.io.File;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 *
 */
public class ExploreJDBCIntegrationTest {

  private static InMemoryTransactionManager transactionManager;
  private static DatasetFramework datasetFramework;
  private static DatasetService datasetService;
  private static ExploreExecutorService exploreExecutorService;
  private static String serviceUrl;

  @BeforeClass
  public static void before() throws Exception {
    Injector injector = Guice.createInjector(createInMemoryModules(CConfiguration.create(), new Configuration()));
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    transactionManager.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    exploreExecutorService.startAndWait();

    datasetFramework = injector.getInstance(DatasetFramework.class);
    String moduleName = "inMemory";
    datasetFramework.addModule(moduleName, new InMemoryOrderedTableModule());
    datasetFramework.addModule("keyValue", new KeyStructValueTableDefinition.KeyStructValueTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyValueTable", "my_table", DatasetProperties.EMPTY);
    DatasetAdmin admin = datasetFramework.getAdmin("my_table", null);
    Assert.assertNotNull(admin);
    admin.create();

    Transaction tx1 = transactionManager.startShort(100);

    // Accessing dataset instance to perform data operations
    KeyStructValueTableDefinition.KeyStructValueTable table = datasetFramework.getDataset("my_table", null);
    Assert.assertNotNull(table);
    table.startTx(tx1);

    KeyStructValueTableDefinition.KeyValue.Value value1 =
        new KeyStructValueTableDefinition.KeyValue.Value("first", Lists.newArrayList(1, 2, 3, 4, 5));
    KeyStructValueTableDefinition.KeyValue.Value value2 =
        new KeyStructValueTableDefinition.KeyValue.Value("two", Lists.newArrayList(10, 11, 12, 13, 14));
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

    // Register explore jdbc driver
    Class.forName("com.continuuity.explore.jdbc.ExploreDriver");

    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable = new RandomEndpointStrategy(
        discoveryServiceClient.discover(Constants.Service.EXPLORE_HTTP_USER_SERVICE)).pick();
    InetSocketAddress addr = discoverable.getSocketAddress();
    serviceUrl = String.format("%s%s:%d", ExploreJDBCUtils.URL_PREFIX, addr.getHostName(), addr.getPort());
  }

  @Test
  public void exploreDriverTest() throws Exception {
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
    Assert.assertEquals("1", rowSet.getString(1));  // TODO change to getInt
    Assert.assertEquals("{\"name\":\"first\",\"ints\":[1,2,3,4,5]}", rowSet.getString(2)); // TODO change to toObject
    Assert.assertTrue(rowSet.next());
    Assert.assertEquals("2", rowSet.getString(1));  // TODO change to getInt
    Assert.assertEquals("{\"name\":\"two\",\"ints\":[10,11,12,13,14]}", rowSet.getString(2)); // TODO change to toObject
    stmt.close();

    // TODO method not yet supported, change that
    // connection.close();
  }

  @AfterClass
  public static void after() throws Exception {
    datasetFramework.deleteInstance("my_table");

    exploreExecutorService.stopAndWait();
    transactionManager.stopAndWait();
    datasetService.startAndWait();
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
