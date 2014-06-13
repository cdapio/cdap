package com.continuuity.hive.data2;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
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
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.hive.HiveClientTestUtils;
import com.continuuity.hive.client.HiveClient;
import com.continuuity.hive.client.guice.HiveClientModule;
import com.continuuity.hive.guice.HiveRuntimeModule;
import com.continuuity.hive.metastore.HiveMetastore;
import com.continuuity.hive.server.HiveServer;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 *
 */
public class HiveServerIntegrationTest {
  private static InMemoryTransactionManager transactionManager;
  private static HiveServer hiveServer;
  private static HiveMetastore inMemoryHiveMetastore;
  private static DatasetFramework datasetFramework;
  private static DatasetService datasetService;
  private static HiveClient hiveClient;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Hive.CFG_LOCAL_DATA_DIR,
              new File(System.getProperty("java.io.tmpdir"), "hive").getAbsolutePath());
    cConf.setBoolean(Constants.Hive.EXPLORE_ENABLED, true);
    Injector injector = Guice.createInjector(createInMemoryModules(cConf, new Configuration()));
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    inMemoryHiveMetastore = injector.getInstance(HiveMetastore.class);
    hiveServer = injector.getInstance(HiveServer.class);
    hiveClient = injector.getInstance(HiveClient.class);

    transactionManager.startAndWait();
    inMemoryHiveMetastore.startAndWait();
    hiveServer.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    datasetFramework = injector.getInstance(DatasetFramework.class);
    String moduleName = "inMemory";
    datasetFramework.addModule(moduleName, new InMemoryOrderedTableModule());
    datasetFramework.addModule("keyValue", new KeyValueTableDefinition.KeyValueTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyValueTable", "my_table", DatasetProperties.EMPTY);
    DatasetAdmin admin = datasetFramework.getAdmin("my_table", null);
    Assert.assertNotNull(admin);
    admin.create();

    Transaction tx1 = transactionManager.startShort(100);

    // Accessing dataset instance to perform data operations
    KeyValueTableDefinition.KeyValueTable table = datasetFramework.getDataset("my_table", null);
    Assert.assertNotNull(table);
    table.startTx(tx1);
    table.put("1", "first");
    table.put("2", "two");
    Assert.assertEquals("first", table.get("1"));

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1, table.getTxChanges());
    transactionManager.commit(tx1);

    table.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    table.startTx(tx2);

    Assert.assertEquals("first", table.get("1"));
  }

  @AfterClass
  public static void stop() throws Exception {
    hiveServer.stopAndWait();
    inMemoryHiveMetastore.stopAndWait();
    transactionManager.stopAndWait();
    datasetService.startAndWait();
  }


  @Test
  public void testTable() throws Exception {
    KeyValueTableDefinition.KeyValueTable table = datasetFramework.getDataset("my_table", null);
    Assert.assertNotNull(table);
    Transaction tx = transactionManager.startShort(100);
    table.startTx(tx);
    Assert.assertEquals("first", table.get("1"));
    transactionManager.abort(tx);
  }

  @Test
  public void testHiveIntegration() throws Exception {
    hiveClient.sendCommand("drop table if exists kv_table", null, null);
    hiveClient.sendCommand("create external table kv_table (key STRING, value STRING) " +
                           "stored by 'com.continuuity.hive.datasets.DatasetStorageHandler' " +
                           "with serdeproperties (\"reactor.dataset.name\"=\"my_table\");", null, null);
    HiveClientTestUtils.assertCmdFindPattern(hiveClient, "describe kv_table", "key.*string.*value.*string");
    HiveClientTestUtils.assertCmdFindPattern(hiveClient, "select key, value from kv_table",
                                             "1.*first.*2.*two");
    // todo fix select *
    //HiveClientTestUtils.assertCmdFindPattern(hiveClient, "select * from kv_table",
    //    "1.*first.*2.*two");
  }

  @Test
  public void testHiveDatasetsJoin() throws Exception {
    // Performing admin operations to create another dataset instance
    datasetFramework.addInstance("keyValueTable", "my_table_2", DatasetProperties.EMPTY);
    DatasetAdmin admin = datasetFramework.getAdmin("my_table_2", null);
    admin.create();

    Transaction tx1 = transactionManager.startShort(100);

    // Accessing dataset instance to perform data operations
    KeyValueTableDefinition.KeyValueTable table = datasetFramework.getDataset("my_table_2", null);
    Assert.assertNotNull(table);
    table.startTx(tx1);
    table.put("1", "first");
    table.put("2", "two");
    table.put("3", "three");

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1, table.getTxChanges());
    transactionManager.commit(tx1);

    table.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    table.startTx(tx2);

    Assert.assertEquals("first", table.get("1"));

    // Create hive table for first key value table
    hiveClient.sendCommand("drop table if exists kv_table", null, null);
    hiveClient.sendCommand("create external table kv_table (key STRING, value STRING) " +
                           "stored by 'com.continuuity.hive.datasets.DatasetStorageHandler' " +
                           "with serdeproperties (\"reactor.dataset.name\"=\"my_table\");", null, null);

    // Create hive table for second key value table
    hiveClient.sendCommand("drop table if exists kv_table_2", null, null);
    hiveClient.sendCommand("create external table kv_table_2 (key STRING, value STRING) " +
                           "stored by 'com.continuuity.hive.datasets.DatasetStorageHandler' " +
                           "with serdeproperties (\"reactor.dataset.name\"=\"my_table_2\");", null, null);

    // Assert join operation
    HiveClientTestUtils.assertCmdFindPattern(
        hiveClient,
        "select kv_table.value from kv_table join kv_table_2 on (kv_table.key=kv_table_2.key);",
        "first.*two"
    );
  }

  private static List<Module> createInMemoryModules(CConfiguration configuration, Configuration hConf) {
    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());

    return ImmutableList.of(
      new ConfigModule(configuration, hConf),
      new IOModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DataSetServiceModules().getInMemoryModule(),
      new DataFabricModules().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new AuthModule(),
      new HiveRuntimeModule().getInMemoryModules(),
      new HiveClientModule()
    );
  }

}
