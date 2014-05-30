package com.continuuity.hive.data2;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetServiceModules;
import com.continuuity.data2.datafabric.dataset.service.DatasetManagerService;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.hive.HiveClientTestUtils;
import com.continuuity.hive.client.HiveClient;
import com.continuuity.hive.client.guice.HiveClientModule;
import com.continuuity.hive.guice.HiveRuntimeModule;
import com.continuuity.hive.inmemory.InMemoryHiveMetastore;
import com.continuuity.hive.server.HiveServer;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class HiveServerIntegrationTest {
  private static InMemoryTransactionManager transactionManager;
  private static HiveServer hiveServer;
  private static InMemoryHiveMetastore inMemoryHiveMetastore;
  private static DatasetManager datasetManager;
  private static DatasetManagerService datasetManagerService;
  private static HiveClient hiveClient;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = Guice.createInjector(createInMemoryModules(CConfiguration.create(), new Configuration()));
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    inMemoryHiveMetastore = injector.getInstance(InMemoryHiveMetastore.class);
    hiveServer = injector.getInstance(HiveServer.class);
    hiveClient = injector.getInstance(HiveClient.class);

    transactionManager.startAndWait();
    inMemoryHiveMetastore.startAndWait();
    hiveServer.startAndWait();

    datasetManagerService = injector.getInstance(DatasetManagerService.class);
    datasetManagerService.startAndWait();

    datasetManager = injector.getInstance(DatasetManager.class);
    String moduleName = "inMemory";
    datasetManager.register(moduleName, InMemoryTableModule.class);
    datasetManager.register("keyValue", KeyValueTableDefinition.KeyValueTableModule.class);

    // Performing admin operations to create dataset instance
    datasetManager.addInstance("keyValueTable", "my_table", DatasetInstanceProperties.EMPTY);
    DatasetAdmin admin = datasetManager.getAdmin("my_table", null);
    Assert.assertNotNull(admin);
    admin.create();

    Transaction tx1 = transactionManager.startShort(100);

    // Accessing dataset instance to perform data operations
    KeyValueTableDefinition.KeyValueTable table = datasetManager.getDataset("my_table", null);
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
    datasetManagerService.startAndWait();
  }


  @Test
  public void testTable() throws Exception {
    KeyValueTableDefinition.KeyValueTable table = datasetManager.getDataset("my_table", null);
    Assert.assertNotNull(table);
    Transaction tx = transactionManager.startShort(100);
    table.startTx(tx);
    Assert.assertEquals("first", table.get("1"));
    transactionManager.abort(tx);
  }

  @Test
  public void testHiveIntegration() throws Exception {
    hiveClient.sendCommand("drop table if exists kv_table", null, null);
    hiveClient.sendCommand("create external table kv_table (customer STRING, quantity int) " +
                           "stored by 'com.continuuity.hive.datasets.DatasetStorageHandler' " +
                           "with serdeproperties (\"reactor.dataset.name\"=\"my_table\");", null, null);
    HiveClientTestUtils.assertCmdFindPattern(hiveClient, "describe kv_table", "key.*string.*value.*string");
    HiveClientTestUtils.assertCmdFindPattern(hiveClient, "select key, value from kv_table",
        "1.*first.*2.*two");
    // todo fix select *
    //HiveClientTestUtils.assertCmdFindPattern(hiveClient, "select * from kv_table",
    //    "1.*first.*2.*two");
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
