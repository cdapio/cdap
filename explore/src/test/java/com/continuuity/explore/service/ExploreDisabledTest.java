package com.continuuity.explore.service;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetServiceModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.explore.client.DiscoveryExploreClient;
import com.continuuity.explore.client.ExploreClient;
import com.continuuity.explore.guice.ExploreRuntimeModule;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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
 * Test deployment behavior when explore module is disabled.
 */
public class ExploreDisabledTest {
  private static InMemoryTransactionManager transactionManager;
  private static DatasetFramework datasetFramework;
  private static DatasetService datasetService;

  @BeforeClass
  public static void start() throws Exception {
    Injector injector = Guice.createInjector(createInMemoryModules(CConfiguration.create(), new Configuration()));
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    transactionManager.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    ExploreClient exploreClient = injector.getInstance(DiscoveryExploreClient.class);
    Assert.assertFalse(exploreClient.isAvailable());

    datasetFramework = injector.getInstance(DatasetFramework.class);
  }

  @AfterClass
  public static void stop() throws Exception {
    datasetService.stopAndWait();
    transactionManager.stopAndWait();
  }

  @Test
  public void testDeployRecordScannable() throws Exception {
    // Try to deploy a dataset that is not record scannable, when explore is enabled.
    // This should be processed with no exception being thrown
    datasetFramework.addModule("module1", new KeyStructValueTableDefinition.KeyStructValueTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", "table1", DatasetProperties.EMPTY);

    Transaction tx1 = transactionManager.startShort(100);

    // Accessing dataset instance to perform data operations
    KeyStructValueTableDefinition.KeyStructValueTable table = datasetFramework.getDataset("table1", null);
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

    datasetFramework.deleteInstance("table1");
    datasetFramework.deleteModule("module1");
  }

  @Test
  public void testDeployNotRecordScannable() throws Exception {
    // Try to deploy a dataset that is not record scannable, when explore is enabled.
    // This should be processed with no exceptionbeing thrown
    datasetFramework.addModule("module2", new NotRecordScannableTableDefinition.NotRecordScannableTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("NotRecordScannableTableDef", "table2", DatasetProperties.EMPTY);

    Transaction tx1 = transactionManager.startShort(100);

    // Accessing dataset instance to perform data operations
    NotRecordScannableTableDefinition.KeyValueTable table = datasetFramework.getDataset("table2", null);
    Assert.assertNotNull(table);
    table.startTx(tx1);

    table.write("key1", "value1");
    table.write("key2", "value2");
    Assert.assertEquals("value1", new String(table.read("key1")));

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1, table.getTxChanges());
    transactionManager.commit(tx1);

    table.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    table.startTx(tx2);

    Assert.assertEquals("value1", new String(table.read("key1")));

    datasetFramework.deleteInstance("table2");
    datasetFramework.deleteModule("module2");
  }

  private static List<Module> createInMemoryModules(CConfiguration configuration, Configuration hConf) {
    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());
    configuration.setBoolean(Constants.Explore.CFG_EXPLORE_ENABLED, false);
    configuration.set(Constants.Explore.CFG_LOCAL_DATA_DIR,
                      new File(System.getProperty("java.io.tmpdir"), "hive").getAbsolutePath());

    return ImmutableList.of(
        new ConfigModule(configuration, hConf),
        new IOModule(),
        new DiscoveryRuntimeModule().getInMemoryModules(),
        new LocationRuntimeModule().getInMemoryModules(),
        new DataSetServiceModules().getInMemoryModule(),
        new DataFabricModules().getInMemoryModules(),
        new DataSetsModules().getInMemoryModule(),
        new MetricsClientRuntimeModule().getInMemoryModules(),
        new AuthModule(),
        new ExploreRuntimeModule().getInMemoryModules()
    );
  }
}
