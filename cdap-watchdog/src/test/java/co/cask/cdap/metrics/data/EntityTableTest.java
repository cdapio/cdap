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
package co.cask.cdap.metrics.data;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data.runtime.DataFabricDistributedModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseMetricsTableModule;
import co.cask.cdap.test.SlowTests;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 *
 */
@Category(SlowTests.class)
public class EntityTableTest {

  private static DatasetFramework dsFramework;
  private static HBaseTestBase testHBase;

  protected MetricsTable getTable(String name) throws Exception {
    return DatasetsUtil.getOrCreateDataset(dsFramework, name, MetricsTable.class.getName(),
                                           DatasetProperties.EMPTY, null, null);
  }

  @Test
  public void testGetId() throws Exception {
    EntityTable entityTable = new EntityTable(getTable("testGetId"));

    // Make sure it is created sequentially
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // It should get the same value (from cache)
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // Construct another entityTable, it should load from storage.
    entityTable = new EntityTable(getTable("testGetId"));
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // ID for different type should have ID starts from 1 again.
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("flow", "flow" + i));
    }
  }

  @Test
  public void testRecycleAfterMaxId() throws Exception {
    EntityTable entityTable = new EntityTable(getTable("testRecycleId"), 101);

    // Generate 500 entries, the (101-200) will replace the (1-100) values and so on as we
    // only have 100 entries as maxId.
    for (long i = 1; i <= 500; i++) {
      entityTable.getId("app", "app" + i);
    }

    // we call getName for the 100 entries, it will be the latest entries 401-500
    for (long i = 1; i <= 100; i++) {
      Assert.assertEquals("app" + String.valueOf(400 + i), entityTable.getName(i, "app"));
    }
  }

  @Test
  public void testGetName() throws Exception {
    EntityTable entityTable = new EntityTable(getTable("testGetName"));

    // Create some entities.
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals((long) i, entityTable.getId("app", "app" + i));
    }

    // Reverse lookup
    for (int i = 1; i <= 10; i++) {
      Assert.assertEquals("app" + i, entityTable.getName(i, "app"));
    }
  }


  @BeforeClass
  public static void init() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, testHBase.getZkConnectionString());
    cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, testHBase.getConfiguration()),
                                             new DiscoveryRuntimeModule().getDistributedModules(),
                                             new ZKClientModule(),
                                             new DataFabricDistributedModule(),
                                             new LocationRuntimeModule().getDistributedModules(),
                                             new TransactionMetricsModule(),
                                             new AbstractModule() {
                                               @Override
                                               protected void configure() {
                                                 install(new FactoryModuleBuilder()
                                                           .implement(DatasetDefinitionRegistry.class,
                                                                      DefaultDatasetDefinitionRegistry.class)
                                                           .build(DatasetDefinitionRegistryFactory.class));
                                               }
                                             });

    dsFramework = new InMemoryDatasetFramework(injector.getInstance(DatasetDefinitionRegistryFactory.class));
    dsFramework.addModule("metrics-hbase", new HBaseMetricsTableModule());
  }

  @AfterClass
  public static void finish() throws Exception {
    testHBase.stopHBase();
  }

}
