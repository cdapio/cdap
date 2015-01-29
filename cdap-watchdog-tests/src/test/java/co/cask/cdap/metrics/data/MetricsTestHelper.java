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

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.data.runtime.DataFabricDistributedModule;
import co.cask.cdap.data.runtime.DataFabricLevelDBModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.leveldb.LevelDBMetricsTableModule;
import co.cask.cdap.metrics.MetricsConstants;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

import java.io.File;

/**
 * Helper class for constructing different test environments for running metrics tests.
 */
public class MetricsTestHelper {

  public static MetricsTableFactory createLocalMetricsTableFactory(File dataDir) throws DatasetManagementException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, dataDir.getAbsolutePath());
    cConf.set(MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME, "300");

    Injector injector = Guice.createInjector(new ConfigModule(cConf),
                                             new LocationRuntimeModule().getStandaloneModules(),
                                             new DataFabricLevelDBModule(),
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

    DatasetFramework dsFramework =
      new InMemoryDatasetFramework(injector.getInstance(DatasetDefinitionRegistryFactory.class), cConf);
    dsFramework.addModule("metricsTable-leveldb", new LevelDBMetricsTableModule());
    return new DefaultMetricsTableFactory(cConf, dsFramework);
  }

  public static MetricsTableFactory createHBaseMetricsTableFactory(Configuration hConf)
                                                                   throws DatasetManagementException {
    CConfiguration cConf = CConfiguration.create();
    String zkConnectStr = hConf.get(HConstants.ZOOKEEPER_QUORUM) + ":" + hConf.get(HConstants.ZOOKEEPER_CLIENT_PORT);
    cConf.set(Constants.Zookeeper.QUORUM, zkConnectStr);
    cConf.set(MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME, "300");
    cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));

    Injector injector = Guice.createInjector(new ConfigModule(cConf, hConf),
                                             new DiscoveryRuntimeModule().getDistributedModules(),
                                             new ZKClientModule(),
                                             new LocationRuntimeModule().getDistributedModules(),
                                             new DataFabricDistributedModule(),
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

    DatasetFramework dsFramework =
      new InMemoryDatasetFramework(injector.getInstance(DatasetDefinitionRegistryFactory.class), cConf);
    dsFramework.addModule("metrics-hbase", new HBaseMetricsTableModule());
    return new DefaultMetricsTableFactory(cConf, dsFramework);
  }

  private MetricsTestHelper() {
  }
}
