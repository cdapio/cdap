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

package co.cask.cdap.data2.dataset2.lib.table.inmemory;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTableTest;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryMetricsTableModule;
import co.cask.cdap.proto.Id;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.junit.BeforeClass;

/**
 * test in-memory metrics tables.
 */
public class InMemoryMetricsTableTest extends MetricsTableTest {

  private static final Id.DatasetModule metricsInMemoryModule =
    Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE_ID, "metrics-inmemory");

  private static DatasetFramework dsFramework;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = Guice.createInjector(
      new ConfigModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
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

    dsFramework = new InMemoryDatasetFramework(injector.getInstance(DatasetDefinitionRegistryFactory.class),
                                               injector.getInstance(CConfiguration.class));
    dsFramework.addModule(metricsInMemoryModule, new InMemoryMetricsTableModule());
  }

  @Override
  protected MetricsTable getTable(String name) throws Exception {
    Id.DatasetInstance metricsDatasetInstanceId = Id.DatasetInstance.from(Constants.SYSTEM_NAMESPACE_ID, name);
    return DatasetsUtil.getOrCreateDataset(dsFramework, metricsDatasetInstanceId, MetricsTable.class.getName(),
                                           DatasetProperties.EMPTY, null, null);
  }
}
