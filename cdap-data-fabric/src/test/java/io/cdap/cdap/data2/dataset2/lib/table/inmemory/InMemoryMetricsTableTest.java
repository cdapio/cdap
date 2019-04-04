/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.data2.dataset2.lib.table.inmemory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data.runtime.TransactionMetricsModule;
import io.cdap.cdap.data2.datafabric.dataset.DatasetsUtil;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.InMemoryDatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.table.MetricsTable;
import io.cdap.cdap.data2.dataset2.lib.table.MetricsTableTest;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.BeforeClass;

/**
 * test in-memory metrics tables.
 */
public class InMemoryMetricsTableTest extends MetricsTableTest {

  private static DatasetFramework dsFramework;

  @BeforeClass
  public static void setup() {
    Injector injector = Guice.createInjector(
      new ConfigModule(),
      new NonCustomLocationUnitTestModule(),
      new InMemoryDiscoveryModule(),
      new DataFabricModules().getInMemoryModules(),
      new TransactionMetricsModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new PrivateModule() {
        @Override
        protected void configure() {
          bind(DatasetDefinitionRegistryFactory.class)
            .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);
          bind(DatasetFramework.class).to(InMemoryDatasetFramework.class);
          expose(DatasetFramework.class);
        }
      });

    dsFramework = injector.getInstance(DatasetFramework.class);
  }

  @Override
  protected MetricsTable getTable(String name) throws Exception {
    DatasetId metricsDatasetInstanceId = NamespaceId.SYSTEM.dataset(name);
    return DatasetsUtil.getOrCreateDataset(dsFramework, metricsDatasetInstanceId, MetricsTable.class.getName(),
                                           DatasetProperties.EMPTY, null);
  }
}
