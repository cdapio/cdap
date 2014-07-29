/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.dataset.lib.table.inmemory;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.TransactionMetricsModule;
import com.continuuity.data2.datafabric.dataset.DatasetsUtil;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.continuuity.data2.dataset.lib.table.MetricsTableTest;
import com.continuuity.data2.dataset2.DatasetDefinitionRegistryFactory;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DefaultDatasetDefinitionRegistry;
import com.continuuity.data2.dataset2.InMemoryDatasetFramework;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryMetricsTableModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.junit.BeforeClass;

/**
 * test in-memory metrics tables.
 */
public class InMemoryMetricsTableTest extends MetricsTableTest {
  private static DatasetFramework dsFramework;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = Guice.createInjector(new LocationRuntimeModule().getInMemoryModules(),
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

    dsFramework = new InMemoryDatasetFramework(injector.getInstance(DatasetDefinitionRegistryFactory.class));
    dsFramework.addModule("metrics-inmemory", new InMemoryMetricsTableModule());
  }

  @Override
  protected MetricsTable getTable(String name) throws Exception {
    return DatasetsUtil.getOrCreateDataset(dsFramework, name, MetricsTable.class.getName(),
                                           DatasetProperties.EMPTY, null);
  }
}
