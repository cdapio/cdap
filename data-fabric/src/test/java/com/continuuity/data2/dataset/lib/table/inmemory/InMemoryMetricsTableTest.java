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

import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.dataset.lib.table.MetricsTableTest;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

/**
 * test in-memory metrics tables.
 */
public class InMemoryMetricsTableTest extends MetricsTableTest {

  @BeforeClass
  public static void setup() {
    Injector injector = Guice.createInjector(new LocationRuntimeModule().getInMemoryModules(),
                                             new DiscoveryRuntimeModule().getInMemoryModules(),
                                             new DataFabricModules().getInMemoryModules(),
                                             new DataSetsModules().getInMemoryModule(),
                                             new TransactionMetricsModule());
    dsAccessor = injector.getInstance(DataSetAccessor.class);
  }
}
