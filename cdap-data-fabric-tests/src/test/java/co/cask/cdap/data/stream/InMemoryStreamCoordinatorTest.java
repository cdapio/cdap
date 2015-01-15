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
package co.cask.cdap.data.stream;

import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.service.InMemoryStreamCoordinator;
import co.cask.cdap.data.stream.service.MDSStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamCoordinator;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.tephra.TransactionManager;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 *
 */
public class InMemoryStreamCoordinatorTest extends StreamCoordinatorTestBase {

  private static Injector injector;

  private static TransactionManager transactionManager;
  private static DatasetService datasetService;

  @BeforeClass
  public static void init() {
    injector = Guice.createInjector(
      new ConfigModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetServiceModules().getInMemoryModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new DataSetsModules().getLocalModule(),
      new TransactionMetricsModule(),
      new AuthModule(),
      new ExploreClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(StreamCoordinator.class).to(InMemoryStreamCoordinator.class).in(Scopes.SINGLETON);
          bind(StreamMetaStore.class).to(MDSStreamMetaStore.class).in(Scopes.SINGLETON);
        }
      }
    );

    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
  }

  @AfterClass
  public static void finish() {
    datasetService.stopAndWait();
    transactionManager.stopAndWait();
  }

  @Override
  protected StreamCoordinator createStreamCoordinator() {
    return injector.getInstance(StreamCoordinator.class);
  }
}
