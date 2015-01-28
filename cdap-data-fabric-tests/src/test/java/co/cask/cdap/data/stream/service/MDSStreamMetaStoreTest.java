/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Test the {@link MDSStreamMetaStore}.
 */
public class MDSStreamMetaStoreTest extends StreamMetaStoreTestBase {

  private static StreamMetaStore streamMetaStore;
  private static DatasetService datasetService;
  private static TransactionManager transactionManager;
  private static Store store;

  @BeforeClass
  public static void init() throws Exception {
    Injector injector = Guice.createInjector(
      new ConfigModule(CConfiguration.create(), new Configuration()),
      new DataSetServiceModules().getInMemoryModule(),
      new DataSetsModules().getLocalModule(),
      new DataFabricModules().getInMemoryModules(),
      new TransactionMetricsModule(),
      new ExploreClientModule(),
      new AuthModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(StreamMetaStore.class).to(MDSStreamMetaStore.class).in(Scopes.SINGLETON);
        }
      }
    );

    streamMetaStore = injector.getInstance(StreamMetaStore.class);
    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    store = new DefaultStore(injector.getInstance(CConfiguration.class), injector.getInstance(LocationFactory.class),
                             injector.getInstance(TransactionSystemClient.class),
                             injector.getInstance(DatasetFramework.class));
  }

  @AfterClass
  public static void destroy() throws Exception {
    datasetService.stopAndWait();
    transactionManager.stopAndWait();
  }

  @Override
  protected StreamMetaStore getStreamMetaStore() {
    return streamMetaStore;
  }

  @Override
  protected void createNamespace(String namespaceId) {
    store.createNamespace(new NamespaceMeta.Builder().setId(namespaceId).build());
  }


}
