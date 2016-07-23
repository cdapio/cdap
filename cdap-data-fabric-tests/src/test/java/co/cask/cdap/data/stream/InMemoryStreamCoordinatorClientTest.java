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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.service.InMemoryStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.security.UGIProvider;
import co.cask.cdap.data2.security.UnsupportedUGIProvider;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 *
 */
public class InMemoryStreamCoordinatorClientTest extends StreamCoordinatorTestBase {

  private static StreamAdmin streamAdmin;
  private static StreamCoordinatorClient coordinatorClient;

  @BeforeClass
  public static void init() throws Exception {
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new DataSetsModules().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new NonCustomLocationUnitTestModule().getModule(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new TransactionMetricsModule(),
      new NotificationFeedServiceRuntimeModule().getInMemoryModules(),
      new ExploreClientModule(),
      new ViewAdminModules().getInMemoryModules(),
      Modules.override(new StreamAdminModules().getInMemoryModules())
        .with(new AbstractModule() {
                @Override
                protected void configure() {
                  bind(StreamMetaStore.class).to(InMemoryStreamMetaStore.class);
                  bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
                }
              })
    );

    setupNamespaces(injector.getInstance(NamespacedLocationFactory.class));
    streamAdmin = injector.getInstance(StreamAdmin.class);
    coordinatorClient = injector.getInstance(StreamCoordinatorClient.class);
    coordinatorClient.startAndWait();
  }

  @AfterClass
  public static void finish() {
    coordinatorClient.stopAndWait();
  }

  @Override
  protected StreamCoordinatorClient getStreamCoordinator() {
    return coordinatorClient;
  }

  @Override
  protected StreamAdmin getStreamAdmin() {
    return streamAdmin;
  }
}
