/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.data2.transaction.stream.hbase;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data.runtime.DataFabricDistributedModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.service.InMemoryStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.security.UGIProvider;
import co.cask.cdap.data2.security.UnsupportedUGIProvider;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStore;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateTestBase;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.service.NoOpNotificationFeedManager;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.SlowTests;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 *
 */
@Category(SlowTests.class)
public class HBaseConsumerStateTest extends StreamConsumerStateTestBase {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  @ClassRule
  public static final HBaseTestBase TEST_HBASE = new HBaseTestFactory().get();

  private static StreamAdmin streamAdmin;
  private static StreamConsumerStateStoreFactory stateStoreFactory;
  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClientService;
  private static HBaseTableUtil tableUtil;

  @BeforeClass
  public static void init() throws Exception {
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    Configuration hConf = TEST_HBASE.getConfiguration();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new NonCustomLocationUnitTestModule().getModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new TransactionMetricsModule(),
      new DataFabricDistributedModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(NamespaceQueryAdmin.class).to(SimpleNamespaceQueryAdmin.class);
        }
      },
      new DataSetsModules().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new ExploreClientModule(),
      new ViewAdminModules().getInMemoryModules(),
      Modules.override(new StreamAdminModules().getDistributedModules())
        .with(new AbstractModule() {
          @Override
          protected void configure() {
            bind(StreamMetaStore.class).to(InMemoryStreamMetaStore.class);
            bind(NotificationFeedManager.class).to(NoOpNotificationFeedManager.class);
            bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
          }
        })
    );
    zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    streamAdmin = injector.getInstance(StreamAdmin.class);
    stateStoreFactory = injector.getInstance(StreamConsumerStateStoreFactory.class);

    tableUtil = injector.getInstance(HBaseTableUtil.class);
    tableUtil.createNamespaceIfNotExists(TEST_HBASE.getHBaseAdmin(),
                                         tableUtil.getHBaseNamespace(TEST_NAMESPACE.toEntityId()));
    tableUtil.createNamespaceIfNotExists(TEST_HBASE.getHBaseAdmin(),
                                         tableUtil.getHBaseNamespace(OTHER_NAMESPACE.toEntityId()));

    setupNamespaces(injector.getInstance(NamespacedLocationFactory.class));
  }

  @AfterClass
  public static void finish() throws Exception {
    try {
      deleteNamespace(OTHER_NAMESPACE);
    } finally {
      try {
        deleteNamespace(TEST_NAMESPACE);
      } finally {
        try {
          zkClientService.stopAndWait();
        } finally {
          zkServer.stopAndWait();
        }
      }
    }
  }

  private static void deleteNamespace(Id.Namespace namespace) throws IOException {
    tableUtil.deleteAllInNamespace(TEST_HBASE.getHBaseAdmin(), tableUtil.getHBaseNamespace(namespace.toEntityId()));
    tableUtil.deleteNamespaceIfExists(TEST_HBASE.getHBaseAdmin(), tableUtil.getHBaseNamespace(namespace.toEntityId()));
  }

  @Override
  protected StreamConsumerStateStore createStateStore(StreamConfig streamConfig) throws Exception {
    return stateStoreFactory.create(streamConfig);
  }

  @Override
  protected StreamAdmin getStreamAdmin() {
    return streamAdmin;
  }
}
