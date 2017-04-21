/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.queue;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data.runtime.DataFabricLocalModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.service.InMemoryStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.data2.transaction.queue.inmemory.InMemoryQueueProducer;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.service.NoOpNotificationFeedManager;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.DefaultOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Tests that injection for local mode uses in-memory for queues.
 */
public class LocalQueueTest extends QueueTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  static CConfiguration conf;

  @BeforeClass
  public static void init() throws Exception {
    conf = CConfiguration.create();
    conf.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, false);
    conf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    Injector injector = Guice.createInjector(
      new ConfigModule(conf),
      new NonCustomLocationUnitTestModule().getModule(),
      new DiscoveryRuntimeModule().getStandaloneModules(),
      new TransactionMetricsModule(),
      new DiscoveryRuntimeModule().getStandaloneModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new NamespaceClientRuntimeModule().getStandaloneModules(),
      new AuthenticationContextModules().getMasterModule(),
      new DataSetsModules().getStandaloneModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
        }
      },
      new DataFabricLocalModule());
    // transaction manager is a "service" and must be started
    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();
    txSystemClient = injector.getInstance(TransactionSystemClient.class);
    queueClientFactory = injector.getInstance(QueueClientFactory.class);
    queueAdmin = injector.getInstance(QueueAdmin.class);
    executorFactory = injector.getInstance(TransactionExecutorFactory.class);
    LevelDBTableService.getInstance().clearTables();
  }

  @Test
  public void testInjection() throws IOException {
    Injector injector = Guice.createInjector(
      new ConfigModule(conf),
      new NonCustomLocationUnitTestModule().getModule(),
      new DiscoveryRuntimeModule().getStandaloneModules(),
      new TransactionMetricsModule(),
      new DataFabricModules().getStandaloneModules(),
      new DataSetsModules().getStandaloneModules(),
      new ExploreClientModule(),
      new ViewAdminModules().getStandaloneModules(),
      new AuthorizationEnforcementModule().getStandaloneModules(),
      new AuthenticationContextModules().getMasterModule(),
      new NamespaceClientRuntimeModule().getStandaloneModules(),
      new AuthorizationTestModule(),
      Modules.override(new StreamAdminModules().getStandaloneModules())
        .with(new AbstractModule() {
          @Override
          protected void configure() {
            bind(StreamMetaStore.class).to(InMemoryStreamMetaStore.class);
            bind(NotificationFeedManager.class).to(NoOpNotificationFeedManager.class);
            bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
            bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
          }
        }));
    QueueClientFactory factory = injector.getInstance(QueueClientFactory.class);
    QueueProducer producer = factory.createProducer(QueueName.fromFlowlet(
      NamespaceId.DEFAULT.getNamespace(), "app", "my", "flowlet", "output"));
    Assert.assertTrue(producer instanceof InMemoryQueueProducer);
  }
}
