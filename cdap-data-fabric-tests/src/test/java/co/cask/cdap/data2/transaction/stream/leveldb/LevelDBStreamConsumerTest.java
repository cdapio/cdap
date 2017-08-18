/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
package co.cask.cdap.data2.transaction.stream.leveldb;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import co.cask.cdap.data.runtime.DataFabricLevelDBModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.service.InMemoryStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamConsumerTestBase;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.service.NoOpNotificationFeedManager;
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
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class LevelDBStreamConsumerTest extends StreamConsumerTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static StreamConsumerFactory consumerFactory;
  private static StreamAdmin streamAdmin;
  private static TransactionSystemClient txClient;
  private static TransactionManager txManager;
  private static QueueClientFactory queueClientFactory;
  private static StreamFileWriterFactory fileWriterFactory;
  private static StreamCoordinatorClient streamCoordinatorClient;

  @BeforeClass
  public static void init() throws Exception {
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new NonCustomLocationUnitTestModule().getModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new DataSetsModules().getInMemoryModules(),
      new DataFabricLevelDBModule(),
      new TransactionMetricsModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new ExploreClientModule(),
      new ViewAdminModules().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getNoOpModule(),
      Modules.override(new StreamAdminModules().getStandaloneModules())
        .with(new AbstractModule() {
          @Override
          protected void configure() {
            bind(StreamMetaStore.class).to(InMemoryStreamMetaStore.class);
            bind(NotificationFeedManager.class).to(NoOpNotificationFeedManager.class);
            bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
            bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
            bind(NamespaceQueryAdmin.class).to(SimpleNamespaceQueryAdmin.class);
          }
        })
    );

    consumerFactory = injector.getInstance(StreamConsumerFactory.class);
    streamAdmin = injector.getInstance(StreamAdmin.class);
    txClient = injector.getInstance(TransactionSystemClient.class);
    txManager = injector.getInstance(TransactionManager.class);
    queueClientFactory = injector.getInstance(QueueClientFactory.class);
    fileWriterFactory = injector.getInstance(StreamFileWriterFactory.class);
    streamCoordinatorClient = injector.getInstance(StreamCoordinatorClient.class);
    streamCoordinatorClient.startAndWait();

    txManager.startAndWait();
    setupNamespaces(injector.getInstance(NamespacedLocationFactory.class));
  }

  @AfterClass
  public static void finish() throws Exception {
    streamCoordinatorClient.stopAndWait();
    txManager.stopAndWait();
  }

  @Override
  protected QueueClientFactory getQueueClientFactory() {
    return queueClientFactory;
  }

  @Override
  protected StreamConsumerFactory getConsumerFactory() {
    return consumerFactory;
  }

  @Override
  protected StreamAdmin getStreamAdmin() {
    return streamAdmin;
  }

  @Override
  protected TransactionSystemClient getTransactionClient() {
    return txClient;
  }

  @Override
  protected StreamFileWriterFactory getFileWriterFactory() {
    return fileWriterFactory;
  }
}
