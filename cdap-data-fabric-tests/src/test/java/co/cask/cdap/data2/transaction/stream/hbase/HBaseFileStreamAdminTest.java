/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.security.UnsupportedUGIProvider;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data.runtime.DataFabricDistributedModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.service.InMemoryStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.audit.InMemoryAuditPublisher;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdminTest;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.service.NoOpNotificationFeedManager;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationEnforcementService;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.test.SlowTests;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.tephra.inmemory.TxInMemory;
import org.apache.tephra.persist.NoOpTransactionStateStorage;
import org.apache.tephra.persist.TransactionStateStorage;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
@Category(SlowTests.class)
public class HBaseFileStreamAdminTest extends StreamAdminTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();
  @ClassRule
  public static HBaseTestBase testHBase = new HBaseTestFactory().get();

  private static StreamAdmin streamAdmin;
  private static TransactionManager txManager;
  private static StreamFileWriterFactory fileWriterFactory;
  private static StreamCoordinatorClient streamCoordinatorClient;
  private static InMemoryAuditPublisher inMemoryAuditPublisher;
  private static AuthorizationEnforcementService authorizationEnforcementService;
  private static Authorizer authorizer;

  @BeforeClass
  public static void init() throws Exception {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    Configuration hConf = testHBase.getConfiguration();
    addCConfProperties(cConf);
    cConf.setInt(Constants.Stream.CONTAINER_INSTANCES, 1);
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new NonCustomLocationUnitTestModule().getModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new TransactionMetricsModule(),
      new DataSetsModules().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new ExploreClientModule(),
      new ViewAdminModules().getInMemoryModules(),
      new AuditModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getNoOpModule(),
      Modules.override(new DataFabricDistributedModule(), new StreamAdminModules().getDistributedModules())
        .with(new AbstractModule() {
          @Override
          protected void configure() {
            bind(TransactionStateStorage.class).to(NoOpTransactionStateStorage.class);
            bind(TransactionSystemClient.class).to(InMemoryTxSystemClient.class).in(Singleton.class);
            bind(StreamMetaStore.class).to(InMemoryStreamMetaStore.class);
            bind(NotificationFeedManager.class).to(NoOpNotificationFeedManager.class);
            bind(NamespaceQueryAdmin.class).to(SimpleNamespaceQueryAdmin.class);
            bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
          }
        })
    );
    ZKClientService zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    streamAdmin = injector.getInstance(StreamAdmin.class);
    txManager = TxInMemory.getTransactionManager(injector.getInstance(TransactionSystemClient.class));
    fileWriterFactory = injector.getInstance(StreamFileWriterFactory.class);
    streamCoordinatorClient = injector.getInstance(StreamCoordinatorClient.class);
    inMemoryAuditPublisher = injector.getInstance(InMemoryAuditPublisher.class);
    authorizer = injector.getInstance(AuthorizerInstantiator.class).get();
    authorizationEnforcementService = injector.getInstance(AuthorizationEnforcementService.class);

    setupNamespaces(injector.getInstance(NamespacedLocationFactory.class));
    txManager.startAndWait();
    streamCoordinatorClient.startAndWait();
    authorizationEnforcementService.startAndWait();
  }

  @AfterClass
  public static void finish() throws Exception {
    authorizationEnforcementService.stopAndWait();
    streamCoordinatorClient.stopAndWait();
    txManager.stopAndWait();
  }

  @Override
  protected StreamAdmin getStreamAdmin() {
    return streamAdmin;
  }

  @Override
  protected StreamFileWriterFactory getFileWriterFactory() {
    return fileWriterFactory;
  }

  @Override
  protected InMemoryAuditPublisher getInMemoryAuditPublisher() {
    return inMemoryAuditPublisher;
  }

  @Override
  protected Authorizer getAuthorizer() {
    return authorizer;
  }
}
