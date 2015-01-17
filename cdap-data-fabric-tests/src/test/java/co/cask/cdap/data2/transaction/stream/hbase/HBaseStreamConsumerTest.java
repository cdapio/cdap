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
package co.cask.cdap.data2.transaction.stream.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data.runtime.DataFabricDistributedModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.service.NoOpStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamConsumerTestBase;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import co.cask.tephra.persist.NoOpTransactionStateStorage;
import co.cask.tephra.persist.TransactionStateStorage;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
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
public class HBaseStreamConsumerTest extends StreamConsumerTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static HBaseTestBase testHBase;
  private static CConfiguration cConf;
  private static StreamConsumerFactory consumerFactory;
  private static StreamAdmin streamAdmin;
  private static TransactionSystemClient txClient;
  private static TransactionManager txManager;
  private static QueueClientFactory queueClientFactory;
  private static StreamFileWriterFactory fileWriterFactory;

  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClientService;

  @BeforeClass
  public static void init() throws Exception {
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();

    Configuration hConf = testHBase.getConfiguration();

    cConf = CConfiguration.create();
    cConf.setInt(Constants.Stream.CONTAINER_INSTANCES, 1);
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new TransactionMetricsModule(),
      Modules.override(new DataFabricDistributedModule(), new StreamAdminModules().getDistributedModules())
        .with(new AbstractModule() {
          @Override
          protected void configure() {
            bind(TransactionStateStorage.class).to(NoOpTransactionStateStorage.class);
            bind(TransactionSystemClient.class).to(InMemoryTxSystemClient.class).in(Singleton.class);
            bind(StreamMetaStore.class).to(NoOpStreamMetaStore.class);
          }
        })
    );
    zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    streamAdmin = injector.getInstance(StreamAdmin.class);
    consumerFactory = injector.getInstance(StreamConsumerFactory.class);
    txClient = injector.getInstance(TransactionSystemClient.class);
    txManager = injector.getInstance(TransactionManager.class);
    queueClientFactory = injector.getInstance(QueueClientFactory.class);
    fileWriterFactory = injector.getInstance(StreamFileWriterFactory.class);

    txManager.startAndWait();
  }

  @AfterClass
  public static void finish() throws Exception {
    txManager.stopAndWait();
    testHBase.stopHBase();
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
