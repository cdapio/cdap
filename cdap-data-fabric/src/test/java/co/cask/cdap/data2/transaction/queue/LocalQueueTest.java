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

package co.cask.cdap.data2.transaction.queue;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data.runtime.DataFabricLocalModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.service.NoOpStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBOrderedTableService;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.data2.transaction.queue.inmemory.InMemoryQueueProducer;
import co.cask.cdap.data2.transaction.queue.leveldb.LevelDBQueueProducer;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.TxConstants;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Tests that injection for local mode uses in-memory for queues and levelDB for streams.
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
      new LocationRuntimeModule().getStandaloneModules(),
      new DiscoveryRuntimeModule().getStandaloneModules(),
      new TransactionMetricsModule(),
      new DataFabricLocalModule(),
      Modules.override(new StreamAdminModules().getStandaloneModules())
        .with(new AbstractModule() {
          @Override
          protected void configure() {
            bind(StreamMetaStore.class).to(NoOpStreamMetaStore.class);
          }
        }));
    // transaction manager is a "service" and must be started
    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();
    txSystemClient = injector.getInstance(TransactionSystemClient.class);
    queueClientFactory = injector.getInstance(QueueClientFactory.class);
    queueAdmin = injector.getInstance(QueueAdmin.class);
    streamAdmin = injector.getInstance(StreamAdmin.class);
    executorFactory = injector.getInstance(TransactionExecutorFactory.class);
    LevelDBOrderedTableService.getInstance().clearTables();
  }

  @Test
  public void testInjection() throws IOException {
    Injector injector = Guice.createInjector(
      new ConfigModule(conf),
      new LocationRuntimeModule().getStandaloneModules(),
      new DiscoveryRuntimeModule().getStandaloneModules(),
      new TransactionMetricsModule(),
      new DataFabricModules().getStandaloneModules(),
      new DataSetsModules().getLocalModule(),
      Modules.override(new StreamAdminModules().getStandaloneModules())
        .with(new AbstractModule() {
          @Override
          protected void configure() {
            bind(StreamMetaStore.class).to(NoOpStreamMetaStore.class);
          }
        }));
    QueueClientFactory factory = injector.getInstance(QueueClientFactory.class);
    QueueProducer producer = factory.createProducer(QueueName.fromStream("bigriver"));
    Assert.assertTrue(producer instanceof LevelDBQueueProducer);
    producer = factory.createProducer(QueueName.fromFlowlet("app", "my", "flowlet", "output"));
    Assert.assertTrue(producer instanceof InMemoryQueueProducer);
  }

}
