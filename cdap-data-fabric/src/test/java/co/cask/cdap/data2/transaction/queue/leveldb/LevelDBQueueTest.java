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
package co.cask.cdap.data2.transaction.queue.leveldb;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data.runtime.DataFabricLevelDBModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.service.InMemoryStreamMetaStore;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueEvictor;
import co.cask.cdap.data2.transaction.queue.QueueTest;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * LevelDB queue tests.
 */
public class LevelDBQueueTest extends QueueTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    conf.set(Constants.Dataset.TABLE_PREFIX, "test");
    Injector injector = Guice.createInjector(
      new ConfigModule(conf),
      new LocationRuntimeModule().getStandaloneModules(),
      new DataFabricLevelDBModule(),
      new TransactionMetricsModule(),
      Modules.override(new StreamAdminModules().getStandaloneModules())
        .with(new AbstractModule() {
          @Override
          protected void configure() {
            // The tests are actually testing stream on queue implementation, hence bind it to the queue implementation
            bind(StreamAdmin.class).to(LevelDBStreamAdmin.class);
            bind(StreamMetaStore.class).to(InMemoryStreamMetaStore.class);
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
    LevelDBTableService.getInstance().clearTables();
  }

  // TODO: CDAP-1177 Should move to QueueTest after making getApplicationName() etc instance methods in a base class
  @Test
  public void testQueueTableNameFormat() throws Exception {
    QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "application1", "flow1", "flowlet1",
                                                "output1");
    String tableName = ((LevelDBQueueAdmin) queueAdmin).getActualTableName(queueName);
    Assert.assertEquals("default.system.queue.application1.flow1", tableName);
    Assert.assertEquals("application1", LevelDBQueueAdmin.getApplicationName(tableName));
    Assert.assertEquals("flow1", LevelDBQueueAdmin.getFlowName(tableName));

    queueName = QueueName.fromFlowlet("testNamespace", "application1", "flow1", "flowlet1", "output1");
    tableName = ((LevelDBQueueAdmin) queueAdmin).getActualTableName(queueName);
    Assert.assertEquals("testNamespace.system.queue.application1.flow1", tableName);
    Assert.assertEquals("application1", LevelDBQueueAdmin.getApplicationName(tableName));
    Assert.assertEquals("flow1", LevelDBQueueAdmin.getFlowName(tableName));
  }

  @Override
  protected void forceEviction(QueueName queueName, int numGroups) throws Exception {
    QueueEvictor evictor = ((LevelDBQueueClientFactory) queueClientFactory).createEvictor(queueName, numGroups);
    Transaction tx = txSystemClient.startShort();
    // There is no change, just to get the latest transaction for eviction
    txSystemClient.commit(tx);
    Uninterruptibles.getUninterruptibly(evictor.evict(tx));
  }
}
