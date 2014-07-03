package com.continuuity.data2.transaction.queue;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.runtime.DataFabricLocalModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryQueue2Producer;
import com.continuuity.data2.transaction.queue.leveldb.LevelDBQueue2Producer;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.google.inject.Guice;
import com.google.inject.Injector;
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
      new LocationRuntimeModule().getSingleNodeModules(),
      new DiscoveryRuntimeModule().getSingleNodeModules(),
      new TransactionMetricsModule(),
      new DataFabricLocalModule());
    // transaction manager is a "service" and must be started
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    transactionManager.startAndWait();
    txSystemClient = injector.getInstance(TransactionSystemClient.class);
    queueClientFactory = injector.getInstance(QueueClientFactory.class);
    queueAdmin = injector.getInstance(QueueAdmin.class);
    streamAdmin = injector.getInstance(StreamAdmin.class);
    executorFactory = injector.getInstance(TransactionExecutorFactory.class);
    LevelDBOcTableService.getInstance().clearTables();
  }

  @Test
  public void testInjection() throws IOException {
    Injector injector = Guice.createInjector(
      new ConfigModule(conf),
      new LocationRuntimeModule().getSingleNodeModules(),
      new DiscoveryRuntimeModule().getSingleNodeModules(),
      new TransactionMetricsModule(),
      new DataFabricModules().getSingleNodeModules(),
      new DataSetsModules().getLocalModule());
    QueueClientFactory factory = injector.getInstance(QueueClientFactory.class);
    Queue2Producer producer = factory.createProducer(QueueName.fromStream("bigriver"));
    Assert.assertTrue(producer instanceof LevelDBQueue2Producer);
    producer = factory.createProducer(QueueName.fromFlowlet("app", "my", "flowlet", "output"));
    Assert.assertTrue(producer instanceof InMemoryQueue2Producer);
  }

}
