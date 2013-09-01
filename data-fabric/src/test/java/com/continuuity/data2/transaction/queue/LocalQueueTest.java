package com.continuuity.data2.transaction.queue;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricLocalModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.StatePersistor;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryQueue2Producer;
import com.continuuity.data2.transaction.queue.leveldb.LevelDBQueue2Producer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests that injection for local mode uses in-memory for queues and levelDB for streams.
 */
public class LocalQueueTest extends QueueTest {

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.unset(Constants.CFG_DATA_LEVELDB_DIR);
    conf.setBoolean(StatePersistor.CFG_DO_PERSIST, false);
    Injector injector = Guice.createInjector(new DataFabricLocalModule(conf));
    // transaction manager is a "service" and must be started
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    transactionManager.init();
    // Get the local opex
    opex = injector.getInstance(OperationExecutor.class);
    queueClientFactory = injector.getInstance(QueueClientFactory.class);
    queueAdmin = injector.getInstance(QueueAdmin.class);
  }

  @Test
  public void testInjection() throws IOException {
    Injector injector = Guice.createInjector(new DataFabricModules().getSingleNodeModules());
    QueueClientFactory factory = injector.getInstance(QueueClientFactory.class);
    Queue2Producer producer = factory.createProducer(QueueName.fromStream("big", "river"));
    Assert.assertTrue(producer instanceof LevelDBQueue2Producer);
    producer = factory.createProducer(QueueName.fromFlowlet("my", "flowlet", "output"));
    Assert.assertTrue(producer instanceof InMemoryQueue2Producer);
  }

}
