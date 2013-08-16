/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.OperationExecutorService;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class HBaseQueueTest extends HBaseTestBase {

  private static Logger LOG = LoggerFactory.getLogger(HBaseQueueTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static Configuration hConf;
  private static InMemoryZKServer zkServer;
  private static OperationExecutorService opexService;
  private static OperationExecutor opex;

  @Test
  public void testSingleFifo() throws Exception {
    // Create the queue table
    final QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "out");
    final String tableName = "queueTestFifo";
    Queue2Producer producer = createProducer(tableName, queueName);
    TransactionAware txAware = (TransactionAware) producer;

    final int count = 30000;
    LOG.info("Start enqueue {} entries.", count);

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    for (int i = 0; i < count; i++) {
      Transaction transaction = opex.start();

      try {
        byte[] queueData = Bytes.toBytes(i);
        txAware.startTx(transaction);
        producer.enqueue(new QueueEntry(queueData));

        if (opex.canCommit(transaction, txAware.getTxChanges()) && txAware.commitTx()) {
          if (!opex.commit(transaction)) {
            txAware.rollbackTx();
          }
        }
      } catch (Exception e) {
        opex.abort(transaction);
        throw Throwables.propagate(e);
      }
    }

    long elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
    LOG.info("Enqueue {} entries in {} ms", count, elapsed);
    LOG.info("Average {} entries per seconds", (double) count * 1000 / elapsed);

    // Try to dequeue
    final long expectedSum = ((long) count / 2 * ((long) count - 1));
    final AtomicLong valueSum = new AtomicLong();
    final int consumerSize = 5;
    final CyclicBarrier startBarrier = new CyclicBarrier(consumerSize + 1);
    final CountDownLatch completeLatch = new CountDownLatch(consumerSize);
    ExecutorService executor = Executors.newFixedThreadPool(consumerSize);
    for (int i = 0; i < consumerSize; i++) {
      final int instanceId = i;
      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            startBarrier.await();
            Queue2Consumer consumer = createConsumer(tableName, queueName,
                                                     new ConsumerConfig(0, instanceId, consumerSize,
                                                                        DequeueStrategy.FIFO, null));
            TransactionAware txAware = (TransactionAware) consumer;

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.start();

            int dequeueCount = 0;
            while (valueSum.get() != expectedSum) {
              Transaction transaction = opex.start();
              txAware.startTx(transaction);

              try {
                DequeueResult result = consumer.dequeue();
                if (opex.canCommit(transaction, txAware.getTxChanges()) && txAware.commitTx()) {
                  if (!opex.commit(transaction)) {
                    txAware.rollbackTx();
                  }
                }
                if (result.isEmpty()) {
                  continue;
                }

                for (byte[] data : result.getData()) {
                  valueSum.addAndGet(Bytes.toInt(data));
                  dequeueCount++;
                }
              } catch (Exception e) {
                opex.abort(transaction);
                throw Throwables.propagate(e);
              }
            }

            long elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
            LOG.info("Dequeue {} entries in {} ms", dequeueCount, elapsed);
            LOG.info("Average {} entries per seconds", (double) dequeueCount * 1000 / elapsed);
            completeLatch.countDown();
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
          }
        }
      });
    }

    startBarrier.await();
    Assert.assertTrue(completeLatch.await(120, TimeUnit.SECONDS));
    TimeUnit.SECONDS.sleep(2);

    Assert.assertEquals(expectedSum, valueSum.get());
    executor.shutdownNow();
  }

  @Test
  public void testSingleHash() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "out");

    final String tableName = "queueHashTest";
    Queue2Producer producer = createProducer(tableName, queueName);
    TransactionAware txAware = (TransactionAware) producer;

    int count = 30000;
    LOG.info("Start enqueue {} entries.", count);

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    for (int i = 0; i < count; i++) {
      Transaction transaction = opex.start();

      try {
        byte[] queueData = Bytes.toBytes(i);
        txAware.startTx(transaction);
        producer.enqueue(new QueueEntry(queueData));

        if (opex.canCommit(transaction, txAware.getTxChanges()) && txAware.commitTx()) {
          if (!opex.commit(transaction)) {
            txAware.rollbackTx();
          }
        }
      } catch (Exception e) {
        opex.abort(transaction);
        throw Throwables.propagate(e);
      }
    }

    long elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
    LOG.info("Enqueue {} entries in {} ms", count, elapsed);
    LOG.info("Average {} entries per seconds", (double) count * 1000 / elapsed);

    // Try to dequeue
    Queue2Consumer consumer = createConsumer(tableName, queueName,
                                             new ConsumerConfig(0, 0, 1, DequeueStrategy.HASH, "key"));
    txAware = (TransactionAware) consumer;

    stopwatch = new Stopwatch();
    stopwatch.start();

    long sum = 0L;
    final long expectedSum = ((long) count / 2 * ((long) count - 1));
    for (int i = 0; i < count; i++) {
      Transaction transaction = opex.start();
      txAware.startTx(transaction);

      try {
        DequeueResult result = consumer.dequeue();
        if (opex.canCommit(transaction, txAware.getTxChanges()) && txAware.commitTx()) {
          if (!opex.commit(transaction)) {
            txAware.rollbackTx();
          }
        }
        for (byte[] data : result.getData()) {
          sum += Bytes.toInt(data);
        }
      } catch (Exception e) {
        opex.abort(transaction);
        throw Throwables.propagate(e);
      }
    }

    elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
    LOG.info("Dequeue {} entries in {} ms", count, elapsed);
    LOG.info("Average {} entries per seconds", (double) count * 1000 / elapsed);

    Assert.assertEquals(expectedSum, sum);
  }

  @Test
  public void testBatchHash() throws OperationException, IOException {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "out");

    final String tableName = "queueBatchHashTest";
    Queue2Producer producer = createProducer(tableName, queueName);
    TransactionAware txAware = (TransactionAware) producer;

    int count = 30000;
    int batchSize = 5;
    byte[] queueData = "queue data".getBytes(Charsets.UTF_8);
    LOG.info("Start enqueue {} entries with batch size {}.", count, batchSize);

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    for (int i = 0; i < count / batchSize; i++) {
      Transaction transaction = opex.start();

      try {
        txAware.startTx(transaction);

        for (int j = 0; j < batchSize; j++) {
          producer.enqueue(new QueueEntry(queueData));
        }

        if (opex.canCommit(transaction, txAware.getTxChanges()) && txAware.commitTx()) {
          if (!opex.commit(transaction)) {
            txAware.rollbackTx();
          }
        }
      } catch (Exception e) {
        opex.abort(transaction);
        throw Throwables.propagate(e);
      }
    }

    long elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
    LOG.info("Enqueue {} entries of batch size {} in {} ms", count, batchSize, elapsed);
    LOG.info("Average {} entries per seconds", (double) count * 1000 / elapsed);

    // Try to dequeue
    batchSize = 50;
    Queue2Consumer consumer = createConsumer(tableName, queueName,
                                             new ConsumerConfig(0, 0, 1, DequeueStrategy.HASH, "key"));
    txAware = (TransactionAware) consumer;

    stopwatch = new Stopwatch();
    stopwatch.start();

    for (int i = 0; i < count / batchSize; i++) {
      Transaction transaction = opex.start();
      txAware.startTx(transaction);

      try {
        DequeueResult result = consumer.dequeue(batchSize);
        if (opex.canCommit(transaction, txAware.getTxChanges()) && txAware.commitTx()) {
          if (!opex.commit(transaction)) {
            txAware.rollbackTx();
          }
        }
        int j = 0;
        for (byte[] data : result.getData()) {
          Assert.assertEquals("queue data", new String(data, Charsets.UTF_8));
          j++;
        }
      } catch (Exception e) {
        opex.abort(transaction);
        throw Throwables.propagate(e);
      }
    }

    elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
    LOG.info("Dequeue {} entries in {} ms of batch size {}", count, elapsed, batchSize);
    LOG.info("Average {} entries per seconds", (double) count * 1000 / elapsed);
  }

  @BeforeClass
  public static void init() throws Exception {
    // Start ZooKeeper
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    // Start hbase
    HBaseTestBase.startHBase();
    hConf = HBaseTestBase.getConfiguration();

    final DataFabricDistributedModule dataFabricModule = new DataFabricDistributedModule(hConf);

    // Customize test configuration
    final CConfiguration cConf = dataFabricModule.getConfiguration();
    cConf.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zkServer.getConnectionStr());
    cConf.set(com.continuuity.data.operation.executor.remote.Constants.CFG_DATA_OPEX_SERVER_PORT,
              Integer.toString(Networks.getRandomPort()));

    final Injector injector = Guice.createInjector(dataFabricModule);

    opexService = injector.getInstance(OperationExecutorService.class);
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          opexService.start(new String[]{}, cConf);
        } catch (ServerException e) {
          LOG.error("Exception.", e);
        }
      }
    };
    t.start();

    // Get the remote opex
    opex = injector.getInstance(OperationExecutor.class);
  }

  @AfterClass
  public static void finish() throws Exception {
    opexService.stop(true);
    HBaseTestBase.stopHBase();
    zkServer.stopAndWait();
  }

  private static Queue2Producer createProducer(String tableName, QueueName queueName) throws IOException {
    return new HBaseQueueClientFactory(getHBaseAdmin(), tableName).createProducer(queueName);
  }

  private static Queue2Consumer createConsumer(String tableName, QueueName queueName,
                                               ConsumerConfig config) throws IOException {
    return new HBaseQueueClientFactory(getHBaseAdmin(), tableName).createConsumer(queueName, config);
  }
}
