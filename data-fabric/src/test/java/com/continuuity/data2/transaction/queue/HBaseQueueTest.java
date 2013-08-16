/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
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
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
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

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueueTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static InMemoryZKServer zkServer;
  private static OperationExecutorService opexService;
  private static OperationExecutor opex;
  private static QueueClientFactory queueClientFactory;

  // Simple enqueue and dequeue with one consumer, no batch
  @Test
  public void testSingleFifo() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "singlefifo");
    enqueueDequeue(queueName, 30000, 30000, 1, 1, DequeueStrategy.FIFO, 1, 120, TimeUnit.SECONDS);
  }

  // Simple enqueue and dequeue with three consumers, no batch
  @Test
  public void testMultiFifo() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "multififo");
    enqueueDequeue(queueName, 30000, 30000, 1, 3, DequeueStrategy.FIFO, 1, 120, TimeUnit.SECONDS);
  }

  // Simple enqueue and dequeue with one consumer, no batch
  @Test
  public void testSingleHash() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "singlehash");
    enqueueDequeue(queueName, 60000, 30000, 1, 1, DequeueStrategy.HASH, 1, 120, TimeUnit.SECONDS);
  }

  @Test
  public void testMultiHash() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "multihash");
    enqueueDequeue(queueName, 60000, 30000, 1, 3, DequeueStrategy.HASH, 1, 120, TimeUnit.SECONDS);
  }

  // Batch enqueue and batch dequeue with one consumer.
  @Test
  public void testBatchHash() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "batchhash");
    enqueueDequeue(queueName, 60000, 30000, 10, 1, DequeueStrategy.HASH, 50, 120, TimeUnit.SECONDS);
  }

  @BeforeClass
  public static void init() throws Exception {
    // Start ZooKeeper
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    // Start hbase
    HBaseTestBase.startHBase();

    final DataFabricDistributedModule dataFabricModule =
      new DataFabricDistributedModule(HBaseTestBase.getConfiguration());

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
    queueClientFactory = injector.getInstance(QueueClientFactory.class);
  }

  @AfterClass
  public static void finish() throws Exception {
    opexService.stop(true);
    HBaseTestBase.stopHBase();
    zkServer.stopAndWait();
  }

  private void enqueueDequeue(final QueueName queueName, int preEnqueueCount,
                              int concurrentCount, int enqueueBatchSize,
                              final int consumerSize, final DequeueStrategy dequeueStrategy,
                              final int dequeueBatchSize,
                              long timeout, TimeUnit timeoutUnit) throws Exception {

    Preconditions.checkArgument(preEnqueueCount % enqueueBatchSize == 0, "Count must be divisible by enqueueBatchSize");
    Preconditions.checkArgument(concurrentCount % enqueueBatchSize == 0, "Count must be divisible by enqueueBatchSize");

    createEnqueueRunnable(queueName, preEnqueueCount, enqueueBatchSize, null).run();

    final CyclicBarrier startBarrier = new CyclicBarrier(consumerSize + 2);
    ExecutorService executor = Executors.newFixedThreadPool(consumerSize + 1);

    // Enqueue thread
    executor.submit(createEnqueueRunnable(queueName, concurrentCount, enqueueBatchSize, startBarrier));

    // Dequeue
    final long expectedSum = ((long) preEnqueueCount / 2 * ((long) preEnqueueCount - 1)) +
                             ((long) concurrentCount / 2 * ((long) concurrentCount - 1));
    final AtomicLong valueSum = new AtomicLong();
    final CountDownLatch completeLatch = new CountDownLatch(consumerSize);

    for (int i = 0; i < consumerSize; i++) {
      final int instanceId = i;
      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            startBarrier.await();
            LOG.info("Consumer {} starts consuming {}", instanceId, queueName.getSimpleName());
            Queue2Consumer consumer = queueClientFactory.createConsumer(queueName,
                                                                        new ConsumerConfig(0, instanceId,
                                                                                           consumerSize,
                                                                                           dequeueStrategy, "key"));
            try {
              TransactionAware txAware = (TransactionAware) consumer;

              Stopwatch stopwatch = new Stopwatch();
              stopwatch.start();

              int dequeueCount = 0;
              while (valueSum.get() != expectedSum) {
                Transaction transaction = opex.start();
                txAware.startTx(transaction);

                try {
                  DequeueResult result = consumer.dequeue(dequeueBatchSize);

                  Preconditions.checkState(opex.canCommit(transaction, txAware.getTxChanges()),
                                           "Conflicts in pre commit check.");
                  Preconditions.checkState(txAware.commitTx(), "Fails to commit.");
                  Preconditions.checkState(opex.commit(transaction), "Fails to commit transaction.");

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
              LOG.info("Dequeue {} entries in {} ms for {}", dequeueCount, elapsed, queueName.getSimpleName());
              LOG.info("Dequeue avg {} entries per seconds for {}",
                       (double)dequeueCount * 1000 / elapsed, queueName.getSimpleName());
              completeLatch.countDown();
            } finally {
              if (consumer instanceof Closeable) {
                ((Closeable) consumer).close();
              }
            }
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
          }
        }
      });
    }

    startBarrier.await();
    Assert.assertTrue(completeLatch.await(timeout, timeoutUnit));
    TimeUnit.SECONDS.sleep(2);

    Assert.assertEquals(expectedSum, valueSum.get());
    executor.shutdownNow();
  }

  private Runnable createEnqueueRunnable(final QueueName queueName, final int count,
                                         final int batchSize, final CyclicBarrier barrier) {
    return new Runnable() {

      @Override
      public void run() {
        try {
          if (barrier != null) {
            barrier.await();
          }
          Queue2Producer producer = queueClientFactory.createProducer(queueName);
          try {
            TransactionAware txAware = (TransactionAware) producer;

            LOG.info("Start enqueue {} entries.", count);

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.start();

            // Pre-Enqueue
            int batches = count / batchSize;
            List<QueueEntry> queueEntries = Lists.newArrayListWithCapacity(batchSize);
            for (int i = 0; i < batches; i++) {
              Transaction transaction = opex.start();
              txAware.startTx(transaction);

              queueEntries.clear();
              for (int j = 0; j < batchSize; j++) {
                int val = i * batchSize + j;
                byte[] queueData = Bytes.toBytes(val);
                queueEntries.add(new QueueEntry("key", val, queueData));
              }

              producer.enqueue(queueEntries);

              Preconditions.checkState(opex.canCommit(transaction, txAware.getTxChanges()),
                                       "Conflicts in pre commit check.");
              Preconditions.checkState(txAware.commitTx(), "Fails to commit.");
              Preconditions.checkState(opex.commit(transaction), "Fails to commit transaction.");
            }

            long elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
            LOG.info("Enqueue {} entries in {} ms for {}", count, elapsed, queueName.getSimpleName());
            LOG.info("Enqueue avg {} entries per seconds for {}",
                     (double)count * 1000 / elapsed, queueName.getSimpleName());
            stopwatch.stop();
          } finally {
            if (producer instanceof Closeable) {
              ((Closeable) producer).close();
            }
          }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    };
  }
}
