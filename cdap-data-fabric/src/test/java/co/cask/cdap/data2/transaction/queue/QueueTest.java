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

package co.cask.cdap.data2.transaction.queue;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.DequeueResult;
import co.cask.cdap.data2.queue.DequeueStrategy;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueConsumer;
import co.cask.cdap.data2.queue.QueueEntry;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests for queues. Extend this class to run the tests against an implementation of the queues.
 */
public abstract class QueueTest {

  private static final Logger LOG = LoggerFactory.getLogger(QueueTest.class);

  private static final int ROUNDS = 100;
  private static final long TIMEOUT_MS = 2 * 60 * 1000L;

  protected static TransactionSystemClient txSystemClient;
  protected static QueueClientFactory queueClientFactory;
  protected static QueueAdmin queueAdmin;
  protected static StreamAdmin streamAdmin;
  protected static TransactionManager transactionManager;
  protected static TransactionExecutorFactory executorFactory;

  @AfterClass
  public static void shutdownTx() {
    if (transactionManager != null) {
      transactionManager.stopAndWait();
    }
  }

  @Test
  public void testCreateProducerWithMetricsEnsuresTableExists() throws Exception {
    QueueName queueName = QueueName.fromStream("someStream");
    final QueueProducer producer = queueClientFactory.createProducer(queueName, new QueueMetrics() {
      @Override
      public void emitEnqueue(int count) {}

      @Override
      public void emitEnqueueBytes(int bytes) {}
    });

    Assert.assertNotNull(producer);
  }

  @Test
  public void testDropAllQueues() throws Exception {
    // create a queue and a stream and enqueue one entry each
    QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "myApp", "myFlow", "myFlowlet", "tDAQ");
    QueueName streamName = QueueName.fromStream("tDAQStream");
    final QueueProducer qProducer = queueClientFactory.createProducer(queueName);
    final QueueProducer sProducer = queueClientFactory.createProducer(streamName);
    executorFactory.createExecutor(Lists.newArrayList((TransactionAware) qProducer, (TransactionAware) sProducer))
                   .execute(new TransactionExecutor.Subroutine() {
                     @Override
                     public void apply() throws Exception {
                       qProducer.enqueue(new QueueEntry(Bytes.toBytes("q42")));
                       sProducer.enqueue(new QueueEntry(Bytes.toBytes("s42")));
                     }
                   });
    // drop all queues
    queueAdmin.dropAll();
    // verify that queue is gone and stream is still there
    final QueueConsumer qConsumer = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(0, 0, 1, DequeueStrategy.FIFO, null), 1);
    final QueueConsumer sConsumer = queueClientFactory.createConsumer(
      streamName, new ConsumerConfig(0, 0, 1, DequeueStrategy.FIFO, null), 1);
    executorFactory.createExecutor(Lists.newArrayList((TransactionAware) qConsumer, (TransactionAware) sConsumer))
                   .execute(new TransactionExecutor.Subroutine() {
                     @Override
                     public void apply() throws Exception {
                       DequeueResult<byte[]> dequeue = qConsumer.dequeue();
                       Assert.assertTrue(dequeue.isEmpty());
                       dequeue = sConsumer.dequeue();
                       Assert.assertFalse(dequeue.isEmpty());
                       Iterator<byte[]> iterator = dequeue.iterator();
                       Assert.assertTrue(iterator.hasNext());
                       Assert.assertArrayEquals(Bytes.toBytes("s42"), iterator.next());
                     }
                   });
  }

  @Test
  public void testDropAllStreams() throws Exception {
    // create a queue and a stream and enqueue one entry each
    QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "myApp", "myFlow", "myFlowlet", "tDAS");
    QueueName streamName = QueueName.fromStream("tDASStream");
    final QueueProducer qProducer = queueClientFactory.createProducer(queueName);
    final QueueProducer sProducer = queueClientFactory.createProducer(streamName);
    executorFactory.createExecutor(Lists.newArrayList((TransactionAware) qProducer, (TransactionAware) sProducer))
                   .execute(new TransactionExecutor.Subroutine() {
                     @Override
                     public void apply() throws Exception {
                       qProducer.enqueue(new QueueEntry(Bytes.toBytes("q42")));
                       sProducer.enqueue(new QueueEntry(Bytes.toBytes("s42")));
                     }
                   });
    // drop all queues
    streamAdmin.dropAll();
    // verify that queue is gone and stream is still there
    final QueueConsumer qConsumer = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(0, 0, 1, DequeueStrategy.FIFO, null), 1);
    final QueueConsumer sConsumer = queueClientFactory.createConsumer(
      streamName, new ConsumerConfig(0, 0, 1, DequeueStrategy.FIFO, null), 1);
    executorFactory.createExecutor(Lists.newArrayList((TransactionAware) qConsumer, (TransactionAware) sConsumer))
                   .execute(new TransactionExecutor.Subroutine() {
                     @Override
                     public void apply() throws Exception {
                       DequeueResult<byte[]> dequeue = sConsumer.dequeue();
                       Assert.assertTrue(dequeue.isEmpty());
                       dequeue = qConsumer.dequeue();
                       Assert.assertFalse(dequeue.isEmpty());
                       Iterator<byte[]> iterator = dequeue.iterator();
                       Assert.assertTrue(iterator.hasNext());
                       Assert.assertArrayEquals(Bytes.toBytes("q42"), iterator.next());
                     }
                   });
  }


  @Test
  public void testStreamQueue() throws Exception {
    QueueName queueName = QueueName.fromStream("my_stream");
    final QueueProducer producer = queueClientFactory.createProducer(queueName);
    executorFactory.createExecutor(Lists.newArrayList((TransactionAware) producer))
      .execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        QueueEntry entry = new QueueEntry(Bytes.toBytes("my_data"));
        producer.enqueue(entry);
      }
    });

    final QueueConsumer consumer =
      queueClientFactory.createConsumer(queueName,
                                        new ConsumerConfig(0, 0, 1, DequeueStrategy.FIFO, null), 1);
    executorFactory.createExecutor(Lists.newArrayList((TransactionAware) consumer))
      .execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          DequeueResult<byte[]> dequeue = consumer.dequeue();
          Assert.assertTrue(dequeue != null && !dequeue.isEmpty());
          Iterator<byte[]> iterator = dequeue.iterator();
          Assert.assertTrue(iterator.hasNext());
          Assert.assertArrayEquals(Bytes.toBytes("my_data"), iterator.next());
          Assert.assertFalse(iterator.hasNext());
        }
      });
  }

  // Simple enqueue and dequeue with one consumer, no batch
  @Test(timeout = TIMEOUT_MS)
  public void testSingleFifo() throws Exception {
    QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "app", "flow", "flowlet", "singlefifo");
    enqueueDequeue(queueName, ROUNDS, ROUNDS, 1, 1, DequeueStrategy.FIFO, 1);
  }

  // Simple enqueue and dequeue with three consumers, no batch
  @Category(SlowTests.class)
  @Test(timeout = TIMEOUT_MS)
  public void testMultiFifo() throws Exception {
    QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "app", "flow", "flowlet", "multififo");
    enqueueDequeue(queueName, ROUNDS, ROUNDS, 1, 3, DequeueStrategy.FIFO, 1);
  }

  // Simple enqueue and dequeue with one consumer, no batch
  @Test(timeout = TIMEOUT_MS)
  public void testSingleHash() throws Exception {
    QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "app", "flow", "flowlet", "singlehash");
    enqueueDequeue(queueName, 2 * ROUNDS, ROUNDS, 1, 1, DequeueStrategy.HASH, 1);
  }

  @Category(SlowTests.class)
  @Test(timeout = TIMEOUT_MS)
  public void testMultiHash() throws Exception {
    QueueName queueName = QueueName.fromStream("bingoBang");
    enqueueDequeue(queueName, 2 * ROUNDS, ROUNDS, 1, 3, DequeueStrategy.HASH, 1);
  }

  // Batch enqueue and batch dequeue with one consumer.
  @Category(SlowTests.class)
  @Test(timeout = TIMEOUT_MS)
  public void testBatchHash() throws Exception {
    QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "app", "flow", "flowlet", "batchhash");
    enqueueDequeue(queueName, 2 * ROUNDS, ROUNDS, 10, 1, DequeueStrategy.HASH, 10);
  }

  @Test(timeout = TIMEOUT_MS)
  public void testQueueAbortRetrySkip() throws Exception {
    QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "app", "flow", "flowlet", "queuefailure");
    configureGroups(queueName, ImmutableMap.of(0L, 1, 1L, 1));

    createEnqueueRunnable(queueName, 5, 1, null).run();

    QueueConsumer fifoConsumer = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(0, 0, 1, DequeueStrategy.FIFO, null), 2);
    QueueConsumer hashConsumer = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(1, 0, 1, DequeueStrategy.HASH, "key"), 2);

    TransactionContext txContext = createTxContext(fifoConsumer, hashConsumer);
    txContext.start();

    Assert.assertEquals(0, Bytes.toInt(fifoConsumer.dequeue().iterator().next()));
    Assert.assertEquals(0, Bytes.toInt(hashConsumer.dequeue().iterator().next()));

    // Abort the consumer transaction
    txContext.abort();

    // Dequeue again in a new transaction, should see the same entries
    txContext.start();
    Assert.assertEquals(0, Bytes.toInt(fifoConsumer.dequeue().iterator().next()));
    Assert.assertEquals(0, Bytes.toInt(hashConsumer.dequeue().iterator().next()));
    txContext.finish();

    // Dequeue again, now should get next entry
    txContext.start();
    Assert.assertEquals(1, Bytes.toInt(fifoConsumer.dequeue().iterator().next()));
    Assert.assertEquals(1, Bytes.toInt(hashConsumer.dequeue().iterator().next()));
    txContext.finish();

    // Dequeue a result and abort.
    txContext.start();
    DequeueResult<byte[]> fifoResult = fifoConsumer.dequeue();
    DequeueResult<byte[]> hashResult = hashConsumer.dequeue();

    Assert.assertEquals(2, Bytes.toInt(fifoResult.iterator().next()));
    Assert.assertEquals(2, Bytes.toInt(hashResult.iterator().next()));
    txContext.abort();

    // Now skip the result with a new transaction.
    txContext.start();
    fifoResult.reclaim();
    hashResult.reclaim();
    txContext.finish();

    // Dequeue again, it should see a new entry
    txContext.start();
    Assert.assertEquals(3, Bytes.toInt(fifoConsumer.dequeue().iterator().next()));
    Assert.assertEquals(3, Bytes.toInt(hashConsumer.dequeue().iterator().next()));
    txContext.finish();

    // Dequeue again, it should see a new entry
    txContext.start();
    Assert.assertEquals(4, Bytes.toInt(fifoConsumer.dequeue().iterator().next()));
    Assert.assertEquals(4, Bytes.toInt(hashConsumer.dequeue().iterator().next()));
    txContext.finish();

    txContext.start();
    if (fifoConsumer instanceof Closeable) {
      ((Closeable) fifoConsumer).close();
    }
    if (hashConsumer instanceof Closeable) {
      ((Closeable) hashConsumer).close();
    }
    txContext.finish();

    verifyQueueIsEmpty(queueName, 2, 1);
  }

  @Test(timeout = TIMEOUT_MS)
  public void testRollback() throws Exception {
    QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "app", "flow", "flowlet", "queuerollback");
    QueueProducer producer = queueClientFactory.createProducer(queueName);
    QueueConsumer consumer = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(0, 0, 1, DequeueStrategy.FIFO, null), 1);

    TransactionContext txContext = createTxContext(producer, consumer,
                                        new TransactionAware() {

      boolean canCommit = false;

      @Override
      public void startTx(Transaction tx) {
      }

      @Override
      public Collection<byte[]> getTxChanges() {
        return ImmutableList.of();
      }

      @Override
      public boolean commitTx() throws Exception {
        // Flip-flop between commit success/failure.
        boolean res = canCommit;
        canCommit = !canCommit;
        return res;
      }

      @Override
      public void postTxCommit() {
      }

      @Override
      public boolean rollbackTx() throws Exception {
        return true;
      }


      @Override
      public String getTransactionAwareName() {
        return "test";
      }                                    
    });

    // First, try to enqueue and commit would fail
    txContext.start();
    try {
      producer.enqueue(new QueueEntry(Bytes.toBytes(1)));
      txContext.finish();
      // If reaches here, it's wrong, as exception should be thrown.
      Assert.assertTrue(false);
    } catch (TransactionFailureException e) {
      txContext.abort();
    }

    // Try to enqueue again. Within the same transaction, dequeue should be empty.
    txContext.start();
    producer.enqueue(new QueueEntry(Bytes.toBytes(1)));
    Assert.assertTrue(consumer.dequeue().isEmpty());
    txContext.finish();

    // This time, enqueue has been committed, dequeue would see the item
    txContext.start();
    try {
      Assert.assertEquals(1, Bytes.toInt(consumer.dequeue().iterator().next()));
      txContext.finish();
      // If reaches here, it's wrong, as exception should be thrown.
      Assert.assertTrue(false);
    } catch (TransactionFailureException e) {
      txContext.abort();
    }

    // Dequeue again, since last tx was rollback, this dequeue should see the item again.
    txContext.start();
    Assert.assertEquals(1, Bytes.toInt(consumer.dequeue().iterator().next()));
    txContext.finish();
  }

  @Test
  public void testOneFIFOEnqueueDequeue() throws Exception {
    testOneEnqueueDequeue(DequeueStrategy.FIFO);
  }

  @Test
  public void testOneRoundRobinEnqueueDequeue() throws Exception {
    testOneEnqueueDequeue(DequeueStrategy.ROUND_ROBIN);
  }

  @Test
  public void testClearAllForFlow() throws Exception {
    testClearOrDropAllForFlow(false);
  }

  @Test
  public void testDropAllForFlow() throws Exception {
    testClearOrDropAllForFlow(true);
  }

  @Test
  public void testDropAllForNamespace() throws Exception {
    // deliberately used namespace names 'namespace' and 'namespace1' to test correct prefix matching
    // create 4 queues
    QueueName myQueue1 = QueueName.fromFlowlet("namespace", "myapp1", "myflow1", "myflowlet1", "myout1");
    QueueName myQueue2 = QueueName.fromFlowlet("namespace", "myapp2", "myflow2", "myflowlet2", "myout2");
    QueueName yourQueue1 = QueueName.fromFlowlet("namespace1", "yourapp1", "yourflow1", "yourflowlet1", "yourout1");
    QueueName yourQueue2 = QueueName.fromFlowlet("namespace1", "yourapp2", "yourflow2", "yourflowlet2", "yourout2");

    String myQueueName1 = myQueue1.toString();
    String myQueueName2 = myQueue2.toString();
    String yourQueueName1 = yourQueue1.toString();
    String yourQueueName2 = yourQueue2.toString();

    queueAdmin.create(myQueueName1);
    queueAdmin.create(myQueueName2);
    queueAdmin.create(yourQueueName1);
    queueAdmin.create(yourQueueName2);

    // verify that queues got created
    Assert.assertTrue(queueAdmin.exists(myQueueName1) && queueAdmin.exists(myQueueName2) &&
                        queueAdmin.exists(yourQueueName1) && queueAdmin.exists(yourQueueName2));

    // create some consumer configurations for all queues
    configureGroups(myQueue1, ImmutableMap.of(0L, 1, 1L, 1));
    configureGroups(myQueue2, ImmutableMap.of(0L, 1, 1L, 1));
    configureGroups(yourQueue1, ImmutableMap.of(0L, 1, 1L, 1));
    configureGroups(yourQueue2, ImmutableMap.of(0L, 1, 1L, 1));

    // verify that the consumer config exists
    verifyConsumerConfigExists(myQueue1, myQueue2, yourQueue1, yourQueue2);

    // drop queues in namespace 'namespace'
    queueAdmin.dropAllInNamespace("namespace");

    // verify queues in 'namespace' are dropped
    Assert.assertFalse(queueAdmin.exists(myQueueName1) || queueAdmin.exists(myQueueName2));
    // also verify that consumer config of all queues in 'myspace' is deleted
    verifyConsumerConfigIsDeleted(myQueue1, myQueue2);

    // but the ones in 'namespace1' still exist
    Assert.assertTrue(queueAdmin.exists(yourQueueName1) && queueAdmin.exists(yourQueueName2));
    // consumer config for queues in 'namespace1' should also still exist
    verifyConsumerConfigExists(yourQueue1, yourQueue2);

    // drop queues in 'namespace1'
    queueAdmin.dropAllInNamespace("namespace1");

    // verify queues in 'namespace1' are dropped
    Assert.assertFalse(queueAdmin.exists(yourQueueName1) || queueAdmin.exists(yourQueueName2));
    // verify that the consumer config of all queues in 'namespace1' is deleted
    verifyConsumerConfigIsDeleted(yourQueue1, yourQueue2);
  }

  @Test
  public void testClearAllForFlowWithNoQueues() throws Exception {
    queueAdmin.dropAll();
    queueAdmin.clearAllForFlow(Constants.DEFAULT_NAMESPACE, "app", "flow");
  }

  @Test
  public void testDropAllForFlowWithNoQueues() throws Exception {
    queueAdmin.dropAll();
    queueAdmin.dropAllForFlow(Constants.DEFAULT_NAMESPACE, "app", "flow");
  }

  private void testClearOrDropAllForFlow(boolean doDrop) throws Exception {
    // this test is the same for clear and drop, except fot two small places...
    // using a different app name for each case as this test leaves some entries
    String app = doDrop ? "tDAFF" : "tCAFF";

    QueueName queueName1 = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, app, "flow1", "flowlet1", "out1");
    QueueName queueName2 = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, app, "flow1", "flowlet2", "out2");
    QueueName queueName3 = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, app, "flow2", "flowlet1", "out");
    configureGroups(queueName1, ImmutableMap.of(0L, 1, 1L, 1));
    configureGroups(queueName2, ImmutableMap.of(0L, 1, 1L, 1));
    configureGroups(queueName3, ImmutableMap.of(0L, 1, 1L, 1));
    QueueProducer producer1 = queueClientFactory.createProducer(queueName1);
    QueueProducer producer2 = queueClientFactory.createProducer(queueName2);
    QueueProducer producer3 = queueClientFactory.createProducer(queueName3);
    TransactionContext txContext = createTxContext(producer1, producer2, producer3);
    txContext.start();
    for (int i = 0; i < 10; i++) {
      for (QueueProducer producer : Arrays.asList(producer1, producer2, producer3)) {
        producer.enqueue(new QueueEntry(Bytes.toBytes(i)));
      }
    }
    txContext.finish();

    // consume 1 element from each queue
    ConsumerConfig consumerConfig = new ConsumerConfig(0, 0, 1, DequeueStrategy.FIFO, null);
    QueueConsumer consumer1 = queueClientFactory.createConsumer(queueName1, consumerConfig, 1);
    QueueConsumer consumer2 = queueClientFactory.createConsumer(queueName2, consumerConfig, 1);
    QueueConsumer consumer3 = queueClientFactory.createConsumer(queueName3, consumerConfig, 1);
    txContext = createTxContext(consumer1, consumer2, consumer3);
    txContext.start();
    for (QueueConsumer consumer : Arrays.asList(consumer1, consumer2, consumer3)) {
      DequeueResult<byte[]> result = consumer.dequeue(1);
      Assert.assertFalse(result.isEmpty());
      Assert.assertArrayEquals(Bytes.toBytes(0), result.iterator().next());
    }
    txContext.finish();

    // verify the consumer config was deleted
    verifyConsumerConfigExists(queueName1, queueName2);

    // clear/drop all queues for flow1
    if (doDrop) {
      queueAdmin.dropAllForFlow(Constants.DEFAULT_NAMESPACE, app, "flow1");
    } else {
      queueAdmin.clearAllForFlow(Constants.DEFAULT_NAMESPACE, app, "flow1");
    }

    if (doDrop) {
      // verify that only flow2's queues still exist
      Assert.assertFalse(queueAdmin.exists(queueName1.toString()));
      Assert.assertFalse(queueAdmin.exists(queueName2.toString()));
      Assert.assertTrue(queueAdmin.exists(queueName3.toString()));
    } else {
      // verify all queues still exist
      Assert.assertTrue(queueAdmin.exists(queueName1.toString()));
      Assert.assertTrue(queueAdmin.exists(queueName2.toString()));
      Assert.assertTrue(queueAdmin.exists(queueName3.toString()));
    }
    // verify the consumer config was deleted
    verifyConsumerConfigIsDeleted(queueName1, queueName2);

    // create new consumers because existing ones may have pre-fetched and cached some entries
    consumer1 = queueClientFactory.createConsumer(queueName1, consumerConfig, 1);
    consumer2 = queueClientFactory.createConsumer(queueName2, consumerConfig, 1);
    consumer3 = queueClientFactory.createConsumer(queueName3, consumerConfig, 1);

    txContext = createTxContext(consumer1, consumer2, consumer3);
    txContext.start();
    // attempt to consume from flow1's queues, should be empty
    for (QueueConsumer consumer : Arrays.asList(consumer1, consumer2)) {
      DequeueResult<byte[]> result = consumer.dequeue(1);
      Assert.assertTrue(result.isEmpty());
    }
    // but flow2 was not deleted -> consumer 3 should get another entry
    DequeueResult<byte[]> result = consumer3.dequeue(1);
    Assert.assertFalse(result.isEmpty());
    Assert.assertArrayEquals(Bytes.toBytes(1), result.iterator().next());
    txContext.finish();
  }

  protected void verifyConsumerConfigExists(QueueName ... queueNames) throws InterruptedException {
    // do nothing, HBase test will override this
  }
  protected void verifyConsumerConfigIsDeleted(QueueName ... queueNames) throws InterruptedException {
    // do nothing, HBase test will override this
  }

  @Test
  public void testReset() throws Exception {
    // NOTE: using different name of the queue from other unit-tests because this test leaves entries
    QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "app", "flow", "flowlet", "queueReset");
    configureGroups(queueName, ImmutableMap.of(0L, 1, 1L, 1));
    QueueProducer producer = queueClientFactory.createProducer(queueName);
    TransactionContext txContext = createTxContext(producer);
    txContext.start();
    producer.enqueue(new QueueEntry(Bytes.toBytes(0)));
    producer.enqueue(new QueueEntry(Bytes.toBytes(1)));
    producer.enqueue(new QueueEntry(Bytes.toBytes(2)));
    producer.enqueue(new QueueEntry(Bytes.toBytes(3)));
    producer.enqueue(new QueueEntry(Bytes.toBytes(4)));
    txContext.finish();

    QueueConsumer consumer1 = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(0, 0, 1, DequeueStrategy.FIFO, null), 2);

    // Check that there's smth in the queue, but do not consume: abort tx after check
    txContext = createTxContext(consumer1);
    txContext.start();
    Assert.assertEquals(0, Bytes.toInt(consumer1.dequeue().iterator().next()));
    txContext.finish();

    // Reset queues
    queueAdmin.dropAll();

    // we gonna need another one to check again to avoid caching side-affects
    QueueConsumer consumer2 = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(1, 0, 1, DequeueStrategy.FIFO, null), 2);
    txContext = createTxContext(consumer2);
    // Check again: should be nothing in the queue
    txContext.start();
    Assert.assertTrue(consumer2.dequeue().isEmpty());
    txContext.finish();

    // add another entry
    txContext = createTxContext(producer);
    txContext.start();
    producer.enqueue(new QueueEntry(Bytes.toBytes(5)));
    txContext.finish();

    txContext = createTxContext(consumer2);
    // Check again: consumer should see new entry
    txContext.start();
    Assert.assertEquals(5, Bytes.toInt(consumer2.dequeue().iterator().next()));
    txContext.finish();
  }

  @Category(SlowTests.class)
  @Test
  public void testConcurrentEnqueue() throws Exception {
    // This test is for testing multiple producers that writes with a delay after a transaction started.
    // This is for verifying consumer advances the startKey correctly.
    final QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "app", "flow", "flowlet",
                                                      "concurrent");
    configureGroups(queueName, ImmutableMap.of(0L, 1));

    final CyclicBarrier barrier = new CyclicBarrier(4);

    // Starts three producers to enqueue concurrently. For each entry, starts a TX, sleep, enqueue, commit.
    ExecutorService executor = Executors.newFixedThreadPool(3);
    final int entryCount = 50;
    for (int i = 0; i < 3; i++) {
      final QueueProducer producer = queueClientFactory.createProducer(queueName);
      final int producerId = i + 1;
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            barrier.await();
            for (int i = 0; i < entryCount; i++) {
              TransactionContext txContext = createTxContext(producer);
              txContext.start();
              // Sleeps at different rate to make the scan in consumer has higher change to see
              // the transaction but not the entry (as not yet written)
              TimeUnit.MILLISECONDS.sleep(producerId * 50);
              producer.enqueue(new QueueEntry(Bytes.toBytes(i)));
              txContext.finish();
            }
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
          } finally {
            if (producer instanceof Closeable) {
              Closeables.closeQuietly((Closeable) producer);
            }
          }
        }
      });
    }

    // sum(0..entryCount) * 3
    int expectedSum = entryCount * (entryCount - 1) / 2 * 3;
    QueueConsumer consumer = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(0, 0, 1, DequeueStrategy.FIFO, null), 1);

    // Trigger starts of producer
    barrier.await();

    int dequeueSum = 0;
    int noProgress = 0;
    while (dequeueSum != expectedSum && noProgress < 200) {
      TransactionContext txContext = createTxContext(consumer);
      txContext.start();
      DequeueResult<byte[]> result = consumer.dequeue();
      if (!result.isEmpty()) {
        noProgress = 0;
        int value = Bytes.toInt(result.iterator().next());
        dequeueSum += value;
      } else {
        noProgress++;
        TimeUnit.MILLISECONDS.sleep(10);
      }
      txContext.finish();
    }

    if (consumer instanceof Closeable) {
      Closeables.closeQuietly((Closeable) consumer);
    }

    Assert.assertEquals(expectedSum, dequeueSum);
  }

  @Test
  public void testMultiStageConsumer() throws Exception {
    final QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "app", "flow", "flowlet",
                                                      "multistage");
    configureGroups(queueName, ImmutableMap.of(0L, 2));

    // Enqueue 10 items
    final QueueProducer producer = queueClientFactory.createProducer(queueName);
    for (int i = 0; i < 10; i++) {
      TransactionContext txContext = createTxContext(producer);
      txContext.start();
      producer.enqueue(new QueueEntry("key", i, Bytes.toBytes(i)));
      txContext.finish();
    }

    // Consumer all even entries
    QueueConsumer consumer = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(0, 0, 2, DequeueStrategy.HASH, "key"), 1);

    for (int i = 0; i < 5; i++) {
      TransactionContext txContext = createTxContext(consumer);
      txContext.start();
      DequeueResult<byte[]> result = consumer.dequeue();
      Assert.assertTrue(!result.isEmpty());
      Assert.assertEquals(i * 2, Bytes.toInt(result.iterator().next()));
      txContext.finish();
    }

    if (consumer instanceof Closeable) {
      ((Closeable) consumer).close();
    }

    // Consume 2 odd entries
    consumer = queueClientFactory.createConsumer(
    queueName, new ConsumerConfig(0, 1, 2, DequeueStrategy.HASH, "key"), 1);
    TransactionContext txContext = createTxContext(consumer);
    txContext.start();
    DequeueResult<byte[]> result = consumer.dequeue(2);
    Assert.assertEquals(2, result.size());
    Iterator<byte[]> iter = result.iterator();
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(i * 2 + 1, Bytes.toInt(iter.next()));
    }
    txContext.finish();

    // Close the consumer and re-create with the same instance ID, it should keep consuming
    if (consumer instanceof Closeable) {
      ((Closeable) consumer).close();
    }

    // Consume the rest odd entries
    consumer = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(0, 1, 2, DequeueStrategy.HASH, "key"), 1);
    for (int i = 2; i < 5; i++) {
      txContext = createTxContext(consumer);
      txContext.start();
      result = consumer.dequeue();
      Assert.assertTrue(!result.isEmpty());
      Assert.assertEquals(i * 2 + 1, Bytes.toInt(result.iterator().next()));
      txContext.finish();
    }
  }

  private void testOneEnqueueDequeue(DequeueStrategy strategy) throws Exception {
    // since this is used by more than one test method, ensure uniqueness of the queue name by adding strategy
    QueueName queueName = QueueName.fromFlowlet(Constants.DEFAULT_NAMESPACE, "app", "flow", "flowlet",
                                                "queue1" + strategy.toString());
    configureGroups(queueName, ImmutableMap.of(0L, 1, 1L, 1));
    QueueProducer producer = queueClientFactory.createProducer(queueName);
    TransactionContext txContext = createTxContext(producer);
    txContext.start();
    producer.enqueue(new QueueEntry(Bytes.toBytes(55)));
    txContext.finish();

    QueueConsumer consumer = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(0, 0, 1, strategy, null), 2);

    txContext = createTxContext(consumer);
    txContext.start();
    Assert.assertEquals(55, Bytes.toInt(consumer.dequeue().iterator().next()));
    txContext.finish();

    if (producer instanceof Closeable) {
      ((Closeable) producer).close();
    }
    if (consumer instanceof Closeable) {
      ((Closeable) consumer).close();
    }

    forceEviction(queueName);

    // verifying that consumer of the 2nd group can process items: they were not evicted
    QueueConsumer consumer2 = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(1, 0, 1, strategy, null), 2);

    txContext = createTxContext(consumer2);
    txContext.start();
    Assert.assertEquals(55, Bytes.toInt(consumer2.dequeue().iterator().next()));
    txContext.finish();

    if (consumer2 instanceof Closeable) {
      ((Closeable) consumer2).close();
    }

    // now all should be evicted
    verifyQueueIsEmpty(queueName, 2, 1);
  }

  private void enqueueDequeue(final QueueName queueName, int preEnqueueCount,
                              int concurrentCount, int enqueueBatchSize,
                              final int consumerSize, final DequeueStrategy dequeueStrategy,
                              final int dequeueBatchSize) throws Exception {

    configureGroups(queueName, ImmutableMap.of(0L, consumerSize));

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
            QueueConsumer consumer = queueClientFactory.createConsumer(
              queueName, new ConsumerConfig(0, instanceId, consumerSize, dequeueStrategy, "key"), 1);
            try {
              TransactionContext txContext = createTxContext(consumer);

              Stopwatch stopwatch = new Stopwatch();
              stopwatch.start();

              int dequeueCount = 0;
              while (valueSum.get() < expectedSum) {
                txContext.start();

                try {
                  DequeueResult<byte[]> result = consumer.dequeue(dequeueBatchSize);
                  txContext.finish();

                  if (result.isEmpty()) {
                    continue;
                  }

                  for (byte[] data : result) {
                    valueSum.addAndGet(Bytes.toInt(data));
                    dequeueCount++;
                  }
                } catch (TransactionFailureException e) {
                  LOG.error("Operation error", e);
                  txContext.abort();
                  throw Throwables.propagate(e);
                }
              }

              long elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
              LOG.info("Dequeue {} entries in {} ms for {}", dequeueCount, elapsed, queueName.getSimpleName());
              LOG.info("Dequeue avg {} entries per seconds for {}",
                       (double) dequeueCount * 1000 / elapsed, queueName.getSimpleName());

              if (consumer instanceof Closeable) {
                txContext.start();
                ((Closeable) consumer).close();
                txContext.finish();
              }

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
    completeLatch.await();

    Assert.assertEquals(expectedSum, valueSum.get());

    // Only check eviction for queue.
    if (!queueName.isStream()) {
      verifyQueueIsEmpty(queueName, 1, consumerSize);
    }
    executor.shutdownNow();
  }

  private TransactionContext createTxContext(Object... txAwares) {
    TransactionAware[] casted = new TransactionAware[txAwares.length];
    for (int i = 0; i < txAwares.length; i++) {
      casted[i] = (TransactionAware) txAwares[i];
    }
    return new TransactionContext(txSystemClient, casted);
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
          QueueProducer producer = queueClientFactory.createProducer(queueName);
          try {
            TransactionContext txContext = createTxContext(producer);

            LOG.info("Start enqueue {} entries.", count);

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.start();

            // Pre-Enqueue
            int batches = count / batchSize;
            List<QueueEntry> queueEntries = Lists.newArrayListWithCapacity(batchSize);
            // include some negative hash values and some positive ones
            int hashValueMultiplier = -1;
            for (int i = 0; i < batches; i++) {
              txContext.start();

              try {
                queueEntries.clear();
                for (int j = 0; j < batchSize; j++) {
                  int val = i * batchSize + j;
                  byte[] queueData = Bytes.toBytes(val);
                  queueEntries.add(new QueueEntry("key", hashValueMultiplier * val, queueData));
                  hashValueMultiplier *= -1;
                }

                producer.enqueue(queueEntries);
                txContext.finish();
              } catch (TransactionFailureException e) {
                LOG.error("Operation error", e);
                txContext.abort();
                throw Throwables.propagate(e);
              }
            }

            long elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
            LOG.info("Enqueue {} entries in {} ms for {}", count, elapsed, queueName.getSimpleName());
            LOG.info("Enqueue avg {} entries per seconds for {}",
                     (double) count * 1000 / elapsed, queueName.getSimpleName());
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

  protected void configureGroups(QueueName queueName, Map<Long, Integer> groupInfo) throws Exception {
    // Do NOTHING by default
  }

  protected void verifyQueueIsEmpty(QueueName queueName, int numberOfGroups, int instancesPerGroup) throws Exception {
    // Verify the queue is empty
    for (int i = 0; i < numberOfGroups; i++) {
      for (int j = 0; j < instancesPerGroup; j++) {
        QueueConsumer consumer = queueClientFactory.createConsumer(
          queueName, new ConsumerConfig(i, j, instancesPerGroup, DequeueStrategy.FIFO, null), -1);

        TransactionContext txContext = createTxContext(consumer);
        try {
          txContext.start();
          Assert.assertTrue(consumer.dequeue().isEmpty());
          txContext.finish();
        } catch (TransactionFailureException e) {
          txContext.abort();
          throw Throwables.propagate(e);
        } finally {
          if (consumer instanceof Closeable) {
            ((Closeable) consumer).close();
          }
        }
      }
    }

    forceEviction(queueName);

    // the queue has been consumed by n consumers. Use a consumerId greater than n to make sure it can dequeue.
    QueueConsumer consumer = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(numberOfGroups + 1, 0, 1, DequeueStrategy.FIFO, null), -1);

    TransactionContext txContext = createTxContext(consumer);
    txContext.start();
    DequeueResult<byte[]> result = consumer.dequeue();
    if (!result.isEmpty()) {
      StringBuilder resultString = new StringBuilder();
      Iterator<byte[]> resultIter = result.iterator();
      while (resultIter.hasNext()) {
        if (resultString.length() > 0) {
          resultString.append(", ");
        }
        resultString.append(Bytes.toInt(resultIter.next()));
      }
      LOG.info("Queue should be empty but returned result: " + result.toString() + ", value = " +
               resultString.toString());
    }
    Assert.assertTrue("Entire queue should be evicted after test but dequeue succeeds.", result.isEmpty());
    txContext.abort();
  }

  protected void forceEviction(QueueName queueName) throws Exception {
    // do nothing by default: in most cases eviction happens along with consuming elements of the queue
  }
}
