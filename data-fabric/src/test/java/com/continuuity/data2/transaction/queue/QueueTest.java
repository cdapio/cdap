package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.queue.DequeueStrategy;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * tests for queues. Extend this class to run the tests against an implementation of the queues.
 */
public abstract class QueueTest {

  private static final Logger LOG = LoggerFactory.getLogger(QueueTest.class);

  private static final int ROUNDS = 1000;
  private static final long TIMEOUT_MS = 2 * 60 * 1000L;

  protected static OperationExecutor opex;
  protected static QueueClientFactory queueClientFactory;
  protected static QueueAdmin queueAdmin;
  protected static InMemoryTransactionManager transactionManager;

  @AfterClass
  public static void shutdownTx() {
    if (transactionManager != null) {
      transactionManager.close();
    }
  }


  // Simple enqueue and dequeue with one consumer, no batch
  @Test(timeout = TIMEOUT_MS)
  public void testSingleFifo() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "singlefifo");
    enqueueDequeue(queueName, ROUNDS, ROUNDS, 1, 1, DequeueStrategy.FIFO, 1);
  }

  // Simple enqueue and dequeue with three consumers, no batch
  @Test(timeout = TIMEOUT_MS)
  public void testMultiFifo() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "multififo");
    enqueueDequeue(queueName, ROUNDS, ROUNDS, 1, 3, DequeueStrategy.FIFO, 1);
  }

  // Simple enqueue and dequeue with one consumer, no batch
  @Test(timeout = TIMEOUT_MS)
  public void testSingleHash() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "singlehash");
    enqueueDequeue(queueName, 2 * ROUNDS, ROUNDS, 1, 1, DequeueStrategy.HASH, 1);
  }

  @Test(timeout = TIMEOUT_MS)
  public void testMultiHash() throws Exception {
    QueueName queueName = QueueName.fromStream("bingo", "bang");
    enqueueDequeue(queueName, 2 * ROUNDS, ROUNDS, 1, 3, DequeueStrategy.HASH, 1);
  }

  // Batch enqueue and batch dequeue with one consumer.
  @Test(timeout = TIMEOUT_MS)
  public void testBatchHash() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "batchhash");
    enqueueDequeue(queueName, 2 * ROUNDS, ROUNDS, 10, 1, DequeueStrategy.HASH, 50);
  }

  @Test(timeout = TIMEOUT_MS)
  public void testQueueAbortRetrySkip() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "queuefailure");
    createEnqueueRunnable(queueName, 5, 1, null).run();

    Queue2Consumer fifoConsumer = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(0, 0, 1, DequeueStrategy.FIFO, null), 2);
    Queue2Consumer hashConsumer = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(1, 0, 1, DequeueStrategy.HASH, "key"), 2);

    TxManager txManager = new TxManager((TransactionAware) fifoConsumer, (TransactionAware) hashConsumer);
    txManager.start();

    Assert.assertEquals(0, Bytes.toInt(fifoConsumer.dequeue().iterator().next()));
    Assert.assertEquals(0, Bytes.toInt(hashConsumer.dequeue().iterator().next()));

    // Abort the consumer transaction
    txManager.abort();

    // Dequeue again in a new transaction, should see the same entries
    txManager.start();
    Assert.assertEquals(0, Bytes.toInt(fifoConsumer.dequeue().iterator().next()));
    Assert.assertEquals(0, Bytes.toInt(hashConsumer.dequeue().iterator().next()));
    txManager.commit();

    // Dequeue again, now should get next entry
    txManager.start();
    Assert.assertEquals(1, Bytes.toInt(fifoConsumer.dequeue().iterator().next()));
    Assert.assertEquals(1, Bytes.toInt(hashConsumer.dequeue().iterator().next()));
    txManager.commit();

    // Dequeue a result and abort.
    txManager.start();
    DequeueResult fifoResult = fifoConsumer.dequeue();
    DequeueResult hashResult = hashConsumer.dequeue();

    Assert.assertEquals(2, Bytes.toInt(fifoResult.iterator().next()));
    Assert.assertEquals(2, Bytes.toInt(hashResult.iterator().next()));
    txManager.abort();

    // Now skip the result with a new transaction.
    txManager.start();
    fifoResult.reclaim();
    hashResult.reclaim();
    txManager.commit();

    // Dequeue again, it should see a new entry
    txManager.start();
    Assert.assertEquals(3, Bytes.toInt(fifoConsumer.dequeue().iterator().next()));
    Assert.assertEquals(3, Bytes.toInt(hashConsumer.dequeue().iterator().next()));
    txManager.commit();

    // Dequeue again, it should see a new entry
    txManager.start();
    Assert.assertEquals(4, Bytes.toInt(fifoConsumer.dequeue().iterator().next()));
    Assert.assertEquals(4, Bytes.toInt(hashConsumer.dequeue().iterator().next()));
    txManager.commit();

    txManager.start();
    if (fifoConsumer instanceof Closeable) {
      ((Closeable) fifoConsumer).close();
    }
    if (hashConsumer instanceof Closeable) {
      ((Closeable) hashConsumer).close();
    }
    txManager.commit();

    verifyQueueIsEmpty(queueName, 2);
  }

  @Test(timeout = TIMEOUT_MS)
  public void testRollback() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "queuerollback");
    Queue2Producer producer = queueClientFactory.createProducer(queueName);
    Queue2Consumer consumer = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(0, 0, 1, DequeueStrategy.FIFO, null), 1);

    TxManager txManager = new TxManager((TransactionAware) producer,
                                        (TransactionAware) consumer,
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
    });

    // First, try to enqueue and commit would fail
    txManager.start();
    try {
      producer.enqueue(new QueueEntry(Bytes.toBytes(1)));
      txManager.commit();
      // If reaches here, it's wrong, as exception should be thrown.
      Assert.assertTrue(false);
    } catch (OperationException e) {
      txManager.abort();
    }

    // Try to enqueue again. Within the same transaction, dequeue should be empty.
    txManager.start();
    producer.enqueue(new QueueEntry(Bytes.toBytes(1)));
    Assert.assertTrue(consumer.dequeue().isEmpty());
    txManager.commit();

    // This time, enqueue has been committed, dequeue would see the item
    txManager.start();
    try {
      Assert.assertEquals(1, Bytes.toInt(consumer.dequeue().iterator().next()));
      txManager.commit();
      // If reaches here, it's wrong, as exception should be thrown.
      Assert.assertTrue(false);
    } catch (OperationException e) {
      txManager.abort();
    }

    // Dequeue again, since last tx was rollback, this dequeue should see the item again.
    txManager.start();
    Assert.assertEquals(1, Bytes.toInt(consumer.dequeue().iterator().next()));
    txManager.commit();
  }

  @Test
  public void testReset() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "queue1");
    createEnqueueRunnable(queueName, 5, 1, null).run();

    Queue2Consumer consumer1 = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(0, 0, 1, DequeueStrategy.FIFO, null), 2);

    TxManager txManager = new TxManager((TransactionAware) consumer1);

    // Check that there's smth in the queue, but do not consume: abort tx after check
    txManager.start();
    Assert.assertEquals(0, Bytes.toInt(consumer1.dequeue().iterator().next()));
    txManager.abort();

    // Reset queues
    queueAdmin.dropAll();

    // we gonna need another one to check again to avoid caching side-affects
    Queue2Consumer consumer2 = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(1, 0, 1, DequeueStrategy.FIFO, null), 2);
    txManager = new TxManager((TransactionAware) consumer2);
    // Check again: should be nothing in the queue
    txManager.start();
    Assert.assertFalse(consumer2.dequeue().iterator().hasNext());
    txManager.commit();
  }

  private void enqueueDequeue(final QueueName queueName, int preEnqueueCount,
                              int concurrentCount, int enqueueBatchSize,
                              final int consumerSize, final DequeueStrategy dequeueStrategy,
                              final int dequeueBatchSize) throws Exception {

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
            Queue2Consumer consumer = queueClientFactory.createConsumer(
              queueName, new ConsumerConfig(0, instanceId, consumerSize, dequeueStrategy, "key"), 1);
            try {
              TxManager txManager = new TxManager((TransactionAware) consumer);

              Stopwatch stopwatch = new Stopwatch();
              stopwatch.start();

              int dequeueCount = 0;
              while (valueSum.get() < expectedSum) {
                txManager.start();

                try {
                  DequeueResult result = consumer.dequeue(dequeueBatchSize);
                  txManager.commit();

                  if (result.isEmpty()) {
                    continue;
                  }

                  for (byte[] data : result) {
                    valueSum.addAndGet(Bytes.toInt(data));
                    dequeueCount++;
                  }
                } catch (OperationException e) {
                  LOG.error("Operation error", e);
                  txManager.abort();
                  throw Throwables.propagate(e);
                }
              }

              long elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
              LOG.info("Dequeue {} entries in {} ms for {}", dequeueCount, elapsed, queueName.getSimpleName());
              LOG.info("Dequeue avg {} entries per seconds for {}",
                       (double) dequeueCount * 1000 / elapsed, queueName.getSimpleName());

              if (consumer instanceof Closeable) {
                txManager.start();
                ((Closeable) consumer).close();
                txManager.commit();
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
    TimeUnit.SECONDS.sleep(2);

    Assert.assertEquals(expectedSum, valueSum.get());

    verifyQueueIsEmpty(queueName, 1);
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
            TxManager txManager = new TxManager((TransactionAware) producer);

            LOG.info("Start enqueue {} entries.", count);

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.start();

            // Pre-Enqueue
            int batches = count / batchSize;
            List<QueueEntry> queueEntries = Lists.newArrayListWithCapacity(batchSize);
            for (int i = 0; i < batches; i++) {
              txManager.start();

              try {
                queueEntries.clear();
                for (int j = 0; j < batchSize; j++) {
                  int val = i * batchSize + j;
                  byte[] queueData = Bytes.toBytes(val);
                  queueEntries.add(new QueueEntry("key", val, queueData));
                }

                producer.enqueue(queueEntries);
                txManager.commit();
              } catch (OperationException e) {
                LOG.error("Operation error", e);
                txManager.abort();
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

  /**
   *
   */
  protected static final class TxManager {
    private final Collection<TransactionAware> txAwares;
    private Transaction transaction;

    protected TxManager(TransactionAware...txAware) {
      txAwares = ImmutableList.copyOf(txAware);
    }

    public void start() throws OperationException {
      transaction = opex.start();
      for (TransactionAware txAware : txAwares) {
        txAware.startTx(transaction);
      }
    }

    public void commit() throws OperationException {
      // Collects change sets
      Set<byte[]> changeSet = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
      for (TransactionAware txAware : txAwares) {
        changeSet.addAll(txAware.getTxChanges());
      }

      // Check for conflicts
      if (!opex.canCommit(transaction, changeSet)) {
        throw new OperationException(StatusCode.TRANSACTION_CONFLICT, "Cannot commit tx: conflict detected");
      }

      // Persist changes
      for (TransactionAware txAware : txAwares) {
        try {
          if (!txAware.commitTx()) {
            throw new OperationException(StatusCode.INVALID_TRANSACTION, "Fails to commit tx.");
          }
        } catch (Exception e) {
          throw new OperationException(StatusCode.INVALID_TRANSACTION, "Fails to commit tx.", e);
        }
      }

      // Make visible
      if (!opex.commit(transaction)) {
        throw new OperationException(StatusCode.INVALID_TRANSACTION, "Fails to make tx visible.");
      }

      // Post commit call
      for (TransactionAware txAware : txAwares) {
        try {
          txAware.postTxCommit();
        } catch (Throwable t) {
          LOG.error("Post commit call failure.", t);
        }
      }
    }

    public void abort() throws OperationException {
      for (TransactionAware txAware : txAwares) {
        try {
          if (!txAware.rollbackTx() || !opex.abort(transaction)) {
            LOG.error("Fail to rollback: {}", txAware);
          }
        } catch (Exception e) {
          LOG.error("Exception in rollback: {}", txAware, e);
        }
      }
    }
  }

  private void verifyQueueIsEmpty(QueueName queueName, int numActualConsumers) throws IOException, OperationException {
    // the queue has been consumed by n consumers. Use a consumerId greater than n to make sure it can dequeue.
    Queue2Consumer consumer = queueClientFactory.createConsumer(
      queueName, new ConsumerConfig(numActualConsumers + 1, 0, 1, DequeueStrategy.FIFO, null), -1);

    TxManager txManager = new TxManager((TransactionAware) consumer);
    txManager.start();
    Assert.assertTrue("Entire queue should be evicted after test but dequeue succeeds.",
                       consumer.dequeue().isEmpty());
    txManager.abort();
  }
}
