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
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
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

  protected static OperationExecutor opex;
  protected static QueueClientFactory queueClientFactory;

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

  @Test
  public void testQueueAbortRetry() throws Exception {
    QueueName queueName = QueueName.fromFlowlet("flow", "flowlet", "queuefailure");
    createEnqueueRunnable(queueName, 5, 1, null).run();

    Queue2Consumer fifoConsumer = queueClientFactory.createConsumer(queueName,
                                                                    new ConsumerConfig(0, 0, 1, 2,
                                                                                       DequeueStrategy.FIFO, null));
    Queue2Consumer hashConsumer = queueClientFactory.createConsumer(queueName,
                                                                    new ConsumerConfig(1, 0, 1, 2,
                                                                                       DequeueStrategy.HASH, "key"));

    TxManager txManager = new TxManager((TransactionAware) fifoConsumer, (TransactionAware) hashConsumer);
    txManager.start();

    Assert.assertEquals(0, Bytes.toInt(fifoConsumer.dequeue().getData().iterator().next()));
    Assert.assertEquals(0, Bytes.toInt(hashConsumer.dequeue().getData().iterator().next()));

    // Abort the consumer transaction
    txManager.abort();

    // Dequeue again in a new transaction, should see the same entries
    txManager.start();
    Assert.assertEquals(0, Bytes.toInt(fifoConsumer.dequeue().getData().iterator().next()));
    Assert.assertEquals(0, Bytes.toInt(hashConsumer.dequeue().getData().iterator().next()));

    txManager.commit();

    // Dequeue again, now should get next entry
    txManager.start();
    Assert.assertEquals(1, Bytes.toInt(fifoConsumer.dequeue().getData().iterator().next()));
    Assert.assertEquals(1, Bytes.toInt(hashConsumer.dequeue().getData().iterator().next()));

    txManager.commit();
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
                                                                                           consumerSize, 1,
                                                                                           dequeueStrategy, "key"));
            try {
              TxManager txManager = new TxManager((TransactionAware) consumer);

              Stopwatch stopwatch = new Stopwatch();
              stopwatch.start();

              int dequeueCount = 0;
              while (valueSum.get() != expectedSum) {
                txManager.start();

                try {
                  DequeueResult result = consumer.dequeue(dequeueBatchSize);
                  txManager.commit();

                  if (result.isEmpty()) {
                    continue;
                  }

                  for (byte[] data : result.getData()) {
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


  private static final class TxManager {
    private final Collection<TransactionAware> txAwares;
    private Transaction transaction;

    TxManager(TransactionAware...txAware) {
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
          if (!txAware.rollbackTx()) {
            LOG.error("Fail to rollback: {}", txAware);
          }
        } catch (Exception e) {
          LOG.error("Exception in rollback: {}", txAware, e);
        }
      }
      opex.abort(transaction);
    }
  }
}
