package com.continuuity.data.operation.ttqueue.randomized;

import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;
import com.continuuity.data.operation.ttqueue.TTQueue;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class Consumer implements Runnable {
  private final int id;
  private final TransactionOracle oracle;
  private final ConsumerControl consumerControl;
  private final int numGroups;
  private final ListeningExecutorService listeningExecutorService;
  private final TestConfig testConfig;
  private final TestController testController;
  private final TTQueue queue;
  private final QueueConfig queueConfig;
  private final Queue<Integer> dequeueList;

  private final AtomicInteger dequeueRunsDone = new AtomicInteger(0);
  private final AtomicInteger stopFlag = new AtomicInteger(0);
  private final AtomicInteger asyncDegree = new AtomicInteger(0);
  private static final int STOP_TRIES_PER_INSTANCE = 3;

  private final ConsumerHolder consumerHolder;

  private static final Logger log = LoggerFactory.getLogger(Consumer.class);

  public Consumer(int id, QueueConsumer queueConsumer, TransactionOracle oracle, ConsumerControl consumerControl,
                  int numGroups,
                  ListeningExecutorService listeningExecutorService, TestConfig testConfig,
                  TestController testController, TTQueue queue, QueueConfig queueConfig, Queue<Integer> dequeueList) {
    this.id = id;
    this.oracle = oracle;
    this.consumerControl = consumerControl;
    this.numGroups = numGroups;
    this.listeningExecutorService = listeningExecutorService;
    this.testConfig = testConfig;
    this.testController = testController;
    this.queue = queue;
    this.queueConfig = queueConfig;
    this.dequeueList = dequeueList;
    this.consumerHolder = new ConsumerHolder(queueConsumer);
  }

  @Override
  public void run() {
    try {
      log.info(getLogMessage(String.format("groupSize:%d started", consumerControl.getSize())));

      // Note: in this test we have a different batch size for each consumer of same group
      final int maxAsyncDegree = queueConfig.isSingleEntry() ?  1 : testConfig.getAsyncDegree();
      log.info(getLogMessage(String.format("maxAsyncDegree=%d", maxAsyncDegree)));

      int stopTries = 0;
      int runId = 0;
      int msgCtr = 0;
      int maxStopTries = STOP_TRIES_PER_INSTANCE * maxAsyncDegree;

      // Run till end of queue or number of runs are done
      while (stopTries < maxStopTries &&
        dequeueRunsDone.get() + asyncDegree.get() < consumerControl.getNumDequeueRuns()) {
        // Create maxAsyncDegree dequeues at a time
        if (asyncDegree.get() < maxAsyncDegree) {
          ++runId;
          asyncDegree.incrementAndGet();
          listeningExecutorService.submit(new QueueDequeue(runId, 0, listeningExecutorService, consumerHolder));
          TimeUnit.MILLISECONDS.sleep(testConfig.getDequeueSleepMs());
        } else {
          TimeUnit.MILLISECONDS.sleep(1000);
        }

        if (msgCtr++ % 200 == 0) {
          msgCtr = 1;
          log.info(getLogMessage(
            String.format(
              "runId=%d, dequeueRunsDone=%d, numDequeueRunsToDo=%d, asyncDegree=%d, maxAsyncDegree=%d, stopTries=%d",
              runId, dequeueRunsDone.get(), consumerControl.getNumDequeueRuns(), asyncDegree.get(),
              maxAsyncDegree, stopTries
            )));
        }

        if (stopFlag.get() > 0) {
          TimeUnit.MILLISECONDS.sleep(100);
          stopTries++;
        } else {
          stopTries = 0;
        }
      }

      TimeUnit.MILLISECONDS.sleep(100);

      // Wait for all async calls to complete
      msgCtr = 0;
      while (asyncDegree.get() > 0) {
        if (msgCtr++ % 200 == 0) {
          msgCtr = 1;
          log.info(getLogMessage(String.format("Waiting for pending async calls (%d) to complete...",
                                               asyncDegree.get())));
        }
        TimeUnit.MILLISECONDS.sleep(100);
      }

      // If all dequeues report empty queue for maxStopTries, then the queue is really empty.
      // Report it to the consumer group.
      if (stopTries >= maxStopTries - 1) {
        log.info(getLogMessage("Consumer at queue end."));
        consumerControl.setConsumersAtQueueEnd(id);
      } else {
        log.info(getLogMessage(
          String.format("dequeueRunsDone=%d, numDequeueRunsToDo=%d, asyncDegree=%d, maxAsyncDegree=%d",
                        dequeueRunsDone.get(), consumerControl.getNumDequeueRuns(), asyncDegree.get(),
                        maxAsyncDegree
          )));
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String getLogMessage(String message) {
    return String.format("Group:%d Consumer:%d: %s", consumerControl.getId(), id, message);
  }

  private Iterable<Integer> entriesToInt(QueueEntry[] entries) {
    return Iterables.transform(Arrays.asList(entries), new Function<QueueEntry, Integer>() {
      @Nullable
      @Override
      public Integer apply(@Nullable QueueEntry input) {
        if (input == null) {
          return -100;
        }
        return Bytes.toInt(input.getData());
      }
    });
  }

  /**
   *
   */
  public class QueueDequeue implements Runnable {
    private final int runId;
    private final int crashId;
    private final ListeningExecutorService listeningExecutorService;
    private final ConsumerHolder consumerHolder;
    private final int oldConsumerId;

    private final Logger log = LoggerFactory.getLogger(QueueDequeue.class);

    public QueueDequeue(int runId, int crashId, ListeningExecutorService listeningExecutorService,
                        ConsumerHolder consumerHolder) {
      this.runId = runId;
      this.crashId = crashId;
      this.listeningExecutorService = listeningExecutorService;
      this.consumerHolder = consumerHolder;
      this.oldConsumerId = consumerHolder.getConsumerId();
    }

    @Override
    public void run() {
      Transaction transaction = oracle.startTransaction(true);

      try {
        TimeUnit.MILLISECONDS.sleep(testConfig.getDequeueSleepMs());
        synchronized (consumerHolder) {
          QueueConsumer consumer = consumerHolder.getConsumer("dequeue", runId, crashId);
          if (oldConsumerId != consumerHolder.getConsumerId()) {
            // Consumer has crashed!
            log.info(getLogMessage(String.format("Consumer has crashed for runId=%d crashId=%d.", runId, crashId)));
            listeningExecutorService.submit(new QueueDequeue(runId, crashId + 1, listeningExecutorService,
                                                             consumerHolder));
            return;
          }
          DequeueResult result = queue.dequeue(consumer, transaction.getReadPointer());
          QueueInfo queueInfo = queue.getQueueInfo();
          log.info(getLogMessage(String.format("QueueInfo=%s", queueInfo.getJSONString())));
          if (result.isEmpty()) {
            if (testController.canDequeueStop()) {
              stopFlag.incrementAndGet();
            }
            asyncDegree.decrementAndGet();
            log.info(getLogMessage(String.format("Stop flag is true. asyncDegree=%d", asyncDegree.get())));
            return;
          } else {
            stopFlag.set(0);
          }
          Iterable<Integer> dequeued = entriesToInt(result.getEntries());
          log.info(getLogMessage(String.format("Intermediate dequeue list=%s", dequeued)));
          listeningExecutorService.submit(new QueueAck(runId, crashId, listeningExecutorService, consumerHolder, result,
                                                       transaction));
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    private String getLogMessage(String message) {
      return Consumer.this.getLogMessage(String.format("runId=%d, crashId=%d %s", runId, crashId, message));
    }
  }

  /**
   *
   */
  public class QueueAck implements Runnable {
    private final int runId;
    private final int crashId;
    private final ListeningExecutorService listeningExecutorService;
    private final ConsumerHolder consumerHolder;
    private final int oldConsumerId;
    private final DequeueResult dequeueResult;
    private final Transaction transaction;

    private final Logger log = LoggerFactory.getLogger(QueueAck.class);

    public QueueAck(int runId, int crashId, ListeningExecutorService listeningExecutorService,
                    ConsumerHolder consumerHolder, DequeueResult dequeueResult, Transaction transaction) {
      this.runId = runId;
      this.crashId = crashId;
      this.listeningExecutorService = listeningExecutorService;
      this.consumerHolder = consumerHolder;
      this.oldConsumerId = consumerHolder.getConsumerId();
      this.dequeueResult = dequeueResult;
      this.transaction = transaction;
    }

    @Override
    public void run() {
      try {
        TimeUnit.MILLISECONDS.sleep(testConfig.getDequeueSleepMs());
        synchronized (consumerHolder) {
          QueueConsumer consumer = consumerHolder.getConsumer("ack", runId, crashId);
          if (oldConsumerId != consumerHolder.getConsumerId()) {
            // Consumer has crashed!
            log.info(getLogMessage(String.format("Consumer has crashed for runId=%d crashId=%d.", runId, crashId)));
            listeningExecutorService.submit(new QueueDequeue(runId, crashId + 1, listeningExecutorService,
                                                             consumerHolder));
            return;
          }
          Assert.assertTrue(dequeueResult.isSuccess());
          queue.ack(dequeueResult.getEntryPointers(), consumer, transaction);

          listeningExecutorService.submit(new QueueCommit(runId, crashId, listeningExecutorService, consumerHolder,
                                                       dequeueResult, transaction));
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    private String getLogMessage(String message) {
      return Consumer.this.getLogMessage(String.format("runId=%d crashId=%d %s", runId, crashId, message));
    }
  }

  /**
   *
   */
  public class QueueCommit implements Runnable {
    private final int runId;
    private final int crashId;
    private final ListeningExecutorService listeningExecutorService;
    private final ConsumerHolder consumerHolder;
    private final DequeueResult dequeueResult;
    private final Transaction transaction;

    private final Logger log = LoggerFactory.getLogger(QueueAck.class);

    public QueueCommit(int runId, int crashId, ListeningExecutorService listeningExecutorService,
                    ConsumerHolder consumerHolder, DequeueResult dequeueResult, Transaction transaction) {
      this.runId = runId;
      this.crashId = crashId;
      this.listeningExecutorService = listeningExecutorService;
      this.consumerHolder = consumerHolder;
      this.dequeueResult = dequeueResult;
      this.transaction = transaction;
    }

    @Override
    public void run() {
      try {
        TimeUnit.MILLISECONDS.sleep(testConfig.getDequeueSleepMs());
        synchronized (consumerHolder) {
          if (testConfig.shouldUnack()) {
            // Note: consumer cannot crash after an ack before txn is committed since Opex runs
            // all the operations together. However, Opex can crash which is not tested here.
            QueueConsumer consumer = consumerHolder.getConsumer();
            log.info(getLogMessage(String.format("Unacking list =%s", entriesToInt(dequeueResult.getEntries()))));
            queue.unack(dequeueResult.getEntryPointers(), consumer, transaction);
            oracle.abortTransaction(transaction);
            oracle.removeTransaction(transaction);
            listeningExecutorService.submit(new QueueAck(runId, crashId, listeningExecutorService, consumerHolder,
                                                         dequeueResult, oracle.startTransaction(true)));
            return;
          } else {
            oracle.commitTransaction(transaction);
          }
          Iterable<Integer> dequeued = entriesToInt(dequeueResult.getEntries());
          Iterables.addAll(dequeueList, dequeued);
          log.info(getLogMessage(String.format("Acked dequeue list=%s", dequeued)));

          listeningExecutorService.submit(new QueueFinalize(runId, crashId, consumerHolder, dequeueResult,
                                                            transaction));
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    private String getLogMessage(String message) {
      return Consumer.this.getLogMessage(String.format("runId=%d crashId=%d %s", runId, crashId, message));
    }
  }

  /**
   *
   */
  public class QueueFinalize implements Runnable {
    private final int runId;
    private final int crashId;
    private final ConsumerHolder consumerHolder;
    private final int oldConsumerId;
    private final DequeueResult dequeueResult;
    private final Transaction transaction;

    public QueueFinalize(int runId, int crashId, ConsumerHolder consumerHolder, DequeueResult dequeueResult,
                         Transaction transaction) {
      this.runId = runId;
      this.crashId = crashId;
      this.consumerHolder = consumerHolder;
      this.oldConsumerId = consumerHolder.getConsumerId();
      this.dequeueResult = dequeueResult;
      this.transaction = transaction;
    }

    @Override
    public void run() {
      try {
        TimeUnit.MILLISECONDS.sleep(testConfig.getDequeueSleepMs());
        synchronized (consumerHolder) {
          QueueConsumer consumer = consumerHolder.getConsumer("finalize", runId, crashId);
          if (oldConsumerId != consumerHolder.getConsumerId()) {
            // Consumer has crashed!
            log.info(getLogMessage(String.format("Consumer has crashed for runId=%d crashId=%d.", runId, crashId)));
            return;
          }
          if (testConfig.shouldFinalize()) {
            queue.finalize(dequeueResult.getEntryPointers(), consumer, numGroups,
                           transaction);
            log.info(getLogMessage(String.format("finalizing=%s",
                                                 entriesToInt(dequeueResult.getEntries()))));
            if (Consumer.this.consumerControl.getId() == 0) {
              QueueInfo queueInfo = queue.getQueueInfo();
              log.info(getLogMessage(String.format("QueueInfo=%s", queueInfo.getJSONString())));
            }
          }
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      } finally {
        final int newAsyncDegree = asyncDegree.decrementAndGet();
        final int neDequeueRunsDone = dequeueRunsDone.incrementAndGet();
        log.info(getLogMessage(String.format("Decremented values of asyncDegree=%d, dequeueRunsDone=%d",
                                             newAsyncDegree, neDequeueRunsDone)));
      }
    }

    private String getLogMessage(String message) {
      return Consumer.this.getLogMessage(String.format("runId=%d crashId=%d %s", runId, crashId, message));
    }
  }

  private class ConsumerHolder {
    private volatile QueueConsumer consumer;

    public ConsumerHolder(QueueConsumer consumer) {
      this.consumer = consumer;
    }

    private void createNewConsumer() throws Exception {
      consumer = new StatefulQueueConsumer(consumer.getInstanceId(), consumer.getGroupId(),
                                                         consumer.getGroupSize(),
                                                         "", TTQueueRandomizedTest.HASH_KEY, consumer.getQueueConfig());
      queue.configure(consumer, oracle.getReadPointer());
    }

    public QueueConsumer getConsumer(String position, int runId, int crashId)
      throws Exception {
      if (testConfig.shouldConsumerCrash()) {
        log.info(getLogMessage(String.format("Crashing consumer before %s runId=%d crashId=%d",
                                             position, runId, crashId)));
        createNewConsumer();
      }
      return consumer;
    }

    public QueueConsumer getConsumer() {
      return consumer;
    }

    public int getConsumerId() {
      return System.identityHashCode(consumer);
    }
  }
}
