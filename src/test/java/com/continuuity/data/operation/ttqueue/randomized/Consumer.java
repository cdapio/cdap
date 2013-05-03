package com.continuuity.data.operation.ttqueue.randomized;

import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;
import com.continuuity.data.operation.ttqueue.TTQueue;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
*
*/
public class Consumer implements Runnable {
  private final int id;
  private final TransactionOracle oracle;
  private final ConsumerGroupControl consumerGroupControl;
  private final ListeningExecutorService listeningExecutorService;
  private final TestConfig testConfig;
  private final TestController testController;
  private final TTQueue queue;
  private final QueueConfig queueConfig;
  private final Queue<Integer> dequeueList;

  private final AtomicInteger dequeueRunsDone = new AtomicInteger(0);
  private final AtomicInteger stopFlag = new AtomicInteger(0);
  private final AtomicInteger asyncDegree = new AtomicInteger(0);
  private static final int MAX_STOP_TRIES = 3;

  private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

  public Consumer(int id, TransactionOracle oracle, ConsumerGroupControl consumerGroupControl,
                  ListeningExecutorService listeningExecutorService, TestConfig testConfig,
                  TestController testController, TTQueue queue, QueueConfig queueConfig, Queue<Integer> dequeueList) {
    this.id = id;
    this.oracle = oracle;
    this.consumerGroupControl = consumerGroupControl;
    this.listeningExecutorService = listeningExecutorService;
    this.testConfig = testConfig;
    this.testController = testController;
    this.queue = queue;
    this.queueConfig = queueConfig;
    this.dequeueList = dequeueList;
  }

  @Override
  public void run() {
    try {
      // Wait for setup to be done
      testController.waitToStart();
      LOG.info(getLogMessage(String.format("groupSize:%d started", consumerGroupControl.getSize())));

      // Note: in this test we have a different batch size for each consumer
      final int maxAsyncDegree = queueConfig.isSingleEntry() ?  1 : testConfig.getAsyncDegree();
      LOG.info(getLogMessage(String.format("maxAsyncDegree=%d", maxAsyncDegree)));

      // Create consumer and configure queue
      ConsumerHolder consumerHolder = new ConsumerHolder();
      consumerGroupControl.doneSingleConfigure();
      LOG.info(getLogMessage("Configure done, waiting for group configuration to be done..."));
      consumerGroupControl.waitForGroupConfigure();
      LOG.info(getLogMessage("Group configuration done, starting dequeues..."));

      int stopTries = 0;
      int runId = 0;
      // Create maxAsyncDegree dequeues at a time
      while(stopTries < MAX_STOP_TRIES &&
        dequeueRunsDone.get() + asyncDegree.get() < consumerGroupControl.getNumDequeueRuns()) {
        ++runId;
        if(asyncDegree.get() < maxAsyncDegree) {
          listeningExecutorService.submit(new QueueDequeue(runId, listeningExecutorService, consumerHolder));
          TimeUnit.MILLISECONDS.sleep(testConfig.getDequeueSleepMs());
        } else {
          TimeUnit.MILLISECONDS.sleep(1000);
        }
        LOG.info(getLogMessage(String.format("Async degree=%d, max async degree=%d", asyncDegree.get(),
                                             maxAsyncDegree)));
        if(stopFlag.get() > 0) {
          TimeUnit.MILLISECONDS.sleep(100);
          stopTries++;
        } else {
          stopTries = 0;
        }
      }

      // Wait for all async calls to complete
      while(asyncDegree.get() > 0) {
        TimeUnit.MILLISECONDS.sleep(100);
      }

      // If all dequeues report empty queue for MAX_STOP_TRIES, then the queue is really empty.
      // Report it to the consumer group.
      if(stopTries >= MAX_STOP_TRIES - 1) {
        LOG.info(getLogMessage("Stop flag is true."));
        consumerGroupControl.setConsumersAtQueueEnd(id);
      } else {
        LOG.info(getLogMessage(String.format("dequeueRunsDone=%d, numDequeueRunsToDo=%d, asyncDegree=%d",
                                             dequeueRunsDone.get(), consumerGroupControl.getNumDequeueRuns(),
                                             asyncDegree.get())));
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String getLogMessage(String message) {
    return String.format("Group:%d Consumer:%d: %s", consumerGroupControl.getId(), id, message);
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

  public class QueueDequeue implements Runnable {
    private final int runId;
    private final ListeningExecutorService listeningExecutorService;
    private final ConsumerHolder consumerHolder;

    public QueueDequeue(int runId, ListeningExecutorService listeningExecutorService,
                        ConsumerHolder consumerHolder) {
      this.runId = runId;
      this.listeningExecutorService = listeningExecutorService;
      this.consumerHolder = consumerHolder;
    }

    @Override
    public void run() {
      asyncDegree.incrementAndGet();
      Transaction transaction = oracle.startTransaction();

      try {
        TimeUnit.MILLISECONDS.sleep(testConfig.getDequeueSleepMs());
        synchronized (consumerHolder) {
          QueueConsumer consumer = consumerHolder.getConsumer("dequeue", runId);
          if(consumerHolder.hasCrashed(runId)) {
            listeningExecutorService.submit(new QueueDequeue(runId, listeningExecutorService, consumerHolder));
            asyncDegree.decrementAndGet();
            return;
          }
          DequeueResult result = queue.dequeue(consumer, transaction.getReadPointer());
          if(result.isEmpty()) {
            if(testController.canDequeueStop()) {
              stopFlag.incrementAndGet();
            }
            asyncDegree.decrementAndGet();
            return;
          } else {
            stopFlag.set(0);
          }
          Iterable<Integer> dequeued = entriesToInt(result.getEntries());
          LOG.info(getLogMessage(String.format("runId=%d intermediate dequeue list=%s", runId, dequeued)));
          listeningExecutorService.submit(new QueueAck(runId, listeningExecutorService, consumerHolder, result,
                                                       transaction));
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public class QueueAck implements Runnable {
    private final int runId;
    private final ListeningExecutorService listeningExecutorService;
    private final ConsumerHolder consumerHolder;
    private final DequeueResult dequeueResult;
    private final Transaction transaction;

    public QueueAck(int runId, ListeningExecutorService listeningExecutorService, ConsumerHolder consumerHolder,
                    DequeueResult dequeueResult, Transaction transaction) {
      this.runId = runId;
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
          QueueConsumer consumer = consumerHolder.getConsumer("ack", runId);
          if(consumerHolder.hasCrashed(runId)) {
            listeningExecutorService.submit(new QueueDequeue(runId, listeningExecutorService, consumerHolder));
            asyncDegree.decrementAndGet();
            return;
          }
          Assert.assertTrue(dequeueResult.isSuccess());
          queue.ack(dequeueResult.getEntryPointers(), consumer, transaction.getReadPointer());
          oracle.commitTransaction(transaction);
          Iterable<Integer> dequeued = entriesToInt(dequeueResult.getEntries());
          Iterables.addAll(dequeueList, dequeued);
          LOG.info(getLogMessage(String.format("runId=%d acked dequeue list=%s", runId, dequeued)));

          listeningExecutorService.submit(new QueueFinalize(runId, consumerHolder, dequeueResult, transaction));
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public class QueueFinalize implements Runnable {
    private final int runId;
    private final ConsumerHolder consumerHolder;
    private final DequeueResult dequeueResult;
    private final Transaction transaction;

    public QueueFinalize(int runId, ConsumerHolder consumerHolder, DequeueResult dequeueResult,
                         Transaction transaction) {
      this.runId = runId;
      this.consumerHolder = consumerHolder;
      this.dequeueResult = dequeueResult;
      this.transaction = transaction;
    }

    @Override
    public void run() {
      try {
        TimeUnit.MILLISECONDS.sleep(testConfig.getDequeueSleepMs());
        synchronized (consumerHolder) {
          QueueConsumer consumer = consumerHolder.getConsumer("finalize", runId);
          if(consumerHolder.hasCrashed(runId)) {
            return;
          }
          if(testConfig.shouldFinalize()) {
            queue.finalize(dequeueResult.getEntryPointers(), consumer, consumerGroupControl.getSize(),
                           transaction.getWriteVersion());
          }
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      } finally {
        asyncDegree.decrementAndGet();
        dequeueRunsDone.incrementAndGet();
      }
    }
  }

  public class ConsumerHolder {
    private volatile QueueConsumer consumer;
    private final AtomicBoolean hasCrashed = new AtomicBoolean(false);
    private Set<Integer> crashInflight = Sets.newHashSet();
    private Set<Integer> inFlight = Sets.newHashSet();

    public ConsumerHolder() throws Exception {
      consumer = createConsumer();
    }

    private QueueConsumer createConsumer() throws Exception {
      QueueConsumer consumer = new StatefulQueueConsumer(id, consumerGroupControl.getId(),
                                                         consumerGroupControl.getSize(),
                                                         "", TTQueueRandomizedTest.HASH_KEY, queueConfig);
      queue.configure(consumer);
      return consumer;
    }

    public QueueConsumer getConsumer(String position, int runId)
      throws Exception {
      inFlight.add(runId);
      if(testConfig.shouldConsumerCrash()) {
        LOG.info(getLogMessage(String.format("Crashing consumer before %s runId=%d", position, runId)));
        hasCrashed.set(true);
        crashInflight.addAll(inFlight);
        consumer = createConsumer();
        return consumer;
      }
      hasCrashed.set(false);
      return consumer;
    }

    public boolean hasCrashed(int runId) {
      boolean crashed = crashInflight.contains(runId);
      crashInflight.remove(runId);
      return crashed;
    }
  }
}
