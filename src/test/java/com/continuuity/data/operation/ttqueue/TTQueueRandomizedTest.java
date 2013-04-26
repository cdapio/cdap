package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryOVCTable;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class TTQueueRandomizedTest {
  private static final Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules());
  private static final TransactionOracle oracle = injector.getInstance(TransactionOracle.class);

  private static final Logger LOG = LoggerFactory.getLogger(TTQueueRandomizedTest.class);

  private static final String HASH_KEY = "hash_key";

  @Test
  public void runRandomizedTest() throws Exception {
    for(int i=0; i<30; ++i) {
      LOG.info(String.format("**************************** Run %d started *************************************", i));
      testRandomized();
      LOG.info(String.format("**************************** Run %d done *************************************", i));
    }
  }

  public void testRandomized() throws Exception {
    CConfiguration cConfiguration = new CConfiguration();
    cConfiguration.setLong(TTQueueNewOnVCTable.TTQUEUE_EVICT_INTERVAL_SECS, 5);
    //cConfiguration.setInt(TTQueueNewOnVCTable.TTQUEUE_MAX_CRASH_DEQUEUE_TRIES, 4);
    // TODO: delete queue data in the end
    TTQueue ttQueue = createQueue(cConfiguration);

    TestConfig testConfig = new TestConfig(new RandomSelectionFunction());
//    TestConfig testConfig = new TestConfig(new DeterministicSelectorFunction());
    final int numProducers = testConfig.getNumProducers();
    LOG.info("Num producers=" + numProducers);
    final int numConsumerGroups = testConfig.getNumConsumerGroups();
    LOG.info("Num consumer groups=" + numConsumerGroups);
    final int numThreads = 50;
    LOG.info("Num threads=" + numThreads);
    ListeningExecutorService listeningExecutorService =
      MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numThreads));
    TestController testController = new TestController();

    ImmutableList.Builder<Integer> builder = ImmutableList.builder();
    final int numEnqueues = testConfig.getNumberEnqueues();
    LOG.info("Num enqueues=" + numEnqueues);
    for(int i = 0; i < numEnqueues; ++i) {
      builder.add(i);
    }
    List<Integer> inputList = builder.build();

    // Create producers
    List<ListenableFuture<?>> producerFutures = Lists.newArrayList();
    Queue<Integer> inputQueue = new ConcurrentLinkedQueue<Integer>(inputList);
    Map<Integer, List<Integer>> enqueuesMap = Maps.newConcurrentMap();
    Map<Integer, List<Integer>> invalidMap = Maps.newConcurrentMap();
    for(int i = 0; i < numProducers; ++i) {
      enqueuesMap.put(i, Lists.<Integer>newArrayList());
      invalidMap.put(i, Lists.<Integer>newArrayList());
      ListenableFuture<?> future = listeningExecutorService.submit(new Producer(i, testConfig, testController,
                                                                                ttQueue,
                                                                                inputQueue,
                                                                                enqueuesMap.get(i), invalidMap.get(i)));
      producerFutures.add(future);
    }

    // Start consumer groups
    List<ListenableFuture<?>> consumerGroupFutures = Lists.newArrayList();
    Map<Integer, Queue<Integer>> dequeueMap = Maps.newConcurrentMap();
    for(int i = 0; i < numConsumerGroups; ++i) {
      dequeueMap.put(i, new ConcurrentLinkedQueue<Integer>());
      ConsumerGroup consumerGroup = new ConsumerGroup(i, listeningExecutorService, testConfig, testController,
                                                      ttQueue, dequeueMap.get(i));
      ListenableFuture<?> future = listeningExecutorService.submit(consumerGroup);
      consumerGroupFutures.add(future);
    }


    // Start the producers and consumers
    LOG.info("Starting test...");
    testController.startTest();

    final Future<?> compositeEnqueueFuture = Futures.allAsList(producerFutures);
    LOG.info("Waiting for enqueues to complete...");
    compositeEnqueueFuture.get();
    LOG.info("Enqueues done.");
    testController.setEnqueueDoneTime(System.currentTimeMillis());

    final Future<?> compositeConsumerFuture = Futures.allAsList(consumerGroupFutures);
    compositeConsumerFuture.get();

    // Verify if all entries were enqueued
    List<Integer> actualEnqueued = Lists.newArrayList(Iterables.concat(enqueuesMap.values()));
    Collections.sort(actualEnqueued);
    List<Integer> actualInvalidated = Lists.newArrayList(Iterables.concat(invalidMap.values()));
    Collections.sort(actualInvalidated);
    List<Integer> actualProcessed = Lists.newArrayList(Iterables.concat(actualEnqueued, actualInvalidated));
    Collections.sort(actualProcessed);
    Assert.assertEquals(actualEnqueued.size(), inputList.size() - actualInvalidated.size());
    Assert.assertEquals(inputList, actualProcessed);

    for(int i = 0; i < numProducers; ++i) {
      LOG.info("Producer:" + i + " enqueueList=" + enqueuesMap.get(i));
      LOG.info("Producer:" + i + " invalidList=" + invalidMap.get(i));
    }

    for(Map.Entry<Integer, Queue<Integer>> group : dequeueMap.entrySet()) {
        List<Integer> dequeuedPerGroup = Lists.newArrayList(group.getValue());
        Collections.sort(dequeuedPerGroup);
        LOG.info(String.format("Group:%d dequeueList=%s", group.getKey(), dequeuedPerGroup));
    }

    LOG.info(String.format("Total entries=%d, Actual enqueued=%d, Invalidated=%d", inputList.size(),
                           actualEnqueued.size(), actualInvalidated.size()));

    // Verify only non-invalidated entries were dequeued
    for(Map.Entry<Integer, Queue<Integer>> group : dequeueMap.entrySet()) {
      List<Integer> actualDequeuedPerGroup = Lists.newArrayList(group.getValue());
      Collections.sort(actualDequeuedPerGroup);
      LOG.info(String.format("Verifying dequeues of group %d. Expected size=%d, actual size=%d", group.getKey(),
                             actualEnqueued.size(), actualDequeuedPerGroup.size()));
      Assert.assertEquals(actualEnqueued, actualDequeuedPerGroup);
    }
  }

  private TTQueue createQueue(CConfiguration conf) {
    return new TTQueueNewOnVCTable(
      new MemoryOVCTable(Bytes.toBytes("TestMemoryNewTTQueue")),
      Bytes.toBytes(this.getClass().getCanonicalName() + "-" + new Random(System.currentTimeMillis()).nextLong()),
      TestTTQueue.oracle, conf);
  }

  public interface SelectionFunction {
    int select(int value);
    boolean isProbable(float probability);
  }

  public static class RandomSelectionFunction implements SelectionFunction {
    private final Random random = new Random(System.currentTimeMillis());

    @Override
    public int select(int value) {
      return random.nextInt(value);
    }

    @Override
    public boolean isProbable(float probability) {
      return random.nextFloat() < probability;
    }
  }

  public static class DeterministicSelectorFunction implements SelectionFunction {
    @Override
    public int select(int value) {
      return value;
    }

    @Override
    public boolean isProbable(float probability) {
      return false;
    }
  }

  public static class TestConfig {
    private final SelectionFunction selectionFunction;

    public TestConfig(SelectionFunction selectionFunction) {
      this.selectionFunction = selectionFunction;
    }

    public int getNumProducers() {
      return selectionFunction.select(5) + 1;
    }

    public int getNumberEnqueues() {
      return selectionFunction.select(1000) + 100;
    }

    public int getEnqueueBatchSize() {
      return selectionFunction.select(50) + 1;
    }

    public int getEnqueueSleepMs() {
      return selectionFunction.select(100);
    }

    public boolean shouldInvalidate() {
      //return false;
      return selectionFunction.isProbable(0.1f);
    }

    public boolean shouldUnack() {
      return selectionFunction.isProbable(0.1f);
    }

    public int getNumConsumerGroups() {
      return selectionFunction.select(5) + 1;
    }

    public int getNumConsumers() {
      return selectionFunction.select(5) + 1;
    }

    public int getConsumerBatchSize() {
      return selectionFunction.select(50) + 1;
    }

    public boolean shouldBatchReturn() {
      return selectionFunction.isProbable(0.85f);
    }

    public int getDequeueSleepMs() {
      return selectionFunction.select(100);
    }

    public int getNumDequeueRuns() {
      return selectionFunction.select(10);
    }

    public boolean shouldFinalize() {
      return selectionFunction.isProbable(0.75f);
    }

    public QueuePartitioner.PartitionerType getPartitionType() {
      return QueuePartitioner.PartitionerType.values()[
        selectionFunction.select(QueuePartitioner.PartitionerType.values().length)
        ];
    }

    public boolean shouldConsumerCrash() {
      return selectionFunction.isProbable(0.15f);
    }

    public boolean shouldRunAsync() {
      return selectionFunction.isProbable(0.5f);
    }

    public int getAsyncDegree() {
      return selectionFunction.select(4) + 1;
    }
  }

  public static class Producer implements Runnable {
    private final int id;
    private final TestConfig testConfig;
    private final TestController testController;
    private final TTQueue queue;
    private final Queue<Integer> inputList;
    private final List<Integer> enqueuedList;
    private final List<Integer> invalidList;

    public Producer(int id, TestConfig testConfig, TestController testController, TTQueue queue,
                    Queue<Integer> inputList, List<Integer> enqueuedList, List<Integer> invalidList) {
      this.id = id;
      this.testConfig = testConfig;
      this.testController = testController;
      this.queue = queue;
      this.inputList = inputList;
      this.enqueuedList = enqueuedList;
      this.invalidList = invalidList;
    }

    @Override
    public void run() {
      try {
        testController.waitToStart();
        LOG.info(String.format("Producer:%d started", id));
        List<Integer> enqueueBatch = getNextEnqueueBatch();
        while(enqueueBatch != null) {
          Transaction transaction =  oracle.startTransaction();

          EnqueueResult result = queue.enqueue(getEnqueueEntries(enqueueBatch), transaction.getWriteVersion());
          TimeUnit.MILLISECONDS.sleep(testConfig.getEnqueueSleepMs());

          if(testConfig.shouldInvalidate()) {
            oracle.abortTransaction(transaction);
            queue.invalidate(result.getEntryPointers(), transaction.getWriteVersion());
            oracle.removeTransaction(transaction);
            invalidList.addAll(enqueueBatch);
            LOG.info(String.format("Producer:%d batchSize=%d invalidBatch=%s", id, enqueueBatch.size(), enqueueBatch));
          } else {
            oracle.commitTransaction(transaction);
            enqueuedList.addAll(enqueueBatch);
            LOG.info(String.format("Producer:%d batchSize=%d enqueueBatch=%s", id, enqueueBatch.size(), enqueueBatch));
          }

          TimeUnit.MILLISECONDS.sleep(testConfig.getEnqueueSleepMs());

          enqueueBatch = getNextEnqueueBatch();
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    private List<Integer> getNextEnqueueBatch() {
      int batchSize = testConfig.getEnqueueBatchSize();
      List<Integer> enqueueBatch = Lists.newArrayListWithExpectedSize(batchSize);
      for(int i = 0; i < batchSize; ++i) {
        Integer entry = inputList.poll();
        if(entry == null) {
          break;
        }
        enqueueBatch.add(entry);
      }
      if(enqueueBatch.isEmpty()) {
        // No more entries to enqueue, producer can be stopped
        return null;
      }
      return enqueueBatch;
    }

    private QueueEntry[] getEnqueueEntries(List<Integer> enqueueBatch) {
      return Iterables.toArray(Iterables.transform(enqueueBatch,
                                                   new Function<Integer, QueueEntry>() {
                                                     @Nullable
                                                     @Override
                                                     public QueueEntry apply(@Nullable Integer input) {
                                                       if(input == null) {
                                                         return null;
                                                       }
                                                       return new QueueEntry(
                                                         ImmutableMap.of(HASH_KEY, input + 2),
                                                         com.continuuity.api.common.Bytes.toBytes(input));
                                                     }
                                                   }),
                               QueueEntry.class);
    }
  }

  public static class TestController {
    private volatile CountDownLatch startLatch = new CountDownLatch(1);
    private volatile long enqueueDoneTime = 0;

    public void waitToStart() throws Exception {
      startLatch.await();
    }

    public void startTest() {
      startLatch.countDown();
    }

    public void setEnqueueDoneTime(long enqueueDoneTime) {
      this.enqueueDoneTime = enqueueDoneTime;
    }

    public boolean canDequeueStop() {
      // Wait for 1s after enqueue is done for things to settle down
      return enqueueDoneTime > 0 && System.currentTimeMillis() > enqueueDoneTime + 5000;
    }
  }

  public class ConsumerGroupControl {
    private final int id;
    private final int size;
    private final QueuePartitioner.PartitionerType partitionerType;
    private final int numDequeueRuns;
    private final CountDownLatch configureLatch;
    private final Set<Integer> consumersAtQueueEnd = Sets.newCopyOnWriteArraySet();

    public ConsumerGroupControl(int id, int size, QueuePartitioner.PartitionerType partitionerType,
                                int numDequeueRuns) {
      this.id = id;
      this.size = size;
      this.partitionerType = partitionerType;
      this.numDequeueRuns = numDequeueRuns;
      this.configureLatch = new CountDownLatch(size);
    }

    public int getId() {
      return id;
    }

    public int getSize() {
      return size;
    }

    public QueuePartitioner.PartitionerType getPartitionerType() {
      return partitionerType;
    }

    public int getNumDequeueRuns() {
      return numDequeueRuns;
    }

    public void setConsumersAtQueueEnd(int consumerId) {
      consumersAtQueueEnd.add(consumerId);
    }

    public Set<Integer> getConsumersAtQueueEnd() {
      return consumersAtQueueEnd;
    }

    public void doneSingleConfigure() {
      configureLatch.countDown();
    }

    public void waitForGroupConfigure() throws Exception {
      configureLatch.await();
    }
  }

  public class ConsumerGroup implements Runnable {
    private final int id;
    private final ListeningExecutorService listeningExecutorService;
    private final TestConfig testConfig;
    private final TestController testController;
    private final TTQueue queue;
    private final Queue<Integer> groupDequeueList;

    public ConsumerGroup(int id, ListeningExecutorService listeningExecutorService, TestConfig testConfig,
                         TestController testController, TTQueue queue, Queue<Integer> groupDequeueList) {
      this.id = id;
      this.listeningExecutorService = listeningExecutorService;
      this.testConfig = testConfig;
      this.testController = testController;
      this.queue = queue;
      this.groupDequeueList = groupDequeueList;
    }

    @Override
    public void run() {
      final QueuePartitioner.PartitionerType partitionerType = testConfig.getPartitionType();
      LOG.info(getLogMessage(String.format("Partition type=%s, batch", partitionerType)));

      int run = 0;
      try {
        while(true) {
          run++;
          // Create consumers
          final int numConsumers = testConfig.getNumConsumers();
          final int batchSize = testConfig.getConsumerBatchSize();
          final boolean batchReturn = testConfig.shouldBatchReturn();
          final boolean isAsync = testConfig.shouldRunAsync();
          QueueConfig config = new QueueConfig(partitionerType, !isAsync, batchSize, batchReturn);
          LOG.info(getLogMessage(String.format("Run=%d, Num consumers=%d, batchSize=%d, batchReturn=%s, isAsync=%s",
                                               run, numConsumers, batchSize, batchReturn, isAsync)));
          List<ListenableFuture<?>> consumerFutures = Lists.newArrayList();
          ConsumerGroupControl consumerGroupControl = new ConsumerGroupControl(id, numConsumers, partitionerType,
                                                                               testConfig.getNumDequeueRuns());
          Map<Integer, Queue<Integer>> groupMap = Maps.newConcurrentMap();
          for(int i = 0; i < numConsumers; ++i) {
            groupMap.put(i, new ConcurrentLinkedQueue<Integer>());
            ListenableFuture<?> future = listeningExecutorService.submit(
              new Consumer(i, consumerGroupControl, listeningExecutorService, testConfig, testController,
                           queue, config, groupMap.get(i)));
            consumerFutures.add(future);
          }

          // Wait for all consumers to complete
          final Future<?> compositeConsumerFuture = Futures.allAsList(consumerFutures);
          compositeConsumerFuture.get();

          Iterables.addAll(groupDequeueList, Iterables.concat(groupMap.values()));

  //        for(int i = 0; i < numConsumers; ++i) {
  //          LOG.info(getLogMessage("Consumer:" + i + " dequeueList=" + groupMap.get(i)));
  //        }

          if(consumerGroupControl.getConsumersAtQueueEnd().size() == numConsumers) {
            break;
          }
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    private String getLogMessage(String message) {
      return String.format("Consumer Group:%d, %s", id, message);
    }
  }

  public class Consumer implements Runnable {
    private final int id;
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

    public Consumer(int id, ConsumerGroupControl consumerGroupControl,
                    ListeningExecutorService listeningExecutorService, TestConfig testConfig,
                    TestController testController, TTQueue queue, QueueConfig queueConfig,
                    Queue<Integer> dequeueList) {
      this.id = id;
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
        testController.waitToStart();
        LOG.info(getLogMessage(String.format("groupSize:%d started", consumerGroupControl.getSize())));

        // Note: in this test we have a different batch size for each consumer
        final int maxAsyncDegree = queueConfig.isSingleEntry() ?  1 : testConfig.getAsyncDegree();
        LOG.info(getLogMessage(String.format("maxAsyncDegree=%d", maxAsyncDegree)));
        ConsumerHolder consumerHolder = new ConsumerHolder();
        consumerGroupControl.doneSingleConfigure();
        LOG.info(getLogMessage("Configure done, waiting for group configuration to be done..."));
        consumerGroupControl.waitForGroupConfigure();
        LOG.info(getLogMessage("Group configuration done, starting dequeues..."));

        int stopTries = 0;
        int runId = 0;
        while((stopTries < MAX_STOP_TRIES && dequeueRunsDone.get() < consumerGroupControl.getNumDequeueRuns()) ||
          asyncDegree.get() > 0) {
          ++runId;
          if(asyncDegree.get() < maxAsyncDegree && dequeueRunsDone.get() < consumerGroupControl.getNumDequeueRuns()) {
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
      return Iterables.transform(Arrays.asList(entries),
                                 new Function<QueueEntry, Integer>() {
                                   @Nullable
                                   @Override
                                   public Integer apply(@Nullable QueueEntry input) {
                                     if(input == null) {
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
                                                           "", HASH_KEY, queueConfig);
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
}
