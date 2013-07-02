package com.continuuity.data.operation.ttqueue.randomized;

import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.ttqueue.EnqueueResult;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.TTQueue;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
*
*/
public class Producer implements Runnable {
  private final int id;
  private final TransactionOracle oracle;
  private final TestConfig testConfig;
  private final TestController testController;
  private final TTQueue queue;
  private final Queue<Integer> inputList;
  private final List<Integer> enqueuedList;
  private final List<Integer> invalidList;

  private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

  public Producer(int id, TransactionOracle oracle, TestConfig testConfig, TestController testController,
                  TTQueue queue, Queue<Integer> inputList, List<Integer> enqueuedList, List<Integer> invalidList) {
    this.id = id;
    this.oracle = oracle;
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
      while (enqueueBatch != null) {
        final Transaction transaction =  oracle.startTransaction(true);

        EnqueueResult result = queue.enqueue(getEnqueueEntries(enqueueBatch), transaction);
        TimeUnit.MILLISECONDS.sleep(testConfig.getEnqueueSleepMs());

        if (testConfig.shouldInvalidate()) {
          oracle.abortTransaction(transaction);
          queue.invalidate(result.getEntryPointers(), transaction);
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
    for (int i = 0; i < batchSize; ++i) {
      Integer entry = inputList.poll();
      if (entry == null) {
        break;
      }
      enqueueBatch.add(entry);
    }
    if (enqueueBatch.isEmpty()) {
      // No more entries to enqueue, producer can be stopped
      return null;
    }
    return enqueueBatch;
  }

  private QueueEntry[] getEnqueueEntries(List<Integer> enqueueBatch) {
    return Iterables.toArray(Iterables.transform(enqueueBatch, new Function<Integer, QueueEntry>() {
      @Nullable
      @Override
      public QueueEntry apply(@Nullable Integer input) {
        if (input == null) {
          return null;
        }
        return new QueueEntry(ImmutableMap.of(TTQueueRandomizedTest.HASH_KEY, input + 2),
                              com.continuuity.api.common.Bytes.toBytes(input));
      }
    }), QueueEntry.class);
  }
}
