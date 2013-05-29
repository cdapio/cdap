package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryOVCTable;
import com.continuuity.data.operation.executor.Transaction;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestMemoryTTQueue extends TestTTQueue {

  @Override
  protected TTQueue createQueue(CConfiguration conf) {
    return new TTQueueOnVCTable(
        new MemoryOVCTable(Bytes.toBytes("TestMemoryTTQueue")),
        Bytes.toBytes("TestTTQueue"),
        TestTTQueue.oracle, conf);
  }

  @Override @Ignore
  public void testInjection() throws OperationException {
    // this test case uses MemoryOvcTable directly - no need to test it
  }

  @Override
  protected int getNumIterations() {
    return 250;
  }

  void enqueuOne(TTQueue queue, int i, Transaction transaction) throws OperationException {
    Assert.assertTrue("Enqueue failure!", queue.enqueue(new QueueEntry(Bytes.toBytes(i)), transaction).isSuccess());
  }

  void dequeueOne(TTQueue queue, QueueConsumer consumer, int numConsumers, int i, Transaction transaction)
    throws OperationException {
    DequeueResult result = queue.dequeue(consumer, transaction.getReadPointer());
    Assert.assertFalse("Dequeue returned empty!", result.isEmpty());
    Assert.assertArrayEquals(Bytes.toBytes(i), result.getEntry().getData());
    queue.ack(result.getEntryPointer(), consumer, transaction);
    queue.finalize(result.getEntryPointer(), consumer, numConsumers, transaction);
  }

  // this enqueues a number of entries and the dequeues them all, repeatedly, with a single consumer
  @Test @Ignore
  public void testSingleConsumerPlenty() throws Exception {
    TTQueue queue = createQueue();

    Transaction transaction = oracle.startTransaction(true);

    int rounds = 1000;
    int entriesPerRound = 10000;

    // we will dequeue with a single consumer and FIFO partitioning
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, new QueueConfig(QueuePartitioner.PartitionerType.FIFO, false));

    long nanoStart = System.nanoTime();
    for (int round = 0; round < rounds; round++) {
      System.out.println("Round no. "+round);

      // enqueue a whole lotta entries
      long nanoStartRound = System.nanoTime();
      for (int i = 0; i < entriesPerRound; i++) {
        enqueuOne(queue, i, transaction);
      }
      long nanoEnqueue = System.nanoTime();

      // dequeue, verify, ack and finalize all of them
      for (int i = 0; i < entriesPerRound; i++) {
        dequeueOne(queue, consumer, 1, i, transaction);
      }
      long nanoDequeue = System.nanoTime();

      long enqueueTime = nanoEnqueue - nanoStartRound;
      long dequeueTime = nanoDequeue - nanoEnqueue;
      long roundTime = nanoDequeue - nanoStartRound;
      long totalTime = nanoDequeue - nanoStart;

      double avgEnqueue = enqueueTime / entriesPerRound;
      double avgDequeue = dequeueTime / entriesPerRound;

      System.out.println("Total time in millis: " + (totalTime / 1000000));
      System.out.println("Round time in millis: " + (roundTime / 1000000));
      System.out.println("Single avg enqueue time in nano: " + avgEnqueue);
      System.out.println("Single avg dequeue time in nano: " + avgDequeue);

      // verify queue is empty now
      Assert.assertTrue("Queue should now be empty!", queue.dequeue(consumer, getDirtyPointer()).isEmpty());

      oracle.commitTransaction(transaction);
    }
  }

  // this enqueues a number of entries and the dequeues them all, repeatedly
  // it uses two consumers, and the second one is always one round behind, that is, eviction of entries
  // always happens with a delay of one round.
  @Test @Ignore
  public void testMultiConsumerPlenty() throws Exception {
    TTQueue queue = createQueue();

    Transaction transaction = oracle.startTransaction(true);

    int rounds = 1000;
    int entriesPerRound = 10000;

    // we will dequeue with a two consumers and FIFO partitioning
    QueueConsumer consumer1 = new QueueConsumer(0, 0, 1, new QueueConfig(QueuePartitioner.PartitionerType.FIFO, false));
    QueueConsumer consumer2 = new QueueConsumer(0, 1, 1, new QueueConfig(QueuePartitioner.PartitionerType.FIFO, false));

    long nanoStart = System.nanoTime();
    for (int round = 0; round < rounds; round++) {
      System.out.println("Round no. "+round);

      // enqueue a whole lotta entries
      long nanoStartRound = System.nanoTime();
      for (int i = 0; i < entriesPerRound; i++) {
        enqueuOne(queue, i, transaction);
      }
      long nanoEnqueue = System.nanoTime();

      // dequeue, verify, ack and finalize all of them
      for (int i = 0; i < entriesPerRound; i++) {
        dequeueOne(queue, consumer1, 2, i, transaction);
      }
      if (round != 0) {
        for (int i = 0; i < entriesPerRound; i++) {
          dequeueOne(queue, consumer2, 2, i, transaction);
        }
      }
      long nanoDequeue = System.nanoTime();

      long enqueueTime = nanoEnqueue - nanoStartRound;
      long dequeueTime = nanoDequeue - nanoEnqueue;
      long roundTime = nanoDequeue - nanoStartRound;
      long totalTime = nanoDequeue - nanoStart;

      double avgEnqueue = enqueueTime / entriesPerRound;
      double avgDequeue = dequeueTime / (entriesPerRound * (round == 0 ? 1 : 2));

      System.out.println("Total time in millis: " + (totalTime / 1000000));
      System.out.println("Round time in millis: " + (roundTime / 1000000));
      System.out.println("Single avg enqueue time in nano: " + avgEnqueue);
      System.out.println("Single avg dequeue time in nano: " + avgDequeue);

      // verify queue is empty now
      Assert.assertTrue("Queue should now be empty!", queue.dequeue(consumer1, getDirtyPointer()).isEmpty());

      oracle.commitTransaction(transaction);
    }
  }

}


