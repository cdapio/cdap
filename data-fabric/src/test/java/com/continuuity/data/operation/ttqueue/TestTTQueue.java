package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType.FIFO;
import static com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType.HASH;
import static com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType.ROUND_ROBIN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * tests TT queues.
 */
public abstract class TestTTQueue {

  private static final long MAX_TIMEOUT_MS = 10000;
  private static final long DEQUEUE_BLOCK_TIMEOUT_MS = 1;

  private static final Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules());
  protected static TransactionOracle oracle = injector.getInstance(TransactionOracle.class);
  protected static TimestampOracle timeOracle = injector.getInstance(TimestampOracle.class);

  protected TTQueue createQueue() throws OperationException {
    return createQueue(new CConfiguration());
  }

  protected abstract TTQueue createQueue(CConfiguration conf)
      throws OperationException;

  protected abstract int getNumIterations();

  // this must match the way that TTQueueOnVCTable creates dirty pointers!
  protected ReadPointer getDirtyPointer() {
    return TransactionOracle.DIRTY_READ_POINTER;
  }
  protected ReadPointer getCleanPointer() {
    return oracle.getReadPointer();
  }
  protected long getDirtyWriteVersion() {
    return TransactionOracle.DIRTY_WRITE_VERSION;
  }

  /**
   * Every subclass should implement this to verify that injection works and uses the correct table type.
   */
  @Test
  public abstract void testInjection() throws OperationException;


  @Test
  public void testLotsOfAsyncDequeueing() throws Exception {
    TTQueue queue = createQueue();

    long startTime = System.currentTimeMillis();

    int numEntries = getNumIterations();

    for (int i = 1; i < numEntries + 1; i++) {
      queue.enqueue(new QueueEntry(Bytes.toBytes(i)), new Transaction(getDirtyWriteVersion(), getDirtyPointer(), true));
    }
    System.out.println("Done enqueueing");

    long enqueueStop = System.currentTimeMillis();

    System.out.println("Finished enqueue of " + numEntries + " entries in " +
        (enqueueStop - startTime) + " ms (" +
        (enqueueStop - startTime) / ((float) numEntries) + " ms/entry)");

    QueueConfig configSync = new QueueConfig(PartitionerType.FIFO, true);
    StatefulQueueConsumer consumerSync = new StatefulQueueConsumer(0, 0, 1, configSync);
    queue.configure(consumerSync, getDirtyPointer());
    for (int i = 1; i < numEntries + 1; i++) {
      MemoryReadPointer rp = new MemoryReadPointer(timeOracle.getTimestamp());
      Transaction transaction = new Transaction(rp.getMaximum(), rp, true);
      DequeueResult result = queue.dequeue(consumerSync, rp);
      assertTrue(result.isSuccess());
      //assertTrue(Bytes.equals(Bytes.toBytes(i), result.getEntry().getData()));
      assertEquals(i, Bytes.toInt(result.getEntry().getData()));
      queue.ack(result.getEntryPointer(), consumerSync, transaction);
//      queue.finalize(result.getEntryPointer(), consumerSync, -1, transaction);
      if (i % 100 == 0) {
        System.out.print(".");
      }
      if (i % 1000 == 0) {
        System.out.println(" " + i);
      }
    }

    long dequeueSyncStop = System.currentTimeMillis();

    System.out.println("Finished sync dequeue of " + numEntries + " entries in " +
        (dequeueSyncStop - enqueueStop) + " ms (" +
        (dequeueSyncStop - enqueueStop) / ((float) numEntries) + " ms/entry)");

    // Async

    QueueConfig configAsync = new QueueConfig(PartitionerType.FIFO, false);
    StatefulQueueConsumer consumerAsync = new StatefulQueueConsumer(0, 2, 1, configAsync);
    queue.configure(consumerAsync, getDirtyPointer());
    for (int i = 1; i < numEntries + 1; i++) {
      DequeueResult result =
          queue.dequeue(consumerAsync, new MemoryReadPointer(timeOracle.getTimestamp()));
      assertTrue(result.isSuccess());
      assertTrue("Expected " + i + ", Actual " + Bytes.toInt(result.getEntry().getData()),
          Bytes.equals(Bytes.toBytes(i), result.getEntry().getData()));
      if (i % 100 == 0) {
        System.out.print(".");
      }
      if (i % 1000 == 0) {
        System.out.println(" " + i);
      }
    }

    long dequeueAsyncStop = System.currentTimeMillis();

    System.out.println("Finished async dequeue of " + numEntries + " entries in " +
        (dequeueAsyncStop - dequeueSyncStop) + " ms (" +
        (dequeueAsyncStop - dequeueSyncStop) / ((float) numEntries) + " ms/entry)");

    // Both queues should be empty for each consumer
    assertTrue(queue.dequeue(consumerSync, new MemoryReadPointer(timeOracle.getTimestamp())).isEmpty());
    assertTrue(queue.dequeue(consumerAsync, new MemoryReadPointer(timeOracle.getTimestamp())).isEmpty());
  }

  @Test
  public void testGroupIdGen() throws Exception {
    TTQueue queue = createQueue();
    int n = 1024;
    Set<Long> groupids = new HashSet<Long>(n);
    for (int i = 0; i < n; i++) {
      long groupid = queue.getGroupID();
      assertFalse(groupids.contains(groupid));
      groupids.add(groupid);
    }
  }

  @Test
  public void testSingleConsumerSimple() throws Exception {
    final boolean singleEntry = true;
    TTQueue queue = createQueue();
    long dirtyVersion = getDirtyWriteVersion();
    ReadPointer dirtyReadPointer = getDirtyPointer();
    Transaction dirtyTxn = new Transaction(dirtyVersion, dirtyReadPointer, true);

    QueueConfig config = new QueueConfig(PartitionerType.FIFO, singleEntry);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);
    queue.configure(consumer, dirtyReadPointer);

    for (int i = 0; i < 2; ++i) {
      byte [] valueOne = Bytes.toBytes("value" + i + "-1");
      byte [] valueTwo = Bytes.toBytes("value" + i + "-2");

      // enqueue two entries
      assertTrue(queue.enqueue(new QueueEntry(valueOne), dirtyTxn).isSuccess());
      assertTrue(queue.enqueue(new QueueEntry(valueTwo), dirtyTxn).isSuccess());

      // dequeue it with the single consumer and FIFO partitioner
      DequeueResult result = queue.dequeue(consumer, dirtyReadPointer);

      // verify we got something and it's the first value
      assertTrue(result.toString(), result.isSuccess());
  //    assertTrue(Bytes.equals(result.getValue(), valueOne));
      assertEquals(Bytes.toString(valueOne), Bytes.toString(result.getEntry().getData()));

      // dequeue again without acking, should still get first value
      result = queue.dequeue(consumer, dirtyReadPointer);
      assertTrue(result.isSuccess());
      assertTrue("Expected (" + Bytes.toString(valueOne) + ") Got (" +
          Bytes.toString(result.getEntry().getData()) + ")",
          Bytes.equals(result.getEntry().getData(), valueOne));

      // ack
      queue.ack(result.getEntryPointer(), consumer, dirtyTxn);
//      queue.finalize(result.getEntryPointer(), consumer, -1, dirtyTxn);

      // dequeue, should get second value
      result = queue.dequeue(consumer, dirtyReadPointer);
      assertTrue(result.isSuccess());
//      assertTrue(Bytes.equals(result.getEntry().getData(), valueTwo));
      assertEquals(Bytes.toString(valueTwo), Bytes.toString(result.getEntry().getData()));

      // ack
      queue.ack(result.getEntryPointer(), consumer, dirtyTxn);
//      queue.finalize(result.getEntryPointer(), consumer, -1, dirtyTxn);

      // verify queue is empty
      result = queue.dequeue(consumer, dirtyReadPointer);
      assertTrue(result.isEmpty());
    }
  }

  @Test
  public void testSingleConsumerMulti() throws Exception {
    TTQueue queue = createQueue();
    ReadPointer readPointer = getCleanPointer();
    long version = readPointer.getMaximum();
    Transaction transaction = new Transaction(version, readPointer, true);

    // enqueue ten entries
    int n = 10;
    for (int i = 0; i < n; i++) {
      assertTrue(queue.enqueue(new QueueEntry(Bytes.toBytes(i + 1)), transaction).isSuccess());
    }

    QueueConfig config = new QueueConfig(PartitionerType.FIFO, false);
    // dequeue it with the single consumer and FIFO partitioner
    StatefulQueueConsumer consumer = new StatefulQueueConsumer(0, 0, 1, config);
    queue.configure(consumer, getDirtyPointer());

    // verify it's the first value
    DequeueResult resultOne = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultOne.getEntry().getData(), Bytes.toBytes(1)));

    // dequeue again without acking, async mode should get the second value
    DequeueResult resultTwo = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultTwo.getEntry().getData(), Bytes.toBytes(2)));

    // ack result two
    queue.ack(resultTwo.getEntryPointer(), consumer, transaction);
//    queue.finalize(resultTwo.getEntryPointer(), consumer, -1, transaction);

    // dequeue, should get third value
    DequeueResult resultThree = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultThree.getEntry().getData(), Bytes.toBytes(3)));

    // ack
    queue.ack(resultThree.getEntryPointer(), consumer, transaction);
//    queue.finalize(resultThree.getEntryPointer(), consumer, -1, transaction);

    // dequeue fourth
    DequeueResult resultFour = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultFour.getEntry().getData(), Bytes.toBytes(4)));

    // dequeue five through ten, and ack them
    for (int i = 5; i < 11; i++) {
      DequeueResult result = queue.dequeue(consumer, readPointer);
      assertTrue(Bytes.equals(result.getEntry().getData(), Bytes.toBytes(i)));
      queue.ack(result.getEntryPointer(), consumer, transaction);
//      queue.finalize(result.getEntryPointer(), consumer, -1, transaction);
    }

    // verify queue is empty (first and fourth not ackd but still dequeued)
    assertTrue(queue.dequeue(consumer, readPointer).isEmpty());

    // second and third ackd, another ack should fail
    // second and third ackd, another ack should fail
    try {
      queue.ack(resultTwo.getEntryPointer(), consumer, transaction);
      fail("ack should fail.");
    } catch (OperationException e) {
      // expected
    }
    try {
      queue.ack(resultThree.getEntryPointer(), consumer, transaction);
      fail("ack should fail.");
    } catch (OperationException e) {
      // expected
    }

    // first and fourth are not acked, ack should pass
    queue.ack(resultOne.getEntryPointer(), consumer, transaction);
//    queue.finalize(resultOne.getEntryPointer(), consumer, -1, transaction);
    queue.ack(resultFour.getEntryPointer(), consumer, transaction);
//    queue.finalize(resultFour.getEntryPointer(), consumer, -1, transaction);

    // queue still empty
    assertTrue(queue.dequeue(consumer, readPointer).isEmpty());
  }

  @Test
  public void testSingleConsumerThreaded() throws Exception {
    TTQueue queue = createQueue();
    ReadPointer readPointer = getCleanPointer();
    long version = readPointer.getMaximum();
    Transaction transaction = new Transaction(version, readPointer, true);

    byte [] valueOne = Bytes.toBytes("value1");
    byte [] valueTwo = Bytes.toBytes("value2");


    // dequeue it with the single consumer and FIFO partitioner
    QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);
    queue.configure(consumer, getDirtyPointer());

    // spawn a thread to dequeue
    QueueDequeuer dequeuer = new QueueDequeuer(queue, consumer, readPointer);
    dequeuer.start();

    // dequeuer should be empty
    assertNull(dequeuer.nonBlockDequeue());

    // trigger a dequeue
    assertTrue(dequeuer.triggerdequeue());
    waitForAndAssertCount(1, dequeuer.dequeueRunLoop);
    waitForAndAssertCount(0, dequeuer.dequeues);

    // nothing in queue so dequeuer should be empty
    DequeueResult result = dequeuer.blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
    assertNull(result);

    // enqueue
    assertTrue(queue.enqueue(new QueueEntry(valueOne), transaction).isSuccess());
    waitForAndAssertCount(1, dequeuer.dequeues);
    waitForAndAssertCount(2, dequeuer.dequeueRunLoop);

    // dequeuer will dequeue, but we should still be able to dequeue!
    result = queue.dequeue(consumer, readPointer);
    assertNotNull(result);
    assertTrue(Bytes.equals(result.getEntry().getData(), valueOne));

    // dequeuer should also have this loaded
    result = dequeuer.blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
    assertNotNull(result);
    assertTrue(Bytes.equals(result.getEntry().getData(), valueOne));

    // ack it!
    queue.ack(result.getEntryPointer(), consumer, transaction);
//    queue.finalize(result.getEntryPointer(), consumer, -1, transaction);

    // trigger another dequeue
    assertTrue(dequeuer.triggerdequeue());
    waitForAndAssertCount(1, dequeuer.dequeues);
    // dequeuer had dequeueped and goes into its next loop
    waitForAndAssertCount(2, dequeuer.dequeueRunLoop);

    // nothing in queue so dequeuer should be empty
    result = dequeuer.blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
    assertNull(result);

    // enqueue
    assertTrue(queue.enqueue(new QueueEntry(valueTwo), transaction).isSuccess());
    waitForAndAssertCount(2, dequeuer.dequeues);
    // dequeuer had dequeueped and goes into its next loop
    waitForAndAssertCount(3, dequeuer.dequeueRunLoop);

    // dequeuer should have value2
    result = dequeuer.blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
    assertNotNull(result);
    assertTrue(Bytes.equals(result.getEntry().getData(), valueTwo));

    // trigger dequeuer again, should get the same one back
    assertTrue(dequeuer.triggerdequeue());
    waitForAndAssertCount(3, dequeuer.dequeues);
    // dequeuer had dequeueped and goes into its next loop
    waitForAndAssertCount(4, dequeuer.dequeueRunLoop);
    result = dequeuer.blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
    assertNotNull(result);
    assertTrue(Bytes.equals(result.getEntry().getData(), valueTwo));

    // ack it!
    queue.ack(result.getEntryPointer(), consumer, transaction);
//    queue.finalize(result.getEntryPointer(), consumer, -1, transaction);

    // verify queue is empty
    assertTrue(queue.dequeue(consumer, readPointer).isEmpty());

    // shut down
    dequeuer.shutdown();
  }

  @Test
  public void testConcurrentEnqueueDequeue() throws Exception {
    final TTQueue queue = createQueue();
    final ReadPointer readPointer = getCleanPointer();
    final long version = readPointer.getMaximum();
    final Transaction transaction = new Transaction(version, readPointer, true);

    AtomicLong dequeueReturns = null;
    if (queue instanceof TTQueueOnVCTable) {
      dequeueReturns = ((TTQueueOnVCTable) queue).dequeueReturns;
    }

    assertNotNull(dequeueReturns);

    final int n = getNumIterations();

    // Create and start a thread that dequeues in a loop
    final QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    final QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);
    queue.configure(consumer, getDirtyPointer());
    final AtomicBoolean stop = new AtomicBoolean(false);
    final Set<byte[]> dequeued = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    final AtomicLong numEmpty = new AtomicLong(0);
    Thread dequeueThread = new Thread() {
      @Override
      public void run() {
        boolean lastSuccess = false;
        while (lastSuccess || !stop.get()) {
          DequeueResult result;
          try {
            result = queue.dequeue(consumer, readPointer);
          } catch (OperationException e) {
            System.out.println("DequeuePayload failed! " + e.getMessage());
            return;
          }
          if (result.isSuccess()) {
            dequeued.add(result.getEntry().getData());
            try {
              queue.ack(result.getEntryPointer(), consumer, transaction);
//              queue.finalize(result.getEntryPointer(), consumer, -1, transaction);
            } catch (OperationException e) {
              fail("Queue ack or finalize failed: " + e.getMessage());
            }
            lastSuccess = true;
          } else {
            lastSuccess = false;
            numEmpty.incrementAndGet();
          }
        }
      }
    };
    dequeueThread.start();

    // After 10ms, should still have zero entries
    assertEquals(0, dequeued.size());

    // Start an enqueueThread to enqueue N entries
    Thread enqueueThread = new Thread() {
      @Override
      public void run() {
        for (int i = 0; i < n; i++) {
          try {
            queue.enqueue(new QueueEntry(Bytes.toBytes(i)), transaction);
          } catch (OperationException e) {
            fail("EnqueuePayload got exception: " + e.getMessage());
          }
        }
      }
    };
    enqueueThread.start();

    // Join the enqueuer
    enqueueThread.join();

    // Tell the dequeuer to stop (once he gets an empty)
    Thread.sleep(200);
    stop.set(true);
    dequeueThread.join();
    System.out.println("DequeueThread is done.  Set size is " +
        dequeued.size() + ", Number of empty returns is " + numEmpty.get());

    // Should have dequeued n entries
    assertEquals(n, dequeued.size());

    // And dequeuedEntries should be >= n
    assertTrue("Expected dequeued >= n (dequeued=" + dequeueReturns.get() +
        ") (n=" + n + ")", n <= dequeueReturns.get());
  }

  // Note: This does not work with round/robin nor fifo - can't guarantee that entry#i has value i
  @Test
  public void testMultiConsumerMultiGroup() throws Exception {
    final String hashKey = "hkey";
    TTQueue queue = createQueue();
    ReadPointer readPointer = getCleanPointer();
    long version = readPointer.getMaximum();
    Transaction transaction = new Transaction(version, readPointer, true);

    AtomicLong dequeueReturns = null;
    if (queue instanceof TTQueueOnVCTable) {
      dequeueReturns = ((TTQueueOnVCTable) queue).dequeueReturns;
    }

    assertNotNull(dequeueReturns);

    // Create 4 consumer groups with 4 consumers each
    int n = 4;
    QueueConsumer [][] consumers = new QueueConsumer[n][];
    for (int i = 0; i < n; i++) {
      consumers[i] = new QueueConsumer[n];
      for (int j = 0; j < n; j++) {
        consumers[i][j] = new StatefulQueueConsumer(j, i, n, "g" + i, hashKey,
                                                    new QueueConfig(PartitionerType.HASH, true));
        queue.configure(consumers[i][j], readPointer);
      }
    }

    // values are longs
    byte [][] values = new byte[n * n][];
    for (int i = 0; i < n * n; i++) {
      values[i] = Bytes.toBytes((long) i);
    }


    // Start a dequeuer for every consumer!
    QueueDequeuer [][] dequeuers = new QueueDequeuer[n][];
    for (int i = 0; i < n; i++) {
      dequeuers[i] = new QueueDequeuer[n];
      for (int j = 0; j < n; j++) {
        dequeuers[i][j] = new QueueDequeuer(queue, consumers[i][j], readPointer);
        dequeuers[i][j].start();
        assertTrue(dequeuers[i][j].triggerdequeue());
      }
    }

    // No queue dequeue returns yet
    long expectedQueuedequeues = 0;
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // verify everyone is empty
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        assertNull(dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS));
      }
    }

    // no dequeues yet
    long numdequeues = 0L;
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        assertEquals(numdequeues, dequeuers[i][j].dequeues.get());
      }
    }

    @SuppressWarnings("unused")
    long numEnqueues = 0;
    // enqueue the first four values
    for (int i = 0; i < n; i++) {
      assertTrue(queue.enqueue(new QueueEntry(ImmutableMap.of(hashKey, i), values[i]), transaction).isSuccess());
      numEnqueues++;
    }

    // wait for n^2 more queuedequeue() returns
    expectedQueuedequeues += (n * n);
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // every dequeuer/consumer should have one result
    numdequeues++;
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        waitForAndAssertCount(numdequeues, dequeuers[i][j].dequeues);
        DequeueResult result = dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
        assertNotNull(result);
        assertEquals(j, Bytes.toLong(result.getEntry().getData()));
      }
    }

    // trigger dequeues again, should get the same entries
    numdequeues++;
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        assertTrue(dequeuers[i][j].triggerdequeue());
        waitForAndAssertCount(numdequeues, dequeuers[i][j].dequeues);
      }
    }

    // wait for 16 more queuedequeue() returns
    expectedQueuedequeues += (n * n);
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // every dequeuer/consumer should have one result
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        DequeueResult result = dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
        assertNotNull(result);
        assertEquals(j, Bytes.toLong(result.getEntry().getData()));
      }
    }

    // directly dequeueping should also yield the same result
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        DequeueResult result = queue.dequeue(consumers[i][j], readPointer);
        assertNotNull(result);
        assertTrue(result.isSuccess());
        assertEquals(j, Bytes.toLong(result.getEntry().getData()));
      }
    }

    // wait for 16 more queuedequeue() returns
    expectedQueuedequeues += (n * n);
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // ack it for groups(0,1) consumers(2,3)
    for (int i = 0; i < n / 2; i++) {
      for (int j = n / 2; j < n; j++) {
        DequeueResult result = queue.dequeue(consumers[i][j], readPointer);
        assertNotNull(result);
        assertEquals(j, Bytes.toLong(result.getEntry().getData()));
        queue.ack(result.getEntryPointer(), consumers[i][j], transaction);
//        queue.finalize(result.getEntryPointer(), consumers[i][j], -1, transaction);
        System.out.println("ACK: i=" + i + ", j=" + j);
      }
    }

    // wait for n more queuedequeue() returns
    expectedQueuedequeues += n;
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // trigger dequeuers
    numdequeues++;
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        waitForAndAssertCount(numdequeues, dequeuers[i][j].dequeueRunLoop);
        assertTrue(dequeuers[i][j].triggerdequeue());
        waitForAndAssertCount(numdequeues, dequeuers[i][j].triggers);
      }
    }

    // wait for (n-1)(n) more queuedequeue() returns
    expectedQueuedequeues += (n * (n - 1));
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // expect null for groups(0,1) consumers(2,3), same value for others
    // ack everyone not ackd
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        if ((i == 0 || i == 1) && (j == 2 || j == 3)) {
          assertNull(dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS));
        } else {
          System.out.println("dequeue: i=" + i + ", j=" + j);
          DequeueResult result = queue.dequeue(consumers[i][j], readPointer);
          assertNotNull(result);
          assertEquals(j, Bytes.toLong(result.getEntry().getData()));
          queue.ack(result.getEntryPointer(), consumers[i][j], transaction);
//          queue.finalize(result.getEntryPointer(), consumers[i][j], -1, transaction);
          // buffer of dequeuer should still have that result
          waitForAndAssertCount(numdequeues, dequeuers[i][j].dequeues);
          result = dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
          assertNotNull(result);
          assertEquals(j, Bytes.toLong(result.getEntry().getData()));
        }
      }
    }

    // wait for (n*(n-1)) more queuedequeue() returns
    expectedQueuedequeues += (n * (n - 1));
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // trigger dequeues again, will get false for groups(0,1) consumers(2,3)
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        boolean dequeueStatus = dequeuers[i][j].triggerdequeue();
        if ((i == 0 || i == 1) && (j == 2 || j == 3)) {
          assertFalse(dequeueStatus);
        } else {
          if (!dequeueStatus) {
            System.out.println("Failed Trigger dequeue {i=" +
                                 i + ", j=" + j + "}");
          }
          assertTrue("Expected to be able to trigger a dequeue but was not {i=" +
              i + ", j=" + j + "}", dequeueStatus);
        }
      }
    }
    // re-align counters so we can keep testing sanely
    // for groups(0,1) consumers(2,3)
    for (int i = 0; i < n / 2; i++) {
      for (int j = n / 2; j < n; j++) {
        dequeuers[i][j].dequeues.incrementAndGet();
      }
    }

    // everyone should be empty
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        assertNull(dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS));
      }
    }

    // enqueue everything!
    for (int i = 0; i < n * n; i++) {
      assertTrue(queue.enqueue(new QueueEntry(ImmutableMap.of(hashKey, i), values[i]), transaction).isSuccess());
      numEnqueues++;
    }

    // wait for n^2 more queuedequeue() wake-ups
    expectedQueuedequeues += (n * n);
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);
    numdequeues++;

    // dequeue and ack everything.  each consumer should have 4 things!
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        long localdequeues = numdequeues;
        for (int k = 0; k < n; k++) {
          waitForAndAssertCount(localdequeues, dequeuers[i][j].dequeues);
          DequeueResult result = dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
          assertNotNull(result);
          assertEquals("i=" + i + ", j=" + j + ", k=" + k + ", threadid=" +
              dequeuers[i][j].getId(),
              (long) (k * n) + j, Bytes.toLong(result.getEntry().getData()));
          queue.ack(result.getEntryPointer(), consumers[i][j], transaction);
//          queue.finalize(result.getEntryPointer(), consumers[i][j], -1, transaction);
          dequeuers[i][j].triggerdequeue();
          localdequeues++;
        }
      }
    }

    // everyone should be empty
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        assertNull(dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS));
      }
    }

    // everyone should be empty
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        dequeuers[i][j].shutdown();
      }
    }

    // check on the queue meta
    /* temporarily disable queue meta check during hbase native transition
    QueueMeta meta = queue.getQueueInfo();
    assertEquals(numEnqueues, meta.currentWritePointer);
    assertEquals(numEnqueues, meta.globalHeadPointer);
    assertEquals(n, meta.groups.length);
     */
  }

  private void waitForAndAssertCount(long expected, AtomicLong wakeUps) {
    long start = System.currentTimeMillis();
    long end = start + MAX_TIMEOUT_MS;
    while (expected > wakeUps.get() &&
        System.currentTimeMillis() < end) {
      try {
        Thread.sleep(DEQUEUE_BLOCK_TIMEOUT_MS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals("Exiting waitForWakeUps (expected=" + expected + ", " +
        "actual=" + wakeUps.get(), expected, wakeUps.get());
  }

  class QueueDequeuer extends Thread {

    private final TTQueue queue;
    private final QueueConsumer consumer;
    private final ReadPointer readPointer;

    private final AtomicBoolean dequeueTrigger = new AtomicBoolean(false);

    AtomicLong triggers = new AtomicLong(0);
    AtomicLong dequeues = new AtomicLong(0);
    AtomicLong dequeueRunLoop = new AtomicLong(0);

    private DequeueResult result;

    private boolean keepgoing = true;

    QueueDequeuer(TTQueue queue, QueueConsumer consumer, ReadPointer readPointer) {
      this.queue = queue;
      this.consumer = consumer;
      this.readPointer = readPointer;
      this.result = null;
    }

    DequeueResult nonBlockDequeue() {
      if (this.result == null) {
        return null;
      }
      DequeueResult ret = this.result;
      this.result = null;
      return ret;
    }

    DequeueResult blockdequeue(long timeout) {
      DequeueResult result = nonBlockDequeue();
      if (result != null) {
        return result;
      }
      long cur = System.currentTimeMillis();
      long end = cur + timeout;
      while (result == null && cur < end) {
        synchronized (this.dequeueTrigger) {
          try {
            this.dequeueTrigger.wait(timeout);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        result = nonBlockDequeue();
        cur = System.currentTimeMillis();
      }
      return result;
    }

    /**
     * trigger the thread to dequeue.
     * @return true if a dequeue was triggered, false if not
     */
    boolean triggerdequeue() {
      synchronized (this.dequeueTrigger) {
        if (this.dequeueTrigger.get()) {
          return false;
        }
        this.result = null;
        this.dequeueTrigger.set(true);
        this.dequeueTrigger.notifyAll();
        return true;
      }
    }

    public void shutdown() {
      this.keepgoing = false;
      interrupt();
    }

    @Override
    public void run() {
      while (this.keepgoing) {
        synchronized (this.dequeueTrigger) {
          this.dequeueRunLoop.incrementAndGet();
          while (!this.dequeueTrigger.get() && this.keepgoing) {
            try {
              this.dequeueTrigger.wait();
            } catch (InterruptedException e) {
              //noinspection ConstantConditions
              if (this.keepgoing) {
                e.printStackTrace();
              }
            }
          }
          this.triggers.incrementAndGet();
        }
        DequeueResult result = null;
        while ((result == null || result.isEmpty()) && this.keepgoing) {
          try {
            result = this.queue.dequeue(this.consumer, this.readPointer);
          } catch (OperationException e) {
            fail("DequeuePayload failed with exception: " + e.getMessage());
          }
        }
        this.dequeueTrigger.set(false);
        this.result = result;
        this.dequeues.incrementAndGet();
      }
    }
  } // end of class QueueDequeuer

  private static final int MAX_CRASH_DEQUEUE_TRIES = 10;

  protected void updateCConfiguration(CConfiguration conf) {
    // Setting evict interval to be high -ve number of seconds for testing,
    // so that evictions can be asserted immediately in tests.
    conf.setLong(TTQueueOnVCTable.TTQUEUE_EVICT_INTERVAL_SECS, Long.MIN_VALUE);
    conf.setInt(TTQueueOnVCTable.TTQUEUE_MAX_CRASH_DEQUEUE_TRIES, MAX_CRASH_DEQUEUE_TRIES);
  }

  // Test DequeueEntry
  @Test
  public void testDequeueEntryEncode() throws Exception {
    TTQueueOnVCTable.DequeueEntry expectedEntry = new TTQueueOnVCTable.DequeueEntry(1, 2);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    expectedEntry.encode(new BinaryEncoder(bos));
    byte[] encodedValue = bos.toByteArray();

    TTQueueOnVCTable.DequeueEntry actualEntry = TTQueueOnVCTable.DequeueEntry.decode(
      new BinaryDecoder(new ByteArrayInputStream(encodedValue)));
    assertEquals(expectedEntry.getEntryId(), actualEntry.getEntryId());
    assertEquals(expectedEntry.getTries(), actualEntry.getTries());
    assertEquals(expectedEntry, actualEntry);
  }

  @Test
  public void testDequeueEntryEquals() throws Exception {
    // DequeueEntry is only compared using entryId, tries is ignored
    assertEquals(new TTQueueOnVCTable.DequeueEntry(12, 10), new TTQueueOnVCTable.DequeueEntry(12));
    assertEquals(new TTQueueOnVCTable.DequeueEntry(12, 10), new TTQueueOnVCTable.DequeueEntry(12, 10));
    assertEquals(new TTQueueOnVCTable.DequeueEntry(12, 10), new TTQueueOnVCTable.DequeueEntry(12, 15));

    assertNotEquals(new TTQueueOnVCTable.DequeueEntry(12, 10), new TTQueueOnVCTable.DequeueEntry(13, 10));
    assertNotEquals(new TTQueueOnVCTable.DequeueEntry(12, 10), new TTQueueOnVCTable.DequeueEntry(13, 11));
  }

  @Test
  public void testDequeueEntryCompare() throws Exception {
    // DequeueEntry is compared only on entryId, tries is ignored
    SortedSet<TTQueueOnVCTable.DequeueEntry> sortedSet = new TreeSet<TTQueueOnVCTable.DequeueEntry>();
    sortedSet.add(new TTQueueOnVCTable.DequeueEntry(5, 1));
    sortedSet.add(new TTQueueOnVCTable.DequeueEntry(2, 3));
    sortedSet.add(new TTQueueOnVCTable.DequeueEntry(1, 2));
    sortedSet.add(new TTQueueOnVCTable.DequeueEntry(3, 2));
    sortedSet.add(new TTQueueOnVCTable.DequeueEntry(4, 3));
    sortedSet.add(new TTQueueOnVCTable.DequeueEntry(4, 5));
    sortedSet.add(new TTQueueOnVCTable.DequeueEntry(0, 2));

    int expected = 0;
    for (TTQueueOnVCTable.DequeueEntry aSortedSet : sortedSet) {
      assertEquals(expected++, aSortedSet.getEntryId());
    }
  }

  @Test
  public void testQueueEntrySetEncode() throws Exception {
    final int max = 10;
    TTQueueOnVCTable.DequeuedEntrySet expectedEntrySet = new TTQueueOnVCTable.DequeuedEntrySet();
    for (int i = 0; i < max; ++i) {
      expectedEntrySet.add(new TTQueueOnVCTable.DequeueEntry(i, i % 2));
    }

    assertEquals(max, expectedEntrySet.size());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    expectedEntrySet.encode(new BinaryEncoder(bos));
    byte[] encodedValue = bos.toByteArray();

    TTQueueOnVCTable.DequeuedEntrySet actualEntrySet = TTQueueOnVCTable.DequeuedEntrySet.decode(new BinaryDecoder(
      new ByteArrayInputStream(encodedValue)));
    assertEquals(expectedEntrySet.size(), actualEntrySet.size());
    for (int i = 0; i < max; ++i) {
      TTQueueOnVCTable.DequeueEntry expectedEntry = expectedEntrySet.min();
      expectedEntrySet.remove(expectedEntry.getEntryId());

      TTQueueOnVCTable.DequeueEntry actualEntry = actualEntrySet.min();
      actualEntrySet.remove(actualEntry.getEntryId());

      assertEquals(expectedEntry.getEntryId(), actualEntry.getEntryId());
      assertEquals(expectedEntry.getTries(), actualEntry.getTries());
    }
  }

  @Test
  public void testDequeueEntrySet() throws Exception {
    final int max = 10;
    TTQueueOnVCTable.DequeuedEntrySet entrySet = new TTQueueOnVCTable.DequeuedEntrySet();
    List<Long> expectedEntryIds = Lists.newArrayListWithCapacity(max);
    List<TTQueueOnVCTable.DequeueEntry> expectedEntryList = Lists.newArrayListWithCapacity(max);
    Set<Long> expectedDroppedEntries = Sets.newHashSetWithExpectedSize(max);

    for (int i = 0; i < max; ++i) {
      entrySet.add(new TTQueueOnVCTable.DequeueEntry(i, i % 2));
      expectedEntryIds.add((long) i);
      expectedEntryList.add(new TTQueueOnVCTable.DequeueEntry(i, i % 2));

      if (i % 2 == 1) {
        expectedDroppedEntries.add((long) i);
      }
    }

    // Verify created lists
    assertEquals(max, entrySet.size());
    assertEquals(max, expectedEntryIds.size());
    assertEquals(max, expectedEntryList.size());
    assertEquals(max / 2, expectedDroppedEntries.size());

    // Verify QueueEntrySet
    assertEquals(expectedEntryIds, entrySet.getEntryIds());
    assertEquals(expectedEntryList, entrySet.getEntryList());

    Set<Long> actualDroppedEntries = entrySet.startNewTry(1);
    assertEquals(expectedDroppedEntries, actualDroppedEntries);

    List<Long> actualRemainingEntries = Lists.newArrayListWithCapacity(max);
    for (int i = 0; i < max; ++i) {
      if (i % 2 == 0) {
        actualRemainingEntries.add((long) i);
      }
    }
    assertEquals(actualRemainingEntries, entrySet.getEntryIds());
  }

  @Test
  public void testWorkingEntryList() {
    final int max = 10;
    Map<Long, byte[]> noCachedEntries = Collections.emptyMap();
    TTQueueOnVCTable.TransientWorkingSet transientWorkingSet =
      new TTQueueOnVCTable.TransientWorkingSet(Lists.<Long>newArrayList(), noCachedEntries);
    assertFalse(transientWorkingSet.hasNext());

    List<Long> workingSet = Lists.newArrayList();
    Map<Long, byte[]> cache = Maps.newHashMap();
    for (long i = 0; i < max; ++i) {
      workingSet.add(i);
      cache.put(i, Bytes.toBytes(i));
    }
    transientWorkingSet = new TTQueueOnVCTable.TransientWorkingSet(workingSet, cache);

    for (int i = 0; i < max; ++i) {
      assertTrue(transientWorkingSet.hasNext());
      assertEquals(new TTQueueOnVCTable.DequeueEntry(i), transientWorkingSet.peekNext());
      assertEquals(new TTQueueOnVCTable.DequeueEntry(i), transientWorkingSet.next());
    }
    assertFalse(transientWorkingSet.hasNext());
  }

  @Test
  public void testReconfigPartitionInstance() throws Exception {
    final TTQueueOnVCTable.ReconfigPartitionInstance reconfigPartitionInstance1 =
      new TTQueueOnVCTable.ReconfigPartitionInstance(3, 100L);

    final TTQueueOnVCTable.ReconfigPartitionInstance reconfigPartitionInstance2 =
      new TTQueueOnVCTable.ReconfigPartitionInstance(2, 100L);

    final TTQueueOnVCTable.ReconfigPartitionInstance reconfigPartitionInstance3 =
      new TTQueueOnVCTable.ReconfigPartitionInstance(2, 101L);

    final TTQueueOnVCTable.ReconfigPartitionInstance reconfigPartitionInstance4 =
      new TTQueueOnVCTable.ReconfigPartitionInstance(3, 100L);

    // Verify equals
    assertEquals(reconfigPartitionInstance1, reconfigPartitionInstance1);
    assertEquals(reconfigPartitionInstance1, reconfigPartitionInstance4);
    assertNotEquals(reconfigPartitionInstance1, reconfigPartitionInstance2);
    assertNotEquals(reconfigPartitionInstance2, reconfigPartitionInstance3);
    assertNotEquals(reconfigPartitionInstance4, reconfigPartitionInstance3);

    // Verify redundancy
    assertTrue(reconfigPartitionInstance1.isRedundant(1000L));
    assertTrue(reconfigPartitionInstance1.isRedundant(101L));
    assertFalse(reconfigPartitionInstance1.isRedundant(100L));
    assertFalse(reconfigPartitionInstance1.isRedundant(99L));
    assertFalse((reconfigPartitionInstance1.isRedundant(10L)));

    // Encode to bytes
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    reconfigPartitionInstance1.encode(new BinaryEncoder(bos));
    byte[] bytes = bos.toByteArray();

    // Decode from bytes
    TTQueueOnVCTable.ReconfigPartitionInstance actual =
      TTQueueOnVCTable.ReconfigPartitionInstance.decode(new BinaryDecoder(new ByteArrayInputStream(bytes)));

    // Verify
    assertEquals(reconfigPartitionInstance1, actual);
  }

  @Test
  public void testReconfigPartitioner() throws Exception {
    TTQueueOnVCTable.ReconfigPartitioner partitioner1 =
      new TTQueueOnVCTable.ReconfigPartitioner(3, PartitionerType.HASH);
    partitioner1.add(0, 5);
    partitioner1.add(1, 10);
    partitioner1.add(2, 12);

    TTQueueOnVCTable.ReconfigPartitioner partitioner2 =
      new TTQueueOnVCTable.ReconfigPartitioner(3, PartitionerType.HASH);
    partitioner2.add(0, 5);
    partitioner2.add(1, 15);
    partitioner2.add(2, 12);

    // Verify equals
    assertEquals(partitioner1, partitioner1);
    assertNotEquals(partitioner1, partitioner2);

    // Verify redundancy
    assertTrue(partitioner1.isRedundant(100L));
    assertTrue(partitioner1.isRedundant(54L));
    assertTrue(partitioner1.isRedundant(13L));
    for (int i = 0; i < 12; ++i) {
      assertFalse(partitioner1.isRedundant(i));
    }

    // Encode to bytes
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    partitioner1.encode(new BinaryEncoder(bos));
    byte[] bytes = bos.toByteArray();

    // Decode from bytes
    TTQueueOnVCTable.ReconfigPartitioner actual =
      TTQueueOnVCTable.ReconfigPartitioner.decode(new BinaryDecoder(new ByteArrayInputStream(bytes)));

    // Verify
    assertEquals(partitioner1, actual);
  }

  @Test
  public void testReconfigPartitionerEmit1() throws Exception {
    // Partition
    // Entries        :1  2  3  4  5  6  7  8  9  10  11  12
    // Consumers Ack  :      0           1  2
    // Partition      :1  2  0  1  2  0  1  2  0  1   2   0

    int groupSize = 3;
    int lastEntry = 12;

    long zeroAck = 3L;
    long oneAck = 7L;
    long twoAck = 8L;

    TTQueueOnVCTable.ReconfigPartitioner partitioner =
      new TTQueueOnVCTable.ReconfigPartitioner(groupSize, PartitionerType.ROUND_ROBIN);
    partitioner.add(0, zeroAck);
    partitioner.add(1, oneAck);
    partitioner.add(2, twoAck);

    verifyTestReconfigPartitionerEmit(lastEntry, ImmutableSet.of(1L, 2L, 3L, 4L, 5L, 7L, 8L), partitioner);
  }

  @Test
  public void testReconfigPartitionerEmit2() throws Exception {
    // Partition
    // Entries        :1  2  3  4  5  6  7  8  9  10  11  12
    // Consumers Ack  :1  2  0
    // Partition      :1  2  0  1  2  0  1  2  0  1   2   0

    int groupSize = 3;
    int lastEntry = 12;

    long zeroAck = 3L;
    long oneAck = 1L;
    long twoAck = 2L;

    TTQueueOnVCTable.ReconfigPartitioner partitioner =
      new TTQueueOnVCTable.ReconfigPartitioner(groupSize, PartitionerType.ROUND_ROBIN);
    partitioner.add(0, zeroAck);
    partitioner.add(1, oneAck);
    partitioner.add(2, twoAck);

    verifyTestReconfigPartitionerEmit(lastEntry, ImmutableSet.of(1L, 2L, 3L), partitioner);
  }

  @Test
  public void testReconfigPartitionerEmit3() throws Exception {
    // Partition
    // Entries        :1  2  3  4  5  6  7  8  9  10  11  12
    // Consumers Ack  :1  2                               0
    // Partition      :1  2  0  1  2  0  1  2  0  1   2   0

    int groupSize = 3;
    int lastEntry = 12;

    long zeroAck = 12L;
    long oneAck = 1L;
    long twoAck = 2L;

    TTQueueOnVCTable.ReconfigPartitioner partitioner =
      new TTQueueOnVCTable.ReconfigPartitioner(groupSize, PartitionerType.ROUND_ROBIN);
    partitioner.add(0, zeroAck);
    partitioner.add(1, oneAck);
    partitioner.add(2, twoAck);

    verifyTestReconfigPartitionerEmit(lastEntry, ImmutableSet.of(1L, 2L, 3L, 6L, 9L, 12L), partitioner);
  }

  @Test
  public void testReconfigPartitionerEmit4() throws Exception {
    // Partition
    // Entries        :1  2  3  4  5  6  7  8  9  10  11  12
    // Consumers Ack  :                           1   2   0
    // Partition      :1  2  0  1  2  0  1  2  0  1   2   0

    int groupSize = 3;
    int lastEntry = 12;

    long zeroAck = 12L;
    long oneAck = 10L;
    long twoAck = 11L;

    TTQueueOnVCTable.ReconfigPartitioner partitioner =
      new TTQueueOnVCTable.ReconfigPartitioner(groupSize, PartitionerType.ROUND_ROBIN);
    partitioner.add(0, zeroAck);
    partitioner.add(1, oneAck);
    partitioner.add(2, twoAck);

    verifyTestReconfigPartitionerEmit(lastEntry, ImmutableSet.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L),
                                      partitioner);
  }

  @Test
  public void testReconfigPartitionersList() throws Exception {
    List<TTQueueOnVCTable.ReconfigPartitioner> partitionerList = Lists.newArrayList();
    List<Long> partitionerListMaxAck = Lists.newArrayList();

    int groupSize = 3;

    TTQueueOnVCTable.ReconfigPartitioner partitioner =
      new TTQueueOnVCTable.ReconfigPartitioner(groupSize, PartitionerType.ROUND_ROBIN);
    partitioner.add(0, 8L);
    partitioner.add(1, 15L);
    partitioner.add(2, 11L);
    partitionerList.add(partitioner);
    partitionerListMaxAck.add(15L);

    // Verify compaction with one partitioner
    TTQueueOnVCTable.ReconfigPartitionersList rl1 =
      new TTQueueOnVCTable.ReconfigPartitionersList(partitionerList);
    assertEquals(1, rl1.getReconfigPartitioners().size());
    rl1.compact(9L);
    assertEquals(1, rl1.getReconfigPartitioners().size());
    rl1.compact(16L);
    assertTrue(rl1.getReconfigPartitioners().isEmpty());
    verifyReconfigPartitionersListCompact(
      new TTQueueOnVCTable.ReconfigPartitionersList(partitionerList), partitionerListMaxAck);

    groupSize = 4;

    partitioner =
      new TTQueueOnVCTable.ReconfigPartitioner(groupSize, PartitionerType.ROUND_ROBIN);
    partitioner.add(0, 30L);
    partitioner.add(1, 14L);
    partitioner.add(2, 15L);
    partitioner.add(3, 28L);
    partitionerList.add(partitioner);
    partitionerListMaxAck.add(30L);

    // Verify compaction with two partitioners
    TTQueueOnVCTable.ReconfigPartitionersList rl2 =
      new TTQueueOnVCTable.ReconfigPartitionersList(partitionerList);
    assertEquals(2, rl2.getReconfigPartitioners().size());
    rl2.compact(8L);
    assertEquals(2, rl2.getReconfigPartitioners().size());
    rl2.compact(16L);
    assertEquals(1, rl2.getReconfigPartitioners().size());
    rl2.compact(31L);
    assertTrue(rl2.getReconfigPartitioners().isEmpty());
    verifyReconfigPartitionersListCompact(
      new TTQueueOnVCTable.ReconfigPartitionersList(partitionerList), partitionerListMaxAck);

    groupSize = 5;

    partitioner =
      new TTQueueOnVCTable.ReconfigPartitioner(groupSize, PartitionerType.ROUND_ROBIN);
    partitioner.add(0, 43L);
    partitioner.add(1, 47L);
    partitioner.add(2, 3L);
    partitioner.add(3, 32L);
    partitioner.add(3, 35L);
    partitionerList.add(partitioner);
    partitionerListMaxAck.add(47L);

    // Verify compaction with 3 partitioners
    verifyReconfigPartitionersListCompact(
      new TTQueueOnVCTable.ReconfigPartitionersList(partitionerList), partitionerListMaxAck);

    groupSize = 2;

    partitioner =
      new TTQueueOnVCTable.ReconfigPartitioner(groupSize, PartitionerType.ROUND_ROBIN);
    partitioner.add(0, 55);
    partitioner.add(1, 50L);
    partitionerList.add(partitioner);
    partitionerListMaxAck.add(55L);

    // Verify compaction with 4 partitioners
    verifyReconfigPartitionersListCompact(
      new TTQueueOnVCTable.ReconfigPartitionersList(partitionerList), partitionerListMaxAck);

    // Verify encode/decode
    TTQueueOnVCTable.ReconfigPartitionersList expectedEncode =
      new TTQueueOnVCTable.ReconfigPartitionersList(partitionerList);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    expectedEncode.encode(new BinaryEncoder(bos));
    byte[] bytes = bos.toByteArray();

    TTQueueOnVCTable.ReconfigPartitionersList actualEncode =
      TTQueueOnVCTable.ReconfigPartitionersList.decode(new BinaryDecoder(new ByteArrayInputStream(bytes)));
    assertEquals(expectedEncode, actualEncode);
  }

  private void verifyReconfigPartitionersListCompact(
    TTQueueOnVCTable.ReconfigPartitionersList reconfigPartitionersList, List<Long> maxAckList) {
    List<Long> sortedMaxAckList = Lists.newLinkedList(maxAckList);
    Collections.sort(sortedMaxAckList);

    long max = sortedMaxAckList.get(sortedMaxAckList.size() - 1);
    for (long i = 0; i <= max; ++i) {
      reconfigPartitionersList.compact(i);
      if (!sortedMaxAckList.isEmpty() && i > sortedMaxAckList.get(0)) {
        sortedMaxAckList.remove(0);
      }
      assertEquals(sortedMaxAckList.size(), reconfigPartitionersList.getReconfigPartitioners().size());
    }
    reconfigPartitionersList.compact(max + 1);
    assertTrue(reconfigPartitionersList.getReconfigPartitioners().isEmpty());
  }

  @Test
  public void testReconfigPartitionersListEmit1() throws Exception {
    List<TTQueueOnVCTable.ReconfigPartitioner> partitionerList = Lists.newArrayList();
    // Partition 1
    // Entries        :1  2  3  4  5  6  7  8  9  10  11  12
    // Consumers Ack  :      0           1  2
    // Partition      :1  2  0  1  2  0  1  2  0  1   2   0

    int groupSize = 3;

    long zeroAck = 3L;
    long oneAck = 7L;
    long twoAck = 8L;

    TTQueueOnVCTable.ReconfigPartitioner partitioner =
      new TTQueueOnVCTable.ReconfigPartitioner(groupSize, PartitionerType.ROUND_ROBIN);
    partitioner.add(0, zeroAck);
    partitioner.add(1, oneAck);
    partitioner.add(2, twoAck);

    partitionerList.add(partitioner);
    Set<Long> expectedAckedEntries1 = ImmutableSet.of(1L, 2L, 3L, 4L, 5L, 7L, 8L);

    // Partition 2
    // Entries        :1  2  3  4  5  6  7  8  9  10  11  12  13  14  15  16  17  18
    // Consumers Ack  :            1     3        2       0
    // Partition      :1  2  3  0  1  2  3  0  1  2   3   0   1   2   3   0   1   2

    groupSize = 4;
    int lastEntry = 18;

    zeroAck = 12L;
    oneAck = 5L;
    twoAck = 10L;
    long threeAck = 7L;

    partitioner =
      new TTQueueOnVCTable.ReconfigPartitioner(groupSize, PartitionerType.ROUND_ROBIN);
    partitioner.add(0, zeroAck);
    partitioner.add(1, oneAck);
    partitioner.add(2, twoAck);
    partitioner.add(3, threeAck);

    partitionerList.add(partitioner);
    Set<Long> expectedAckedEntries2 = ImmutableSet.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 10L, 12L);

    verifyTestReconfigPartitionerEmit(lastEntry, Sets.union(expectedAckedEntries1, expectedAckedEntries2),
                                      new TTQueueOnVCTable.ReconfigPartitionersList(partitionerList));
  }

  private void verifyTestReconfigPartitionerEmit(long queueSize, Set<Long> ackedEntries,
                                                 QueuePartitioner partitioner) {
    int groupSize = -1; // will be ignored
    int instanceId = -2; // will be ignored
    for (long entryId = 1; entryId <= queueSize; ++entryId) {
      if (ackedEntries.contains(entryId)) {
        //        System.out.println("Not Emit:" + entryId);
        assertFalse("Not Emit:" + entryId, partitioner.shouldEmit(groupSize, instanceId, entryId, null));
      } else {
        //        System.out.println("Emit:" + entryId);
        assertTrue("Emit:" + entryId, partitioner.shouldEmit(groupSize, instanceId, entryId, null));
      }
    }
  }

  @Test
  public void testQueueStateImplEncode() throws Exception {
    TTQueueOnVCTable.QueueStateImpl queueState = new TTQueueOnVCTable.QueueStateImpl();
    queueState.setPartitioner(FIFO);
    List<TTQueueOnVCTable.DequeueEntry> dequeueEntryList = Lists.newArrayList(new TTQueueOnVCTable.DequeueEntry(2, 1),
                                                                              new TTQueueOnVCTable.DequeueEntry(3, 0));
    Map<Long, byte[]> cachedEntries = ImmutableMap.of(2L, new byte[]{1, 2}, 3L, new byte[]{4, 5});

    TTQueueOnVCTable.TransientWorkingSet transientWorkingSet =
      new TTQueueOnVCTable.TransientWorkingSet(dequeueEntryList, 2, cachedEntries, 10);
    queueState.setTransientWorkingSet(transientWorkingSet);

    TTQueueOnVCTable.DequeuedEntrySet dequeuedEntrySet =
      new TTQueueOnVCTable.DequeuedEntrySet(Sets.newTreeSet(dequeueEntryList));
    queueState.setDequeueEntrySet(dequeuedEntrySet);

    long consumerReadPointer = 3L;
    queueState.setConsumerReadPointer(consumerReadPointer);

    long queueWritePointer = 6L;
    queueState.setQueueWritePointer(queueWritePointer);

    ClaimedEntryList claimedEntryList = new ClaimedEntryList();
    claimedEntryList.add(new ClaimedEntryRange(4L, 6L));
    claimedEntryList.add(new ClaimedEntryRange(7L, 8L));
    claimedEntryList.add(new ClaimedEntryRange(10L, 20L));
    queueState.setClaimedEntryList(claimedEntryList);

    long lastEvictTimeInSecs = 124325342L;
    queueState.setLastEvictTimeInSecs(lastEvictTimeInSecs);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    queueState.encodeTransient(new BinaryEncoder(bos));
    byte[] bytes = bos.toByteArray();

    TTQueueOnVCTable.QueueStateImpl actual =
      TTQueueOnVCTable.QueueStateImpl.decodeTransient(new BinaryDecoder(new ByteArrayInputStream(bytes)));

    // QueueStateImpl does not override equals and hashcode methods
    assertEquals(FIFO, actual.getPartitioner());
    assertEquals(transientWorkingSet, actual.getTransientWorkingSet());
    assertEquals(dequeuedEntrySet, actual.getDequeueEntrySet());
    assertEquals(consumerReadPointer, actual.getConsumerReadPointer());
    assertEquals(queueWritePointer, actual.getQueueWritePointer());
    assertEquals(claimedEntryList, actual.getClaimedEntryList());
    assertEquals(0L, actual.getLastEvictTimeInSecs()); // last evict is not encoded
  }

  @Test
  public void testSingleConsumerWithHashPartitioning() throws Exception {
    final String hashKey = "hashKey";
    final boolean singleEntry = true;
    final int numQueueEntries = 88;
    final int numConsumers = 4;
    final int consumerGroupId = 0;
    TTQueue queue = createQueue();
    Transaction dirtyTxn = new Transaction(getDirtyWriteVersion(), getDirtyPointer(), true);

    // enqueue some entries
    for (int i = 0; i < numQueueEntries; i++) {
      QueueEntry queueEntry = new QueueEntry(hashKey, i, Bytes.toBytes("value" + i % numConsumers));
      assertTrue(queue.enqueue(queueEntry, dirtyTxn).isSuccess());
    }
    // dequeue it with HASH partitioner
    QueueConfig config = new QueueConfig(PartitionerType.HASH, singleEntry);

    QueueConsumer[] consumers = new QueueConsumer[numConsumers];
    for (int i = 0; i < numConsumers; i++) {
      consumers[i] = new QueueConsumer(i, consumerGroupId, numConsumers, "group1", hashKey, config);
      queue.configure(consumers[i], getDirtyPointer());
    }

    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries);

    // enqueue some more entries
    for (int i = numQueueEntries; i < numQueueEntries * 2; i++) {
      QueueEntry queueEntry = new QueueEntry(hashKey, i, Bytes.toBytes("value" + i % numConsumers));
      assertTrue(queue.enqueue(queueEntry, dirtyTxn).isSuccess());
    }
    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries);
  }

  @Test
  public void testSingleConsumerWithRoundRobinPartitioning() throws Exception {
    final boolean singleEntry = true;
    final int numQueueEntries = 88;
    final int numConsumers = 4;
    final int consumerGroupId = 0;
    TTQueue queue = createQueue();
    Transaction dirtyTxn = new Transaction(getDirtyWriteVersion(), getDirtyPointer(), true);

    // enqueue some entries
    for (int i = 0; i < numQueueEntries; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes("value" + (i + 1) % numConsumers));
      assertTrue(queue.enqueue(queueEntry, dirtyTxn).isSuccess());
    }

    // dequeue it with ROUND_ROBIN partitioner
    QueueConfig config = new QueueConfig(PartitionerType.ROUND_ROBIN, singleEntry);

    QueueConsumer[] consumers = new QueueConsumer[numConsumers];
    for (int i = 0; i < numConsumers; i++) {
      consumers[i] = new QueueConsumer(i, consumerGroupId, numConsumers, "group1", config);
      queue.configure(consumers[i], getDirtyPointer());
    }

    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries);

    // enqueue some more entries
    for (int i = numQueueEntries; i < numQueueEntries * 2; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes("value" + (i + 1) % numConsumers));
      assertTrue(queue.enqueue(queueEntry, dirtyTxn).isSuccess());
    }

    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries);
  }

  private void dequeuePartitionedEntries(TTQueue queue, QueueConsumer[] consumers,
                                         int numConsumers, int totalEnqueues) throws Exception {
    ReadPointer dirtyReadPointer = getDirtyPointer();
    Transaction dirtyTxn = new Transaction(getDirtyWriteVersion(), dirtyReadPointer, true);

    for (int i = 0; i < numConsumers; i++) {
      for (int j = 0; j < totalEnqueues / (2 * numConsumers); j++) {
        DequeueResult result = queue.dequeue(consumers[i], dirtyReadPointer);
        // verify we got something and it's the first value
        assertTrue(result.toString(), result.isSuccess());
        assertEquals("value" + i, Bytes.toString(result.getEntry().getData()));
        // dequeue again without acking, should still get first value
        result = queue.dequeue(consumers[i], dirtyReadPointer);
        assertTrue(result.isSuccess());
        assertEquals("value" + i, Bytes.toString(result.getEntry().getData()));

        // ack
        queue.ack(result.getEntryPointer(), consumers[i], dirtyTxn);
        //        queue.finalize(result.getEntryPointer(), consumers[i], -1, dirtyTxn);

        // dequeue, should get second value
        result = queue.dequeue(consumers[i], dirtyReadPointer);
        assertTrue("Consumer:" + i + " Iteration:" + j, result.isSuccess());
        assertEquals("value" + i, Bytes.toString(result.getEntry().getData()));

        // ack
        queue.ack(result.getEntryPointer(), consumers[i], dirtyTxn);
        //        queue.finalize(result.getEntryPointer(), consumers[i], -1, dirtyTxn);
      }

      // verify queue is empty
      DequeueResult result = queue.dequeue(consumers[i], dirtyReadPointer);
      assertTrue(result.isEmpty());
    }
  }

  @Test
  public void testSingleStatefulConsumerWithHashPartitioning() throws Exception {
    final String hashKey = "hashKey";
    final boolean singleEntry = true;
    // Choose a number that leaves a reminder when divided by batchSize,
    // and be big enough so that it forms a few batches
    final int numQueueEntries = 264;
    final int numConsumers = 4;
    final int consumerGroupId = 0;
    TTQueue queue = createQueue();
    Transaction dirtyTxn = new Transaction(getDirtyWriteVersion(), getDirtyPointer(), true);

    // enqueue some entries
    for (int i = 0; i < numQueueEntries; i++) {
      QueueEntry queueEntry = new QueueEntry(hashKey, i, Bytes.toBytes(i));
      assertTrue(queue.enqueue(queueEntry, dirtyTxn).isSuccess());
    }
    // dequeue it with HASH partitioner
    // TODO: test with more batch sizes
    QueueConfig config = new QueueConfig(PartitionerType.HASH, singleEntry, 29);

    StatefulQueueConsumer[] consumers = new StatefulQueueConsumer[numConsumers];
    for (int i = 0; i < numConsumers; i++) {
      consumers[i] = new StatefulQueueConsumer(i, consumerGroupId, numConsumers, "group1", hashKey, config);
      queue.configure(consumers[i], getDirtyPointer());
    }

    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries, 0, PartitionerType.HASH);
    System.out.println("Round 1 dequeue done");

    // enqueue some more entries
    for (int i = numQueueEntries; i < numQueueEntries * 2; i++) {
      QueueEntry queueEntry = new QueueEntry(hashKey, i, Bytes.toBytes(i));
      assertTrue(queue.enqueue(queueEntry, dirtyTxn).isSuccess());
    }
    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries, numQueueEntries, PartitionerType.HASH);
    System.out.println("Round 2 dequeue done");
  }

  @Test
  public void testSingleStatefulConsumerWithRoundRobinPartitioning() throws Exception {
    final boolean singleEntry = true;
    // Choose a number that doesn't leave a reminder when divided by batchSize,
    // and be big enough so that it forms a few batches
    final int numQueueEntries = 264;
    final int numConsumers = 4;
    final int consumerGroupId = 0;
    TTQueue queue = createQueue();
    Transaction dirtyTxn = new Transaction(getDirtyWriteVersion(), getDirtyPointer(), true);

    // enqueue some entries
    for (int i = 0; i < numQueueEntries; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes(i + 1));
      assertTrue(queue.enqueue(queueEntry, dirtyTxn).isSuccess());
    }

    // dequeue it with ROUND_ROBIN partitioner
    // TODO: test with more batch sizes
    QueueConfig config = new QueueConfig(PartitionerType.ROUND_ROBIN, singleEntry, 11);

    StatefulQueueConsumer[] consumers = new StatefulQueueConsumer[numConsumers];
    for (int i = 0; i < numConsumers; i++) {
      consumers[i] = new StatefulQueueConsumer(i, consumerGroupId, numConsumers, "group1", config);
      queue.configure(consumers[i], getDirtyPointer());
    }

    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries, 0, PartitionerType
      .ROUND_ROBIN);

    // enqueue some more entries
    for (int i = numQueueEntries; i < numQueueEntries * 2; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes(i + 1));
      assertTrue(queue.enqueue(queueEntry, dirtyTxn).isSuccess());
    }

    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries,
                              numQueueEntries, PartitionerType.ROUND_ROBIN);
  }

  private void dequeuePartitionedEntries(TTQueue queue, StatefulQueueConsumer[] consumers, int numConsumers,
                                         int numQueueEntries, int startQueueEntry,
                                         PartitionerType partitionerType) throws Exception {
    ReadPointer dirtyReadPointer = getDirtyPointer();
    Transaction dirtyTxn = new Transaction(getDirtyWriteVersion(), dirtyReadPointer, true);
    for (int consumer = 0; consumer < numConsumers; consumer++) {
      for (int entry = 0; entry < numQueueEntries / (2 * numConsumers); entry++) {
        DequeueResult result = queue.dequeue(consumers[consumer], dirtyReadPointer);
        // verify we got something and it's the first value
        assertTrue(result.toString(), result.isSuccess());
        int expectedValue = startQueueEntry + consumer + (2 * entry * numConsumers);
        if (partitionerType == PartitionerType.ROUND_ROBIN) {
          if (consumer == 0) {
            // Adjust the expected value for consumer 0
            expectedValue += numConsumers;
          }
        }
        assertEquals(expectedValue, Bytes.toInt(result.getEntry().getData()));
        // dequeue again without acking, should still get first value
        result = queue.dequeue(consumers[consumer], dirtyReadPointer);
        assertTrue(result.isSuccess());
        assertEquals(expectedValue, Bytes.toInt(result.getEntry().getData()));

        // ack
        queue.ack(result.getEntryPointer(), consumers[consumer], dirtyTxn);
        //        queue.finalize(result.getEntryPointer(), consumers[consumer], -1, dirtyTxn);

        // dequeue, should get second value
        result = queue.dequeue(consumers[consumer], dirtyReadPointer);
        assertTrue("Consumer:" + consumer + " Entry:" + entry, result.isSuccess());
        expectedValue += numConsumers;
        assertEquals(expectedValue, Bytes.toInt(result.getEntry().getData()));

        // ack
        queue.ack(result.getEntryPointer(), consumers[consumer], dirtyTxn);
        //        queue.finalize(result.getEntryPointer(), consumers[consumer], -1, dirtyTxn);
      }

      // verify queue is empty
      DequeueResult result = queue.dequeue(consumers[consumer], dirtyReadPointer);
      assertTrue(result.isEmpty());
    }
  }

  @Test
  public void testSingleStatefulConsumerWithFifo() throws Exception {
    final boolean singleEntry = true;
    // Make sure numQueueEntries % batchSize == 0 && numQueueEntries % numConsumers == 0
    final int numQueueEntries = 200;
    final int numConsumers = 4;
    final int consumerGroupId = 0;
    TTQueue queue = createQueue();
    Transaction dirtyTxn = new Transaction(getDirtyWriteVersion(), getDirtyPointer(), true);

    // enqueue some entries
    for (int i = 0; i < numQueueEntries; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes(i));
      assertTrue(queue.enqueue(queueEntry, dirtyTxn).isSuccess());
    }

    // dequeue it with ROUND_ROBIN partitioner
    // TODO: test with more batch sizes
    QueueConfig config = new QueueConfig(FIFO, singleEntry, 10);

    StatefulQueueConsumer[] statefulQueueConsumers = new StatefulQueueConsumer[numConsumers];
    QueueConsumer[] queueConsumers = new QueueConsumer[numConsumers];
    for (int i = 0; i < numConsumers; i++) {
      statefulQueueConsumers[i] = new StatefulQueueConsumer(i, consumerGroupId, numConsumers, "group1", config);
      queueConsumers[i] = new QueueConsumer(i, consumerGroupId, numConsumers, "group1", config);
      queue.configure(queueConsumers[i], getDirtyPointer());
    }

    // dequeue and verify
    dequeueFifoEntries(queue, statefulQueueConsumers, numConsumers, numQueueEntries, 0);

    // enqueue some more entries
    for (int i = numQueueEntries; i < 2 * numQueueEntries; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes(i));
      assertTrue(queue.enqueue(queueEntry, dirtyTxn).isSuccess());
    }

    // dequeue and verify
    dequeueFifoEntries(queue, statefulQueueConsumers, numConsumers, numQueueEntries, numQueueEntries);

    // Run with stateless QueueConsumer
    for (int i = 2 * numQueueEntries; i < 3 * numQueueEntries; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes(i));
      assertTrue(queue.enqueue(queueEntry, dirtyTxn).isSuccess());
    }

    // dequeue and verify
    dequeueFifoEntries(queue, queueConsumers, numConsumers, numQueueEntries, 2 * numQueueEntries);

  }

  private void dequeueFifoEntries(TTQueue queue, QueueConsumer[] consumers, int numConsumers,
                                  int numQueueEntries, int startQueueEntry) throws Exception {
    ReadPointer dirtyReadPointer = getDirtyPointer();
    Transaction dirtyTxn = new Transaction(getDirtyWriteVersion(), dirtyReadPointer, true);
    int expectedValue = startQueueEntry;
    for (int consumer = 0; consumer < numConsumers; consumer++) {
      for (int entry = 0; entry < numQueueEntries / (2 * numConsumers); entry++, ++expectedValue) {
        DequeueResult result = queue.dequeue(consumers[consumer], dirtyReadPointer);
        // verify we got something and it's the first value
        assertTrue(String.format("Consumer=%d, entry=%d, %s", consumer, entry, result.toString()), result.isSuccess());
        assertEquals(expectedValue, Bytes.toInt(result.getEntry().getData()));
        // dequeue again without acking, should still get first value
        result = queue.dequeue(consumers[consumer], dirtyReadPointer);
        assertTrue(result.isSuccess());
        assertEquals(expectedValue, Bytes.toInt(result.getEntry().getData()));

        // ack
        queue.ack(result.getEntryPointer(), consumers[consumer], dirtyTxn);
        //        queue.finalize(result.getEntryPointer(), consumers[consumer], -1, dirtyTxn);

        // dequeue, should get second value
        result = queue.dequeue(consumers[consumer], dirtyReadPointer);
        assertTrue(result.isSuccess());
        ++expectedValue;
        assertEquals(expectedValue, Bytes.toInt(result.getEntry().getData()));

        // ack
        queue.ack(result.getEntryPointer(), consumers[consumer], dirtyTxn);
        //        queue.finalize(result.getEntryPointer(), consumers[consumer], -1, dirtyTxn);
      }
    }

    // verify queue is empty for all consumers
    for (int consumer = 0; consumer < numConsumers; ++consumer) {
      DequeueResult result = queue.dequeue(consumers[consumer], dirtyReadPointer);
      assertTrue(result.isEmpty());
    }
  }

  @Test
  public void testMaxCrashDequeueTries() throws Exception {
    TTQueue queue = createQueue();
    final long groupId = 0;
    final int instanceId = 0;
    final int groupSize = 1;
    Transaction dirtyTxn = new Transaction(getDirtyWriteVersion(), getDirtyPointer(), true);

    assertTrue(queue.enqueue(new QueueEntry(Bytes.toBytes(1)), dirtyTxn).isSuccess());
    assertTrue(queue.enqueue(new QueueEntry(Bytes.toBytes(2)), dirtyTxn).isSuccess());

    // dequeue it with FIFO partitioner, single entry mode
    QueueConfig config = new QueueConfig(FIFO, true);

    queue.configure(new StatefulQueueConsumer(instanceId, groupId, groupSize, "", config), getDirtyPointer());

    for (int tries = 0; tries <= MAX_CRASH_DEQUEUE_TRIES; ++tries) {
      // Simulate consumer crashing by sending in empty state every time and not acking the entry
      DequeueResult result = queue.dequeue(new StatefulQueueConsumer(instanceId, groupId, groupSize, "", config),
                                           getDirtyPointer());
      assertTrue(result.isSuccess());
      assertEquals(1, Bytes.toInt(result.getEntry().getData()));
    }

    // After max tries, the entry will be ignored
    StatefulQueueConsumer statefulQueueConsumer =
      new StatefulQueueConsumer(instanceId, groupId, groupSize, "", config);
    for (int tries = 0; tries <= MAX_CRASH_DEQUEUE_TRIES + 10; ++tries) {
      // No matter how many times a dequeue is repeated with state, the same entry needs to be returned
      DequeueResult result = queue.dequeue(statefulQueueConsumer, getDirtyPointer());
      assertTrue(result.isSuccess());
      assertEquals("Tries=" + tries, 2, Bytes.toInt(result.getEntry().getData()));
    }
    DequeueResult result = queue.dequeue(statefulQueueConsumer, getDirtyPointer());
    assertTrue(result.isSuccess());

    // No matter how many times a dequeue is repeated with state NOT_FOUND, the same entry needs to be returned
    for (int tries = 0; tries <= MAX_CRASH_DEQUEUE_TRIES + 10; ++tries) {
      statefulQueueConsumer.setStateType(QueueConsumer.StateType.NOT_FOUND);
      result = queue.dequeue(statefulQueueConsumer, getDirtyPointer());
      assertTrue(result.isSuccess());
      assertEquals("Tries=" + tries, 2, Bytes.toInt(result.getEntry().getData()));
    }

    assertEquals(2, Bytes.toInt(result.getEntry().getData()));
    queue.ack(result.getEntryPointer(), statefulQueueConsumer, dirtyTxn);

    result = queue.dequeue(statefulQueueConsumer, getDirtyPointer());
    assertTrue(result.isEmpty());
  }

  @Test
  public void testReconfigChecks() throws Exception {
    // During reconfigure there should be no dequeued entries and no pending acks for a consumer
    TTQueue queue = createQueue();

    // Enqueue some entries
    Transaction t = oracle.startTransaction(true);
    for (int i = 1; i < 5; ++i) {
      queue.enqueue(new QueueEntry(Bytes.toBytes(i)), t);
    }
    oracle.commitTransaction(t);

    QueueConsumer consumer = new StatefulQueueConsumer(0, 0, 1, new QueueConfig(FIFO, true, 10, false));
    queue.configure(consumer, oracle.getReadPointer());

    DequeueResult result = queue.dequeue(consumer, oracle.getReadPointer());
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(1, Bytes.toInt(result.getEntry().getData()));

    // Trying to change configuration when entries are dequeued should throw error
    try {
      queue.configure(new StatefulQueueConsumer(0, 0, 2, new QueueConfig(FIFO, true, 8, false)),
                      oracle.getReadPointer());
      fail("Configuration change should fail");
    } catch (OperationException e) {
      if (e.getStatus() != StatusCode.ILLEGAL_GROUP_CONFIG_CHANGE) {
        throw e;
      }
    }

    // Ack entry
    t = oracle.startTransaction(true);
    queue.ack(result.getEntryPointers(), consumer, t);

    // Trying to change configuration when acked entries are not committed should throw error
    try {
      queue.configure(new StatefulQueueConsumer(0, 0, 2, new QueueConfig(FIFO, true, 8, false)),
                      oracle.getReadPointer());
      fail("Configuration change should fail");
    } catch (OperationException e) {
      if (e.getStatus() != StatusCode.ILLEGAL_GROUP_CONFIG_CHANGE) {
        throw e;
      }
    }

    oracle.commitTransaction(t);

    // Changing configuration after transaction is committed should not throw any errors
    consumer = new StatefulQueueConsumer(0, 0, 2, new QueueConfig(FIFO, true, 8, true));
    queue.configure(consumer, oracle.getReadPointer());

    result = queue.dequeue(consumer, oracle.getReadPointer());
    assertFalse(result.isEmpty());
    for (int i = 0; i < 3; ++i) {
      Assert.assertEquals(i + 2, Bytes.toInt(result.getEntries()[i].getData()));
    }

    t = oracle.startTransaction(true);
    queue.ack(result.getEntryPointers(), consumer, t);
    oracle.commitTransaction(t);
  }

  @Test
  public void testPartitionTypeCompatibilityCheck() throws Exception {
    // Requested partition type must be same as saved partition type if any
    TTQueue queue = createQueue();

    // Enqueue some entries
    Transaction t = oracle.startTransaction(true);
    for (int i = 1; i < 5; ++i) {
      queue.enqueue(new QueueEntry(Bytes.toBytes(i)), t);
    }
    oracle.commitTransaction(t);

    QueueConsumer consumer = new QueueConsumer(0, 0, 1, new QueueConfig(FIFO, true, 10, false));
    queue.configure(consumer, oracle.getReadPointer());

    // Configuring again with a different partition type should fail
    try {
      queue.configure(new QueueConsumer(0, 0, 1, "gkey", "hkey", new QueueConfig(HASH, true, 10, false)),
                      oracle.getReadPointer());
      fail("Configuration with different partition type should fail");
    } catch (OperationException e) {
      if (e.getStatus() != StatusCode.INVALID_STATE) {
        throw e;
      }
    }

    // Configuring again with a different partition type should fail
    try {
      queue.configure(new QueueConsumer(0, 0, 1, new QueueConfig(ROUND_ROBIN, true, 10, false)),
                      oracle.getReadPointer());
      fail("Configuration with different partition type should fail");
    } catch (OperationException e) {
      if (e.getStatus() != StatusCode.INVALID_STATE) {
        throw e;
      }
    }

    // Configuring again with same partition type is okay
    queue.configure(new QueueConsumer(0, 0, 1, new QueueConfig(FIFO, true, 10, false)),
                    oracle.getReadPointer());

    // Dequeue with different partition type should fail
    try {
      queue.dequeue(new QueueConsumer(0, 0, 1, new QueueConfig(ROUND_ROBIN, true, 5, false)),
                    oracle.getReadPointer());
      fail("Dequeue should fail");
    } catch (OperationException e) {
      if (e.getStatus() != StatusCode.INVALID_STATE) {
        throw e;
      }
    }

    // Dequeue with different partition type should fail
    try {
      queue.dequeue(new QueueConsumer(0, 0, 1, "gkey", "hkey", new QueueConfig(HASH, true, 5, false)),
                    oracle.getReadPointer());
      fail("Dequeue should fail");
    } catch (OperationException e) {
      if (e.getStatus() != StatusCode.INVALID_STATE) {
        throw e;
      }
    }

    // Dequeue with same partition type is okay
    DequeueResult result = queue.dequeue(new QueueConsumer(0, 0, 1, new QueueConfig(FIFO, true, 5, false)),
                                         oracle.getReadPointer());
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(1, Bytes.toInt(result.getEntry().getData()));
  }

  @Test
  public void testFifoReconfig() throws Exception {
    Condition condition = new Condition() {
      @Override
      public boolean check(long entryId, int groupSize, long instanceId, int hash) {
        return true;
      }
    };

    PartitionerType partitionerType = FIFO;

    runConfigTest(partitionerType, condition);
  }

  @Test
  public void testRoundRobinReconfig() throws Exception {
    Condition condition = new Condition() {
      @Override
      public boolean check(long entryId, int groupSize, long instanceId, int hash) {
        return entryId % groupSize == instanceId;
      }
    };

    PartitionerType partitionerType = PartitionerType.ROUND_ROBIN;

    runConfigTest(partitionerType, condition);
  }

  @Test
  public void testHashReconfig() throws Exception {
    Condition condition = new Condition() {
      @Override
      public boolean check(long entryId, int groupSize, long instanceId, int hash) {
        return hash % groupSize == instanceId;
      }
    };

    PartitionerType partitionerType = PartitionerType.HASH;

    runConfigTest(partitionerType, condition);
  }

  public void runConfigTest(PartitionerType partitionerType, Condition condition) throws Exception {
    testReconfig(Lists.newArrayList(3, 2), 54, 5, 6, partitionerType, condition);
    testReconfig(Lists.newArrayList(3, 3, 4, 2), 144, 5, 9, partitionerType, condition);
    testReconfig(Lists.newArrayList(3, 5, 2, 1, 6, 2), 200, 5, 9, partitionerType, condition);
    testReconfig(Lists.newArrayList(3, 5, 2, 1, 6, 2), 200, 9, 9, partitionerType, condition);
    testReconfig(Lists.newArrayList(3, 5, 2, 1, 6, 2), 200, 9, 5, partitionerType, condition);
    testReconfig(Lists.newArrayList(1, 2, 3, 4, 5, 4, 3, 2, 1), 300, 9, 5, partitionerType, condition);
    // this failed before claimed entry lists were sorted
    testReconfig(Lists.newArrayList(3, 5, 2, 1, 6, 2), 50, 5, 3, partitionerType, condition);
  }

  private static final String hashKey = "HashKey";
  interface Condition {
    boolean check(long entryId, int groupSize, long instanceId, int hash);
  }

  // TODO: test with stateful and non-state consumer
  private void testReconfig(List<Integer> consumerCounts, final int numEntries, final int queueBatchSize,
                            final int perConsumerDequeueBatchSize, PartitionerType partitionerType,
                            Condition condition) throws Exception {
    Random random = new Random(System.currentTimeMillis());
    StringWriter debugCollector = new StringWriter();
    TTQueue queue = createQueue();
    Transaction transaction = oracle.startTransaction(true);

    List<Integer> expectedEntries = Lists.newArrayList();
    // Enqueue numEntries
    for (int i = 0; i < numEntries; ++i) {
      expectedEntries.add(i + 1);
      QueueEntry queueEntry = new QueueEntry(hashKey, i + 1, Bytes.toBytes(i + 1));
      assertTrue(debugCollector.toString(), queue.enqueue(queueEntry, transaction).isSuccess());
    }

    expectedEntries = ImmutableList.copyOf(expectedEntries);
    assertEquals(debugCollector.toString(), numEntries, expectedEntries.size());

    List<Integer> actualEntries = Lists.newArrayList();
    List<String> actualPrintEntries = Lists.newArrayList();
    List<Integer> sortedActualEntries;
    List<StatefulQueueConsumer> consumers;
    // dequeue it with FIFO partitioner, single entry mode
    QueueConfig config = new QueueConfig(partitionerType, true, queueBatchSize);
    long groupId = queue.getGroupID();
    int expectedOldConsumerCount = 0;

    loop:
    while (true) {
      for (Integer newConsumerCount : consumerCounts) {
        // Create new consumers
        consumers = Lists.newArrayListWithCapacity(newConsumerCount);
        int actualOldConsumerCount = -1;
        for (int i = 0; i < newConsumerCount; ++i) {
          StatefulQueueConsumer consumer;
          if (partitionerType != PartitionerType.HASH) {
            consumer = new StatefulQueueConsumer(i, groupId, newConsumerCount, config);
          } else {
            consumer = new StatefulQueueConsumer(i, groupId, newConsumerCount, "", hashKey, config);
          }
          consumers.add(consumer);
          debugCollector.write(String.format("Running configure...%n"));
          int oldConsumerCount = queue.configure(consumer, getDirtyPointer());
          if (oldConsumerCount >= 0) {
            actualOldConsumerCount = oldConsumerCount;
          }
        }
        debugCollector.write(String.format("Old consumer count = %d, new consumer count = %s%n",
                                           actualOldConsumerCount, newConsumerCount));
        assertEquals(debugCollector.toString(), expectedOldConsumerCount, actualOldConsumerCount);

        // Dequeue entries with random batch size and random consumers each time
        int numTriesThisRun = 0;
        int numDequeuesThisRun = 0;
        List<StatefulQueueConsumer> workingConsumerList = Lists.newLinkedList(consumers);
        while (!workingConsumerList.isEmpty()) {
          QueueConsumer consumer = workingConsumerList.remove(random.nextInt(workingConsumerList.size()));
          //QueueConsumer consumer = workingConsumerList.remove(0);
          int curBatchSize = random.nextInt(perConsumerDequeueBatchSize + 1);
          //int curBatchSize = perConsumerDequeueBatchSize;
          debugCollector.write(String.format("Current batch size = %d%n", curBatchSize));

          for (int i = 0; i < curBatchSize; ++i) {
            ++numTriesThisRun;
            DequeueResult result = queue.dequeue(consumer, getDirtyPointer());
            debugCollector.write(consumer.getInstanceId() + " dequeued " +
                                   (result.isEmpty() ? "<empty>" : "" + result.getEntryPointer().getEntryId()) +
                                   ", state: " + consumer.getQueueState() + "\n");
            if (result.isEmpty()) {
              break;
            }
            ++numDequeuesThisRun;
            actualEntries.add(Bytes.toInt(result.getEntry().getData()));
            actualPrintEntries.add(consumer.getInstanceId() + ":" + Bytes.toInt(result.getEntry().getData()));
            queue.ack(result.getEntryPointer(), consumer, transaction);
            //            queue.finalize(result.getEntryPointer(), consumer, 1, dirtyTxn);
            assertTrue(debugCollector.toString(),
                       condition.check(
                         result.getEntryPointer().getEntryId(),
                         newConsumerCount,
                         consumer.getInstanceId(),
                         (int) result.getEntryPointer().getEntryId()
                       ));
          }
          actualEntries.add(-1);
        }
        debugCollector.write(String.format("%s%n", actualPrintEntries));
        debugCollector.write(String.format("%s%n", actualEntries));
        sortedActualEntries = Lists.newArrayList(actualEntries);
        Collections.sort(sortedActualEntries);
        debugCollector.write(String.format("%s%n", sortedActualEntries));

        // If all consumers report queue empty then stop
        if (numDequeuesThisRun == 0 && numTriesThisRun >= consumers.size()) {
          sortedActualEntries.removeAll(Lists.newArrayList(-1));
          debugCollector.write(String.format("Expected: %s%n", expectedEntries));
          debugCollector.write(String.format("Actual:   %s%n", sortedActualEntries));
          break loop;
        }
        expectedOldConsumerCount = newConsumerCount;
      }
    }

    oracle.commitTransaction(transaction);

    // Make sure the queue is empty
    for (QueueConsumer consumer : consumers) {
      DequeueResult result = queue.dequeue(consumer, getDirtyPointer());
      assertTrue(debugCollector.toString(), result.isEmpty());
    }

    assertEquals(debugCollector.toString(), expectedEntries, sortedActualEntries);
  }

  @Test
  public void testEvictOnAck_OneGroup() throws Exception {
    QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer consumer = new StatefulQueueConsumer(0, 0, 1, config);

    // first try with evict-on-ack off
    TTQueue queueNormal = createQueue();
    queueNormal.configure(consumer, getDirtyPointer());
    int numGroups = -1;

    // enqueue 10 things
    for (int i = 0; i < 10; i++) {
      Transaction t = oracle.startTransaction(true);
      queueNormal.enqueue(new QueueEntry(Bytes.toBytes(i)), t);
      oracle.commitTransaction(t);
    }

    // dequeue/ack/finalize 10 things w/ numGroups=-1
    for (int i = 0; i < 10; i++) {
      Transaction t = oracle.startTransaction(true);
      DequeueResult result =
        queueNormal.dequeue(consumer, t.getReadPointer());
      Assert.assertFalse(result.isEmpty());
      queueNormal.ack(result.getEntryPointer(), consumer, t);
      queueNormal.finalize(result.getEntryPointer(), consumer, numGroups, t);
      oracle.commitTransaction(t);
    }

    // dequeue is empty
    assertTrue(
      queueNormal.dequeue(consumer, getDirtyPointer()).isEmpty());

    // dequeue with new consumer still has entries (expected)
    consumer = new QueueConsumer(0, 1, 1, config);
    queueNormal.configure(consumer, getDirtyPointer());
    DequeueResult result = queueNormal.dequeue(consumer, getDirtyPointer());
    assertFalse(result.isEmpty());
    assertEquals(0, Bytes.toInt(result.getEntry().getData()));

    // now do it again with evict-on-ack turned on
    TTQueue queueEvict = createQueue();
    numGroups = 1;
    consumer = new StatefulQueueConsumer(0, 0, 1, config);
    queueEvict.configure(consumer, getDirtyPointer());

    // enqueue 10 things
    for (int i = 0; i < 10; i++) {
      Transaction t = oracle.startTransaction(true);
      queueEvict.enqueue(new QueueEntry(Bytes.toBytes(i)), t);
      oracle.commitTransaction(t);
    }

    // dequeue/ack/finalize 10 things w/ numGroups=1
    for (int i = 0; i < 10; i++) {
      Transaction t = oracle.startTransaction(true);
      result = queueEvict.dequeue(consumer, t.getReadPointer());
      queueEvict.ack(result.getEntryPointer(), consumer, t);
      oracle.commitTransaction(t);
      queueEvict.finalize(result.getEntryPointer(), consumer, numGroups, t);
    }

    // dequeue is empty
    assertTrue(
      queueEvict.dequeue(consumer, getDirtyPointer()).isEmpty());

    // dequeue with new consumer should not have more than one entry, since the entry that consumerReadPointer points
    // to will not get finalized
    consumer = new QueueConsumer(0, 2, 1, config);
    queueEvict.configure(consumer, getDirtyPointer());
    Transaction t = oracle.startTransaction(true);
    result = queueEvict.dequeue(consumer, getDirtyPointer());
    queueEvict.ack(result.getEntryPointer(), consumer, t);
    oracle.commitTransaction(t);
    result = queueEvict.dequeue(consumer, getDirtyPointer());
    assertTrue(result.toString(), result.isEmpty());
  }

  @Test
  public void testEvictOnAck_ThreeGroups() throws Exception {
    TTQueue queue = createQueue();
    final boolean singleEntry = true;

    QueueConfig config = new QueueConfig(PartitionerType.FIFO, singleEntry);
    QueueConsumer consumer1 = new StatefulQueueConsumer(0, 2, 1, config);
    queue.configure(consumer1, TransactionOracle.DIRTY_READ_POINTER);
    QueueConsumer consumer2 = new StatefulQueueConsumer(0, 1, 1, config);
    queue.configure(consumer2, TransactionOracle.DIRTY_READ_POINTER);
    QueueConsumer consumer3 = new StatefulQueueConsumer(0, 0, 1, config);
    queue.configure(consumer3, TransactionOracle.DIRTY_READ_POINTER);

    // enable evict-on-ack for 3 groups
    int numGroups = 3;

    // enqueue 10 things
    for (int i = 0; i < 10; i++) {
      Transaction t = oracle.startTransaction(true);
      queue.enqueue(new QueueEntry(Bytes.toBytes(i)), t);
      oracle.commitTransaction(t);
    }

    // dequeue/ack/finalize 10 things w/ group1 and numGroups=3
    for (int i = 0; i < 10; i++) {
      Transaction t = oracle.startTransaction(true);
      DequeueResult result =
        queue.dequeue(consumer1, oracle.getReadPointer());
      assertTrue(Bytes.equals(Bytes.toBytes(i), result.getEntry().getData()));
      queue.ack(result.getEntryPointer(), consumer1, t);
      oracle.commitTransaction(t);
      queue.finalize(result.getEntryPointer(), consumer1, numGroups, t);
    }

    // dequeue is empty
    assertTrue(
      queue.dequeue(consumer1, getDirtyPointer()).isEmpty());

    // dequeue with consumer2 still has entries (expected)
    assertFalse(
      queue.dequeue(consumer2, getDirtyPointer()).isEmpty());

    // dequeue everything with consumer2
    for (int i = 0; i < 10; i++) {
      Transaction t = oracle.startTransaction(true);
      DequeueResult result =
        queue.dequeue(consumer2, t.getReadPointer());
      assertTrue(Bytes.equals(Bytes.toBytes(i), result.getEntry().getData()));
      queue.ack(result.getEntryPointer(), consumer2, t);
      oracle.commitTransaction(t);
      queue.finalize(result.getEntryPointer(), consumer2, numGroups, t);
    }

    // dequeue is empty
    assertTrue(
      queue.dequeue(consumer2, getDirtyPointer()).isEmpty());

    // dequeue with consumer3 still has entries (expected)
    assertFalse(
      queue.dequeue(consumer3, getDirtyPointer()).isEmpty());

    // dequeue everything except the last entry with consumer3
    for (int i = 0; i < 9; i++) {
      Transaction t = oracle.startTransaction(true);
      DequeueResult result =
        queue.dequeue(consumer3, t.getReadPointer());
      assertTrue(Bytes.equals(Bytes.toBytes(i), result.getEntry().getData()));
      queue.ack(result.getEntryPointer(), consumer3, t);
      oracle.commitTransaction(t);
      queue.finalize(result.getEntryPointer(), consumer3, numGroups, t);
    }

    // now the first 8 entries should have been physically evicted!

    // create a new consumer and dequeue, should get the 9th entry!
    QueueConsumer consumer4 = new QueueConsumer(0, 3, 1, config);
    Transaction t = oracle.startTransaction(true);
    queue.configure(consumer4, oracle.getReadPointer());
    DequeueResult result = queue.dequeue(consumer4, t.getReadPointer());
    assertFalse(result.isEmpty());
    assertTrue("Expected 8 but was " + Bytes.toInt(result.getEntry().getData()),
               Bytes.equals(Bytes.toBytes(8), result.getEntry().getData()));
    queue.ack(result.getEntryPointer(), consumer4, t);
    oracle.commitTransaction(t);
    queue.finalize(result.getEntryPointer(), consumer4, ++numGroups, t); // numGroups=4

    // dequeue again with consumer4 should get 9
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer4, getDirtyPointer());
    assertEquals(9, Bytes.toInt(result.getEntry().getData()));
    queue.ack(result.getEntryPointer(), consumer4, t);
    oracle.commitTransaction(t);

    // dequeue again should be empty on consumer4
    result = queue.dequeue(consumer4, getDirtyPointer());
    assertTrue(result.isEmpty());

    // dequeue is empty for 1 and 2
    assertTrue(
      queue.dequeue(consumer1, getDirtyPointer()).isEmpty());
    assertTrue(
      queue.dequeue(consumer2, getDirtyPointer()).isEmpty());

    // consumer 3 still gets entry 9
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer3, t.getReadPointer());
    assertTrue("Expected 9 but was " + Bytes.toInt(result.getEntry().getData()),
               Bytes.equals(Bytes.toBytes(9), result.getEntry().getData()));
    queue.ack(result.getEntryPointer(), consumer3, t);
    oracle.commitTransaction(t);
    // finalize now with numGroups=4
    queue.finalize(result.getEntryPointer(), consumer3, numGroups, t);

    // everyone is empty now!
    assertTrue(
      queue.dequeue(consumer1, getDirtyPointer()).isEmpty());
    assertTrue(
      queue.dequeue(consumer2, getDirtyPointer()).isEmpty());
    assertTrue(
      queue.dequeue(consumer3, getDirtyPointer()).isEmpty());
    assertTrue(
      queue.dequeue(consumer4, getDirtyPointer()).isEmpty());
  }

  @Test
  public void testBatchSyncDisjoint() throws OperationException {
    testBatchSyncDisjoint(PartitionerType.HASH, false);
    testBatchSyncDisjoint(PartitionerType.HASH, true);
    testBatchSyncDisjoint(PartitionerType.ROUND_ROBIN, false);
    testBatchSyncDisjoint(PartitionerType.ROUND_ROBIN, true);
  }

  public void testBatchSyncDisjoint(PartitionerType partitioner, boolean simulateCrash)
    throws OperationException {

    TTQueue queue = createQueue();
    final int numGroups = 1;

    // enqueue 100
    String hashKey = "h";
    QueueEntry[] entries = new QueueEntry[100];
    for (int i = 1; i <= entries.length; i++) {
      entries[i - 1] = new QueueEntry(Collections.singletonMap(hashKey, i), Bytes.toBytes(i));
    }
    Transaction t = oracle.startTransaction(true);
    queue.enqueue(entries, t);
    oracle.commitTransaction(t);

    // we will dequeue with the given partitioning, two consumers in group, hence returns every other entry
    long groupId = 42;
    String groupName = "group";
    QueueConfig config = new QueueConfig(partitioner, true, 15, true);
    QueueConsumer consumer = simulateCrash ?
      // stateless consumer requires reconstruction of state every time, like after a crash
      new QueueConsumer(0, groupId, 2, groupName, hashKey, config) :
      new StatefulQueueConsumer(0, groupId, 2, groupName, hashKey, config);
    queue.configure(consumer, oracle.getReadPointer());

    // dequeue 15 should return entries: 2, 4, .., 30
    t = oracle.startTransaction(true);
    DequeueResult result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(2 * (i + 1), Bytes.toInt(entries[i].getData()));
    }

    // dequeue 15 again, should return the same entries
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(2 * (i + 1), Bytes.toInt(entries[i].getData()));
    }

    // ack all entries received, and then unack them
    t = oracle.startTransaction(true);
    QueueEntryPointer[] pointers = result.getEntryPointers();
    queue.ack(pointers, consumer, t);
    queue.unack(pointers, consumer, t);
    oracle.commitTransaction(t);

    // dequeue 15 again, should return the same entries
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(2 * (i + 1), Bytes.toInt(entries[i].getData()));
    }

    // ack all entries received, and then finalize them
    t = oracle.startTransaction(true);
    pointers = result.getEntryPointers();
    queue.ack(pointers, consumer, t);
    oracle.commitTransaction(t);
    queue.finalize(pointers, consumer, numGroups, t);

    // dequeue 15 again, should now return new ones 32, 34, .. 60
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(2 * (16 + i), Bytes.toInt(entries[i].getData()));
    }

    // ack 10 + finalize
    t = oracle.startTransaction(true);
    pointers = Arrays.copyOf(result.getEntryPointers(), 10);
    queue.ack(pointers, consumer, t);
    oracle.commitTransaction(t);
    queue.finalize(pointers, consumer, numGroups, t);

    // dequeue 15 again, should return the 5 previous ones again: 52, 54, ..., 60
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(5, entries.length);
    for (int i = 0; i < 5; i++) {
      assertEquals(2 * (26 + i), Bytes.toInt(entries[i].getData()));
    }

    // ack the 5 + finalize
    t = oracle.startTransaction(true);
    pointers = result.getEntryPointers();
    queue.ack(pointers, consumer, t);
    oracle.commitTransaction(t);
    queue.finalize(pointers, consumer, numGroups, t);

    // dequeue 15 again, should return 15 new ones: 62, ..., 90
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(2 * (31 + i), Bytes.toInt(entries[i].getData()));
    }

    // now we change the batch size for the consumer to 12 (reconfigure)
    config = new QueueConfig(partitioner, true, 12, true);
    consumer = simulateCrash ?
      new QueueConsumer(0, groupId, 2, groupName, hashKey, config) :
      new StatefulQueueConsumer(0, groupId, 2, groupName, hashKey, config);
    queue.configure(consumer, oracle.getReadPointer());

    // dequeue 12 with new consumer, should return the first 12 of previous 15: 62, ..., 84
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(12, entries.length);
    for (int i = 0; i < 12; i++) {
      assertEquals(2 * (31 + i), Bytes.toInt(entries[i].getData()));
    }

    // ack all 12 + finalize
    t = oracle.startTransaction(true);
    pointers = result.getEntryPointers();
    queue.ack(pointers, consumer, t);
    oracle.commitTransaction(t);
    queue.finalize(pointers, consumer, numGroups, t);

    // dequeue 12 again, should return the remaining 3  of previous 15: 86, 88, 90
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(3, entries.length);
    for (int i = 0; i < 3; i++) {
      assertEquals(2 * (43 + i), Bytes.toInt(entries[i].getData()));
    }

    // ack all of them + finalize
    t = oracle.startTransaction(true);
    pointers = result.getEntryPointers();
    queue.ack(pointers, consumer, t);
    oracle.commitTransaction(t);
    queue.finalize(pointers, consumer, numGroups, t);

    // dequeue another 12, should return only 5 remaining: 92, 94, ..., 100
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(5, entries.length);
    for (int i = 0; i < 5; i++) {
      assertEquals(2 * (46 + i), Bytes.toInt(entries[i].getData()));
    }

    // ack all of them + finalize
    t = oracle.startTransaction(true);
    pointers = result.getEntryPointers();
    queue.ack(pointers, consumer, t);
    oracle.commitTransaction(t);
    queue.finalize(pointers, consumer, numGroups, t);

    // dequeue should return empty
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testBatchSyncFifo() throws OperationException {
    testBatchSyncFifo(false);
    testBatchSyncFifo(true);
  }

  public void testBatchSyncFifo(boolean simulateCrash) throws OperationException {

    TTQueue queue = createQueue();

    // enqueue a batch
    QueueEntry[] entries = new QueueEntry[75];
    for (int i = 1; i <= entries.length; i++) {
      entries[i - 1] = new QueueEntry(Bytes.toBytes(i));
    }
    Transaction t = oracle.startTransaction(true);
    queue.enqueue(entries, t);
    oracle.commitTransaction(t);

    // we will dequeue with fifo partitioning, two consumers in group, hence returns every other entry
    long groupId = 42;
    String groupName = "group";
    QueueConfig config = new QueueConfig(FIFO, true, 15, true);
    QueueConsumer consumer1 = simulateCrash ?
      new QueueConsumer(0, groupId, 2, groupName, config) :
      new StatefulQueueConsumer(0, groupId, 2, groupName, config);
    QueueConsumer consumer2 = simulateCrash ?
      new QueueConsumer(1, groupId, 2, groupName, config) :
      new StatefulQueueConsumer(1, groupId, 2, groupName, config);
    queue.configure(consumer1, oracle.getReadPointer());

    // dequeue 15 should return first 15 entries: 1, 2, .., 15
    t = oracle.startTransaction(true);
    DequeueResult result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(i + 1, Bytes.toInt(entries[i].getData()));
    }

    // dequeue 15 again, should return the same entries
    t = oracle.startTransaction(true);
    result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(i + 1, Bytes.toInt(entries[i].getData()));
    }

    // attempt to ack with different consumer -> fail
    t = oracle.startTransaction(true);
    QueueEntryPointer[] pointers = result1.getEntryPointers();
    try {
      queue.ack(pointers, consumer2, t);
      fail("ack sould have failed due to wrong consumer");
    } catch (OperationException e) {
      // expect ILLEGAL_ACK
      if (e.getStatus() != StatusCode.ILLEGAL_ACK) {
        throw e;
      }
    }
    oracle.commitTransaction(t);

    // dequeue with other consumer should now return next 15 entries: 16, 17, .., 30
    t = oracle.startTransaction(true);
    DequeueResult result2 = queue.dequeue(consumer2, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result2.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(i + 16, Bytes.toInt(entries[i].getData()));
    }

    // ack all entries received, and then unack them
    t = oracle.startTransaction(true);
    pointers = result1.getEntryPointers();
    queue.ack(pointers, consumer1, t);
    queue.unack(pointers, consumer1, t);
    oracle.commitTransaction(t);

    // dequeue 15 again, should return the same entries
    t = oracle.startTransaction(true);
    result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(i + 1, Bytes.toInt(entries[i].getData()));
    }

    // ack all entries received, and then finalize them
    t = oracle.startTransaction(true);
    pointers = result1.getEntryPointers();
    queue.ack(pointers, consumer1, t);
    queue.finalize(pointers, consumer1, 2, t);
    oracle.commitTransaction(t);

    // dequeue 15 again, should now return new ones 31, 32, .. 45
    t = oracle.startTransaction(true);
    result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(31 + i, Bytes.toInt(entries[i].getData()));
    }

    // ack 10 + finalize
    t = oracle.startTransaction(true);
    pointers = Arrays.copyOf(result1.getEntryPointers(), 10);
    queue.ack(pointers, consumer1, t);
    queue.finalize(pointers, consumer1, 2, t);
    oracle.commitTransaction(t);

    // dequeue 15 again, should return the 5 previous ones again: 41, 42, ..., 45
    t = oracle.startTransaction(true);
    result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertEquals(5, entries.length);
    for (int i = 0; i < 5; i++) {
      assertEquals(41 + i, Bytes.toInt(entries[i].getData()));
    }

    // ack the 5 + finalize
    t = oracle.startTransaction(true);
    pointers = result1.getEntryPointers();
    queue.ack(pointers, consumer1, t);
    queue.finalize(pointers, consumer1, 2, t);
    oracle.commitTransaction(t);

    // dequeue 15 again, should return 15 new ones: 46, ..., 60
    t = oracle.startTransaction(true);
    result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(46 + i, Bytes.toInt(entries[i].getData()));
    }

    // now we change the batch size for the consumer to 12 (reconfigure)
    config = new QueueConfig(FIFO, true, 12, true);
    consumer1 = simulateCrash ?
      new QueueConsumer(0, groupId, 2, groupName, config) :
      new StatefulQueueConsumer(0, groupId, 2, groupName, config);
    consumer2 = simulateCrash ?
      new QueueConsumer(1, groupId, 2, groupName, config) :
      new StatefulQueueConsumer(1, groupId, 2, groupName, config);
    queue.configure(consumer1, oracle.getReadPointer());

    // dequeue 12 with new consumer, should return the first 12 of previous 15: 46, 47, ..., 57
    t = oracle.startTransaction(true);
    result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertEquals(12, entries.length);
    for (int i = 0; i < 12; i++) {
      assertEquals(46 + i, Bytes.toInt(entries[i].getData()));
    }

    // dequeue 12 with new consumer 2, should return the first 12 of previous: 16, 17, ..., 27
    t = oracle.startTransaction(true);
    result2 = queue.dequeue(consumer2, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result2.isEmpty());
    entries = result2.getEntries();
    assertEquals(12, entries.length);
    for (int i = 0; i < 12; i++) {
      assertEquals(16 + i, Bytes.toInt(entries[i].getData()));
    }

    // ack all 12 + finalize
    t = oracle.startTransaction(true);
    pointers = result1.getEntryPointers();
    queue.ack(pointers, consumer1, t);
    queue.finalize(pointers, consumer1, 2, t);
    pointers = result2.getEntryPointers();
    queue.ack(pointers, consumer2, t);
    queue.finalize(pointers, consumer2, 2, t);
    oracle.commitTransaction(t);

    // dequeue 12 again, should return the remaining 3  of previous 15: 58, 59, 60
    t = oracle.startTransaction(true);
    result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertEquals(3, entries.length);
    for (int i = 0; i < 3; i++) {
      assertEquals(58 + i, Bytes.toInt(entries[i].getData()));
    }

    // dequeue 12 again with consumer 2, should return the remaining 3  of previous 15: 28, 29, 30
    t = oracle.startTransaction(true);
    result2 = queue.dequeue(consumer2, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result2.isEmpty());
    entries = result2.getEntries();
    assertEquals(3, entries.length);
    for (int i = 0; i < 3; i++) {
      assertEquals(28 + i, Bytes.toInt(entries[i].getData()));
    }

    // ack all of them + finalize
    t = oracle.startTransaction(true);
    pointers = result1.getEntryPointers();
    queue.ack(pointers, consumer1, t);
    queue.finalize(pointers, consumer1, 2, t);
    pointers = result2.getEntryPointers();
    queue.ack(pointers, consumer2, t);
    queue.finalize(pointers, consumer2, 2, t);
    oracle.commitTransaction(t);

    // dequeue another 12, should return some (around 5) of the 15 remaining: 61, 62, ...
    t = oracle.startTransaction(true);
    result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertTrue(entries.length > 2 && entries.length < 10);
    for (int i = 0; i < entries.length; i++) {
      assertEquals(61 + i, Bytes.toInt(entries[i].getData()));
    }
  }

  @Test
  public void testBatchAsyncDisjoint() throws OperationException {
    testBatchAsyncDisjoint(PartitionerType.HASH);
    testBatchAsyncDisjoint(PartitionerType.ROUND_ROBIN);
  }

  public void testBatchAsyncDisjoint(PartitionerType partitioner) throws OperationException {

    TTQueue queue = createQueue();

    // enqueue 100
    String hashKey = "h";
    QueueEntry[] entries = new QueueEntry[100];
    for (int i = 1; i <= entries.length; i++) {
      entries[i - 1] = new QueueEntry(Collections.singletonMap(hashKey, i), Bytes.toBytes(i));
    }
    Transaction t = oracle.startTransaction(true);
    queue.enqueue(entries, t);
    oracle.commitTransaction(t);

    // we will dequeue with the given partitioning, two consumers in group, hence returns every other entry
    long groupId = 42;
    String groupName = "group";
    QueueConfig config = new QueueConfig(partitioner, false, 15, true);
    QueueConsumer consumer0 = new StatefulQueueConsumer(0, groupId, 2, groupName, hashKey, config);
    QueueConsumer consumer = new StatefulQueueConsumer(1, groupId, 2, groupName, hashKey, config);
    queue.configure(consumer0, oracle.getReadPointer()); // we must configure with instance #0

    // dequeue 15 -> 1 .. 29
    t = oracle.startTransaction(true);
    DequeueResult result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(2 * i + 1, Bytes.toInt(entries[i].getData()));
    }

    // dequeue another 15 -> 31 .. 59
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(2 * i + 31, Bytes.toInt(entries[i].getData()));
    }

    // ack second 15 + finalize
    t = oracle.startTransaction(true);
    QueueEntryPointer[] pointers = result.getEntryPointers();
    queue.ack(pointers, consumer, t);
    queue.finalize(pointers, consumer, 2, t);
    oracle.commitTransaction(t);

    // ack second 15 again -> error
    t = oracle.startTransaction(true);
    try {
      queue.ack(pointers, consumer, t);
      fail("acking for the second time should fail");
    } catch (OperationException e) {
      // expect ILLEGAL_ACK
      if (e.getStatus() != StatusCode.ILLEGAL_ACK) {
        throw e;
      }
    }
    oracle.commitTransaction(t);

    // dequeue another 15 -> 61 .. 89
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(2 * i + 61, Bytes.toInt(entries[i].getData()));
    }

    // ack first 10
    t = oracle.startTransaction(true);
    pointers = Arrays.copyOf(result.getEntryPointers(), 10);
    queue.ack(pointers, consumer, t);
    queue.finalize(pointers, consumer, 2, t);
    oracle.commitTransaction(t);

    // start new consumer (simulate crash)
    config = new QueueConfig(partitioner, false, 10, true);
    consumer = new StatefulQueueConsumer(1, groupId, 2, groupName, hashKey, config);
    queue.configure(consumer, oracle.getReadPointer());

    // dequeue 10 -> 1 .. 19
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(10, entries.length);
    for (int i = 0; i < 10; i++) {
      assertEquals(2 * i + 1, Bytes.toInt(entries[i].getData()));
    }
    QueueEntryPointer[] pointers1 = result.getEntryPointers();

    // dequeue 10 -> 21 .. 29, 81 ... 89
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(10, entries.length);
    for (int i = 0; i < 5; i++) {
      assertEquals(2 * i + 21, Bytes.toInt(entries[i].getData()));
    }
    for (int i = 5; i < 10; i++) {
      assertEquals(2 * i + 71, Bytes.toInt(entries[i].getData()));
    }
    QueueEntryPointer[] pointers2 = result.getEntryPointers();

    // dequeue 10 -> 91 .. 99
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    entries = result.getEntries();
    assertEquals(5, entries.length);
    for (int i = 0; i < 5; i++) {
      assertEquals(2 * i + 91, Bytes.toInt(entries[i].getData()));
    }
    QueueEntryPointer[] pointers3 = result.getEntryPointers();

    // dequeue 10 -> empty
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertTrue(result.isEmpty());

    // ack 1 .. 29
    t = oracle.startTransaction(true);
    pointers = Arrays.copyOf(pointers1, pointers1.length + 5);
    System.arraycopy(pointers2, 0, pointers, pointers1.length, 5);
    queue.ack(pointers, consumer, t);
    queue.finalize(pointers, consumer, 2, t);
    oracle.commitTransaction(t);

    // ack 81 .. 99
    t = oracle.startTransaction(true);
    // note pointers will be out of order
    pointers = Arrays.copyOf(pointers3, pointers3.length + 5);
    System.arraycopy(pointers2, 5, pointers, pointers3.length, 5);
    queue.ack(pointers, consumer, t);
    queue.finalize(pointers, consumer, 2, t);
    oracle.commitTransaction(t);

    // new consumer
    // start new consumer (simulate crash)
    config = new QueueConfig(partitioner, false, 20, true);
    consumer = new StatefulQueueConsumer(1, groupId, 2, groupName, hashKey, config);
    queue.configure(consumer, oracle.getReadPointer());

    // dequeue -> empty
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testBatchAsyncFifo() throws OperationException {

    TTQueue queue = createQueue();

    // enqueue 100
    QueueEntry[] entries = new QueueEntry[100];
    for (int i = 1; i <= entries.length; i++) {
      entries[i - 1] = new QueueEntry(Bytes.toBytes(i));
    }
    Transaction t = oracle.startTransaction(true);
    queue.enqueue(entries, t);
    oracle.commitTransaction(t);

    // we will dequeue with the given partitioning, two consumers in group, hence returns every other entry
    long groupId = 42;
    String groupName = "group";
    QueueConfig config = new QueueConfig(FIFO, false, 15, true);
    QueueConsumer consumer1 = new StatefulQueueConsumer(0, groupId, 2, groupName, config);
    QueueConsumer consumer2 = new StatefulQueueConsumer(1, groupId, 2, groupName, config);
    queue.configure(consumer1, oracle.getReadPointer());
    queue.configure(consumer2, oracle.getReadPointer());

    // dequeue 15 with first consumer -> 1 .. 15
    t = oracle.startTransaction(true);
    DequeueResult result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(i + 1, Bytes.toInt(entries[i].getData()));
    }

    // dequeue another 15 -> 16 .. 30
    t = oracle.startTransaction(true);
    result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(i + 16, Bytes.toInt(entries[i].getData()));
    }

    // dequeue 15 with second consumer -> 31 .. 45
    t = oracle.startTransaction(true);
    DequeueResult result2 = queue.dequeue(consumer2, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result2.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(i + 31, Bytes.toInt(entries[i].getData()));
    }

    // ack second batch with second consumer -> error
    t = oracle.startTransaction(true);
    QueueEntryPointer[] pointers = result1.getEntryPointers();
    try {
      queue.ack(pointers, consumer2, t);
      fail("acking with wrong consumer should fail");
    } catch (OperationException e) {
      // expect ILLEGAL_ACK
      if (e.getStatus() != StatusCode.ILLEGAL_ACK) {
        throw e;
      }
    }
    oracle.commitTransaction(t);

    // ack second batch with first consumer -> ok
    t = oracle.startTransaction(true);
    queue.ack(pointers, consumer1, t);
    queue.finalize(pointers, consumer1, 2, t);
    oracle.commitTransaction(t);

    // ack second batch again -> error
    t = oracle.startTransaction(true);
    try {
      queue.ack(pointers, consumer1, t);
      fail("acking for the second time should fail");
    } catch (OperationException e) {
      // expect ILLEGAL_ACK
      if (e.getStatus() != StatusCode.ILLEGAL_ACK) {
        throw e;
      }
    }
    oracle.commitTransaction(t);

    // simulate crash of consumer 1 - new consumer with batch size 10
    config = new QueueConfig(FIFO, false, 10, true);
    consumer1 = new StatefulQueueConsumer(0, groupId, 2, groupName, config);

    // dequeue 10 with first consumer 1 .. 10
    t = oracle.startTransaction(true);
    result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertEquals(10, entries.length);
    for (int i = 0; i < 10; i++) {
      assertEquals(i + 1, Bytes.toInt(entries[i].getData()));
    }

    // ack these 10
    t = oracle.startTransaction(true);
    pointers = result1.getEntryPointers();
    queue.ack(pointers, consumer1, t);
    queue.finalize(pointers, consumer1, 2, t);
    oracle.commitTransaction(t);

    // dequeue 10 with first consumer 11 .. 15
    t = oracle.startTransaction(true);
    result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertEquals(5, entries.length);
    for (int i = 0; i < 5; i++) {
      assertEquals(i + 11, Bytes.toInt(entries[i].getData()));
    }
    pointers = Arrays.copyOf(result1.getEntryPointers(), 15);

    // dequeue 10 with first consumer 46 .. 55
    t = oracle.startTransaction(true);
    result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertEquals(10, entries.length);
    for (int i = 0; i < 10; i++) {
      assertEquals(i + 46, Bytes.toInt(entries[i].getData()));
    }
    System.arraycopy(result1.getEntryPointers(), 0, pointers, 5, 10);

    // dequeue 15 with second consumer 56 .. 70
    t = oracle.startTransaction(true);
    result2 = queue.dequeue(consumer2, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result2.isEmpty());
    entries = result2.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(i + 56, Bytes.toInt(entries[i].getData()));
    }

    // ack previous 15 from first consumer at once
    t = oracle.startTransaction(true);
    queue.ack(pointers, consumer1, t);
    queue.finalize(pointers, consumer1, 2, t);
    oracle.commitTransaction(t);

    pointers = Arrays.copyOf(result2.getEntryPointers(), 30);
    // dequeue 15 with second consumer 71 .. 85
    t = oracle.startTransaction(true);
    result2 = queue.dequeue(consumer2, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result2.isEmpty());
    entries = result2.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < 15; i++) {
      assertEquals(i + 71, Bytes.toInt(entries[i].getData()));
    }

    // ack last two batches with second consumer
    System.arraycopy(result2.getEntryPointers(), 0, pointers, 15, 15);
    t = oracle.startTransaction(true);
    queue.ack(pointers, consumer2, t);
    queue.finalize(pointers, consumer2, 2, t);
    oracle.commitTransaction(t);

    // dequeue 10 with first consumer 86 .. ?, less than 10 (only 15 left in queue)
    t = oracle.startTransaction(true);
    result1 = queue.dequeue(consumer1, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result1.isEmpty());
    entries = result1.getEntries();
    assertTrue(1 < entries.length && entries.length < 10);
    for (int i = 0; i < entries.length; i++) {
      assertEquals(i + 86, Bytes.toInt(entries[i].getData()));
    }
    int dequeued = entries.length;

    // deqeuee 15 with second consumer ? .. ?, less than 15
    t = oracle.startTransaction(true);
    result2 = queue.dequeue(consumer2, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result2.isEmpty());
    entries = result2.getEntries();
    assertTrue(1 < entries.length && entries.length < 15);
    for (int i = 0; i < entries.length; i++) {
      assertEquals(i + 86 + dequeued, Bytes.toInt(entries[i].getData()));
    }
    dequeued += entries.length;

    // enqueue another 30
    entries = new QueueEntry[30];
    for (int i = 1; i <= entries.length; i++) {
      entries[i - 1] = new QueueEntry(Bytes.toBytes(i + 100));
    }
    t = oracle.startTransaction(true);
    queue.enqueue(entries, t);
    oracle.commitTransaction(t);

    // dequeue 15 with second -> 15
    t = oracle.startTransaction(true);
    result2 = queue.dequeue(consumer2, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result2.isEmpty());
    entries = result2.getEntries();
    assertEquals(15, entries.length);
    for (int i = 0; i < entries.length; i++) {
      assertEquals(i + 86 + dequeued, Bytes.toInt(entries[i].getData()));
    }
  }

  // this tests that the dequeue can deal with a batch of consecutive, invalid queue entries that is larger than the
  // dequeue batch size. If it does not deal with this condition correctly, it may return empty or even go into an
  // infinite loop!
  @Test//(timeout = 10000)
  public void testSkipBatchofInvalidEntries () throws OperationException {
    TTQueue queue = createQueue();

    // enqueue 20, then invalidate a batch of 10 in the middle
    QueueEntry[] entries = new QueueEntry[20];
    for (int i = 1; i <= entries.length; i++) {
      entries[i - 1] = new QueueEntry("p", i, Bytes.toBytes(i));
    }
    Transaction t = oracle.startTransaction(true);
    EnqueueResult enqResult = queue.enqueue(entries, t);
    queue.invalidate(Arrays.copyOfRange(enqResult.getEntryPointers(), 5, 15), t);
    oracle.commitTransaction(t);

    long groupId = 0;
    for (int batchSize = 1; batchSize < 12; batchSize++) {
      for (boolean returnBatch : new boolean[] { true, false }) {
        for (boolean singleEntry : new boolean[] { true, false }) {
          for (PartitionerType partitioner : new PartitionerType[] { FIFO, ROUND_ROBIN, HASH }) {
            testSkipBatchofInvalidEntries(queue, ++groupId, batchSize, returnBatch, singleEntry, partitioner);
          }
        }
      }
    }
  }

  public void testSkipBatchofInvalidEntries(TTQueue queue, long groupId, int batchSize, boolean returnBatch,
                                            boolean singleEntry, PartitionerType partitioner)
    throws OperationException {

    String groupName = "batch=" + batchSize + ":" + returnBatch + "," + (singleEntry ? "single" : "multi")
      + "," + partitioner;
    // System.out.println(groupName);
    QueueConfig config = new QueueConfig(partitioner, singleEntry, batchSize, returnBatch);
    QueueConsumer consumer = new StatefulQueueConsumer(0, groupId, 2, groupName, "p", config);
    queue.configure(consumer, oracle.getReadPointer());

    int numDequeued = 0;
    while (true) {
      Transaction t = oracle.startTransaction(true);
      DequeueResult result = queue.dequeue(consumer, t.getReadPointer());
      oracle.commitTransaction(t);
      if (result.isEmpty()) {
        break;
      }
      numDequeued += result.getEntries().length;
      t = oracle.startTransaction(true);
      queue.ack(result.getEntryPointers(), consumer, t);
      queue.finalize(result.getEntryPointers(), consumer, 1, t);
      oracle.commitTransaction(t);
    }
    int expected = partitioner == FIFO ? 10 : 5;
    assertEquals("Failure for " + groupName + ":", expected, numDequeued);
  }

  // tests that if we use hash partitioning, we can also consume entries without hash key
  @Test
  public void testHashWithoutHashKey() throws OperationException {
    TTQueue queue = createQueue();

    // enqueue 20
    QueueEntry[] entries = new QueueEntry[20];
    for (int i = 1; i <= entries.length; i++) {
      entries[i - 1] = new QueueEntry(Bytes.toBytes(i));
    }
    Transaction t = oracle.startTransaction(true);
    queue.enqueue(entries, t);
    oracle.commitTransaction(t);

    QueueConfig config = new QueueConfig(HASH, false, 4, true);
    QueueConsumer[] consumers = {
      new StatefulQueueConsumer(0, 17, 2, "or'ly", "non-existent-hash-key", config),
      new StatefulQueueConsumer(1, 17, 2, "or'ly", "non-existent-hash-key", config) };
    queue.configure(consumers[0], oracle.getReadPointer());

    int numDequeued = 0;
    boolean allEmpty;
    do {
      allEmpty = true;
      for (QueueConsumer consumer : consumers) {
        t = oracle.startTransaction(true);
        DequeueResult result = queue.dequeue(consumer, t.getReadPointer());
        oracle.commitTransaction(t);
        if (result.isEmpty()) {
          continue;
        }
        allEmpty = false;
        numDequeued += result.getEntries().length;
        t = oracle.startTransaction(true);
        queue.ack(result.getEntryPointers(), consumer, t);
        queue.finalize(result.getEntryPointers(), consumer, 1, t);
        oracle.commitTransaction(t);
      }
    } while (!allEmpty);

    assertEquals(20, numDequeued);
  }

  // this tests that repeated reconfiguration (without changing the group size - that is not allowed if there are
  // dequeued entries) does not drop the dequeued entries.
  @Test
  public void testRepeatedConfigureDoesNotExpireEntries() throws OperationException {
    TTQueue queue = createQueue();

    // enqueue 20
    QueueEntry[] entries = new QueueEntry[20];
    for (int i = 1; i <= entries.length; i++) {
      entries[i - 1] = new QueueEntry(Bytes.toBytes(i));
    }
    Transaction t = oracle.startTransaction(true);
    queue.enqueue(entries, t);
    oracle.commitTransaction(t);

    QueueConfig config = new QueueConfig(FIFO, true, 20, true);
    QueueConsumer consumer = new StatefulQueueConsumer(0, 432567, 1, "xyz", config);
    queue.configure(consumer, oracle.getReadPointer());

    // dequeue but don't ack
    t = oracle.startTransaction(true);
    DequeueResult result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    assertEquals(20, result.getEntries().length);

    // now reconfigure the consumer with the same group size several times
    for (int i = 0; i < MAX_CRASH_DEQUEUE_TRIES + 30; i++) {
      config = new QueueConfig(FIFO, true, 20 + i, true); // varying the batch size but nothing else
      consumer = new StatefulQueueConsumer(0, 432567, 1, "xyz", config);
      queue.configure(consumer, oracle.getReadPointer());
    }

    // dequeue should still return these entries
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    oracle.commitTransaction(t);
    assertFalse(result.isEmpty());
    assertEquals(20, result.getEntries().length);
  }

  // this tests that all configured groups can be listed
  @Test
  public void testListConfiguredGroups() throws OperationException {
    TTQueue queue = createQueue();

    long[] groupIds = { 0, Long.MIN_VALUE, Long.MAX_VALUE, 1, 42, 4536277 };

    for (long id : groupIds) {
      QueueConfig config = new QueueConfig(FIFO, true, 10, false);
      QueueConsumer consumer = new StatefulQueueConsumer(0, id, 1, "xyz", config);
      queue.configure(consumer, oracle.getReadPointer());
    }
    List<Long> returnedGroupIds = ((TTQueueOnVCTable) queue).listAllConfiguredGroups();
    assertEquals(groupIds.length, returnedGroupIds.size());
    for (long id : groupIds) {
      assertTrue(returnedGroupIds.contains(id));
    }

    // make sure getGroupID does not return an already configured group
    long id = queue.getGroupID();
    assertFalse(returnedGroupIds.contains(id));

    // get the queue info
    QueueInfo info = queue.getQueueInfo();
    assertNotNull(info.getJSONString());
    assertFalse(info.getJSONString().isEmpty());
    // System.out.println(info.getJSONString());
  }

  @Test
  public void testEviction() throws Exception {
    // Eviction should not remove entries dequeued or uncommited acked entries or unacked entries
    TTQueue queue = createQueue();

    // enqueue 20
    QueueEntry[] entries = new QueueEntry[20];
    for (int i = 1; i <= entries.length; ++i) {
      entries[i - 1] = new QueueEntry(Bytes.toBytes(i));
    }
    Transaction t = oracle.startTransaction(true);
    queue.enqueue(entries, t);
    oracle.commitTransaction(t);

    // Create 20 QueueEntryPointers for evicting
    QueueEntryPointer[] entryPointers = new QueueEntryPointer[entries.length];
    for (int i = 1; i <= entryPointers.length; ++i) {
      entryPointers[i - 1] = new QueueEntryPointer("dummyQueueName".getBytes(), i, 0);
    }

    // Store all transactions for committing later
    Transaction[] grp1TxnList = new Transaction[entries.length];
    Transaction[] grp2TxnList = new Transaction[entries.length];

    // Create 2 consumer groups with 2 consumers each, and dequeue
    QueueConsumer grp1Consumer1 = new StatefulQueueConsumer(0, 0, 2, new QueueConfig(ROUND_ROBIN, true, 5, false));
    queue.configure(grp1Consumer1, oracle.getReadPointer());
    QueueConsumer grp1Consumer2 = new StatefulQueueConsumer(1, 0, 2, new QueueConfig(ROUND_ROBIN, true, 5, false));
    queue.configure(grp1Consumer2, oracle.getReadPointer());
    QueueConsumer grp2Consumer1 = new StatefulQueueConsumer(0, 1, 2, new QueueConfig(ROUND_ROBIN, true, 5, false));
    queue.configure(grp2Consumer1, oracle.getReadPointer());
    QueueConsumer grp2Consumer2 = new StatefulQueueConsumer(1, 1, 2, new QueueConfig(ROUND_ROBIN, true, 5, false));
    queue.configure(grp2Consumer2, oracle.getReadPointer());

    // Group 2, Consumer 2
    for (int i = 1; i <= entries.length; i += 2) {
      t = oracle.startTransaction(true);
      DequeueResult result = queue.dequeue(grp2Consumer2, t.getReadPointer());
      Assert.assertFalse(result.isEmpty());
      Assert.assertEquals(i, Bytes.toInt(result.getEntry().getData()));
      // Eviction should not happen due to dequeue entries
      runFinalize(queue, entryPointers, grp1Consumer1, grp1Consumer2, grp2Consumer1, grp1Consumer2);
      assertFirstEntry(queue, 1);

      // Ack
      queue.ack(result.getEntryPointer(), grp2Consumer2, t);
      // Eviction should not happen due to uncommitted acks
      runFinalize(queue, entryPointers, grp1Consumer1, grp1Consumer2, grp2Consumer1, grp1Consumer2);
      assertFirstEntry(queue, 1);
      grp2TxnList[i - 1] = t;
    }

    // Group 2, Consumer 1
    for (int i = 2; i <= entries.length; i += 2) {
      t = oracle.startTransaction(true);
      DequeueResult result = queue.dequeue(grp2Consumer1, t.getReadPointer());
      Assert.assertFalse(result.isEmpty());
      Assert.assertEquals(i, Bytes.toInt(result.getEntry().getData()));
      // Eviction should not happen due to dequeue entries
      runFinalize(queue, entryPointers, grp1Consumer1, grp1Consumer2, grp2Consumer1, grp1Consumer2);
      assertFirstEntry(queue, 1);

      // Ack
      queue.ack(result.getEntryPointer(), grp2Consumer1, t);
      // Eviction should not happen due to uncommitted acks
      runFinalize(queue, entryPointers, grp1Consumer1, grp1Consumer2, grp2Consumer1, grp1Consumer2);
      assertFirstEntry(queue, 1);
      grp2TxnList[i - 1] = t;
    }

    // Group 1, Consumer 2
    for (int i = 1; i <= entries.length; i += 2) {
      t = oracle.startTransaction(true);
      DequeueResult result = queue.dequeue(grp1Consumer2, t.getReadPointer());
      Assert.assertFalse(result.isEmpty());
      Assert.assertEquals(i, Bytes.toInt(result.getEntry().getData()));
      // Eviction should not happen due to dequeue entries
      runFinalize(queue, entryPointers, grp1Consumer1, grp1Consumer2, grp2Consumer1, grp1Consumer2);
      assertFirstEntry(queue, 1);

      // Ack
      queue.ack(result.getEntryPointer(), grp1Consumer2, t);
      // Eviction should not happen due to uncommitted acks
      runFinalize(queue, entryPointers, grp1Consumer1, grp1Consumer2, grp2Consumer1, grp1Consumer2);
      assertFirstEntry(queue, 1);
      grp1TxnList[i - 1] = t;
    }

    // Group 1, Consumer 1
    for (int i = 2; i <= entries.length; i += 2) {
      t = oracle.startTransaction(true);
      DequeueResult result = queue.dequeue(grp1Consumer1, t.getReadPointer());
      Assert.assertFalse(result.isEmpty());
      Assert.assertEquals(i, Bytes.toInt(result.getEntry().getData()));
      // Eviction should not happen due to dequeue entries
      runFinalize(queue, entryPointers, grp1Consumer1, grp1Consumer2, grp2Consumer1, grp1Consumer2);
      assertFirstEntry(queue, 1);

      // Ack
      queue.ack(result.getEntryPointer(), grp1Consumer1, t);
      // Eviction should not happen due to uncommitted acks
      runFinalize(queue, entryPointers, grp1Consumer1, grp1Consumer2, grp2Consumer1, grp1Consumer2);
      assertFirstEntry(queue, 1);
      grp1TxnList[i - 1] = t;
    }

    // Commit all acks of Group 2
    for (Transaction t1 : grp2TxnList) {
      oracle.commitTransaction(t1);
      // Run finalize again
      runFinalize(queue, entryPointers, grp1Consumer1, grp1Consumer2, grp2Consumer1, grp1Consumer2);
      assertFirstEntry(queue, 1);
    }

    // Commit all acks of Group 1
    int i = 1;
    for (Transaction t1 : grp1TxnList) {
      oracle.commitTransaction(t1);
      // Run finalize again
      runFinalize(queue, entryPointers, grp1Consumer1, grp1Consumer2, grp2Consumer1, grp1Consumer2);
      // Last 2 entries will not get evicted, due to the way eviction entries are determined using min committed
      // entries of a txn.
      int newFirstEntry = i >= entries.length - 2 ? entries.length - 2 : i + 1;
      assertFirstEntry(queue, newFirstEntry);
      i++;
    }
  }

  private void assertFirstEntry(TTQueue queue, int expectedEntry) throws OperationException {
    QueueConsumer consumer = new StatefulQueueConsumer(0, timeOracle.getTimestamp(), 1,
                                                       new QueueConfig(FIFO, true, 5, false));
    queue.configure(consumer, oracle.getReadPointer());
    DequeueResult dequeueResult = queue.dequeue(consumer, oracle.getReadPointer());
    Assert.assertFalse(dequeueResult.isEmpty());
    Assert.assertEquals(expectedEntry, Bytes.toInt(dequeueResult.getEntry().getData()));
  }

  private void runFinalize(TTQueue queue, QueueEntryPointer[] entryPointers, QueueConsumer grp1Consumer1,
                           QueueConsumer grp1Consumer2, QueueConsumer grp2Consumer1, QueueConsumer grp2Consumer2)
    throws OperationException {
    queue.finalize(entryPointers, grp2Consumer2, 2, new Transaction(getDirtyWriteVersion(),
                                                                    oracle.getReadPointer(), true));
    queue.finalize(entryPointers, grp2Consumer1, 2, new Transaction(getDirtyWriteVersion(),
                                                                    oracle.getReadPointer(), true));
    queue.finalize(entryPointers, grp1Consumer2, 2, new Transaction(getDirtyWriteVersion(),
                                                                    oracle.getReadPointer(), true));
    queue.finalize(entryPointers, grp1Consumer1, 2, new Transaction(getDirtyWriteVersion(),
                                                                    oracle.getReadPointer(), true));

    // Need to run finalize 2 times, since eviction information gets cleared every time we run configure for
    // verification, also the previous finalize with grp1Consumer1 wouldn't write the eviction information till
    // the end of the finalize function, hence (available groups == total groups) check will fail
    queue.finalize(entryPointers, grp1Consumer1, 2, new Transaction(getDirtyWriteVersion(),
                                                                    oracle.getReadPointer(), true));
  }

  @Test
  public void testEvictClaimedEntries() throws Exception {
    // Eviction entry determination should take ClaimedEntryList into account
    TTQueueOnVCTable queue = (TTQueueOnVCTable) createQueue();
    TTQueueOnVCTable.QueueStateImpl queueState = new TTQueueOnVCTable.QueueStateImpl();
    TTQueueOnVCTable.DequeueStrategy fifoDequeueStrategy = queue.new FifoDequeueStrategy();
    queueState.setPartitioner(FIFO);
    queueState.setConsumerReadPointer(10);
    // Min unack entry should be only based on consumerReadPointer
    assertEquals(10, fifoDequeueStrategy.getMinUnAckedEntry(queueState));

    TTQueueOnVCTable.DequeuedEntrySet dequeuedEntrySet = new TTQueueOnVCTable.DequeuedEntrySet(
      Sets.newTreeSet(Lists.newArrayList(new TTQueueOnVCTable.DequeueEntry(9), new TTQueueOnVCTable.DequeueEntry(8))));
    queueState.setDequeueEntrySet(dequeuedEntrySet);
    // Min unack entry should be based on consumerReadPointer, dequeueEntrySet
    assertEquals(8, fifoDequeueStrategy.getMinUnAckedEntry(queueState));

    ClaimedEntryList claimedEntryList = new ClaimedEntryList();
    claimedEntryList.add(5, 6);
    claimedEntryList.add(3, 4);
    queueState.setClaimedEntryList(claimedEntryList);
    // Min unack entry should be based on consumerReadPointer, dequeueEntrySet, ClaimedEntryList
    assertEquals(3, fifoDequeueStrategy.getMinUnAckedEntry(queueState));

    // Clear dequeued entry set
    queueState.setDequeueEntrySet(new TTQueueOnVCTable.DequeuedEntrySet());
    // Min unack entry should still be min of ClaimedEntryList
    assertEquals(3, fifoDequeueStrategy.getMinUnAckedEntry(queueState));

    // Restore dequeued entry set, but clear ClaimedEntryList
    queueState.setDequeueEntrySet(dequeuedEntrySet);
    queueState.setClaimedEntryList(new ClaimedEntryList());
    // Min unack entry should now be min of dequeeudEntrySet
    assertEquals(8, fifoDequeueStrategy.getMinUnAckedEntry(queueState));


    queueState.setReconfigPartitionersList(
      new TTQueueOnVCTable.ReconfigPartitionersList(
        Lists.newArrayList(new TTQueueOnVCTable.ReconfigPartitioner(2, ROUND_ROBIN))));
  }

  @Test
  public void testEvictReconfigPartitionerList() throws Exception {
    // Eviction entry determination should take ClaimedEntryList into account
    TTQueueOnVCTable queue = (TTQueueOnVCTable) createQueue();
    TTQueueOnVCTable.QueueStateImpl queueState = new TTQueueOnVCTable.QueueStateImpl();
    TTQueueOnVCTable.DequeueStrategy fifoDequeueStrategy = queue.new RoundRobinDequeueStrategy();
    queueState.setPartitioner(ROUND_ROBIN);
    queueState.setConsumerReadPointer(10);
    // Min unack entry should be only based on consumerReadPointer
    assertEquals(10, fifoDequeueStrategy.getMinUnAckedEntry(queueState));

    TTQueueOnVCTable.DequeuedEntrySet dequeuedEntrySet = new TTQueueOnVCTable.DequeuedEntrySet(
      Sets.newTreeSet(Lists.newArrayList(new TTQueueOnVCTable.DequeueEntry(9), new TTQueueOnVCTable.DequeueEntry(8))));
    queueState.setDequeueEntrySet(dequeuedEntrySet);
    // Min unack entry should be based on consumerReadPointer, dequeueEntrySet
    assertEquals(8, fifoDequeueStrategy.getMinUnAckedEntry(queueState));

    TTQueueOnVCTable.ReconfigPartitionersList reconfigPartitionersList =
      new TTQueueOnVCTable.ReconfigPartitionersList(Lists.newArrayList(
        new TTQueueOnVCTable.ReconfigPartitioner(7, ROUND_ROBIN),
        new TTQueueOnVCTable.ReconfigPartitioner(8, ROUND_ROBIN)));
    queueState.setReconfigPartitionersList(reconfigPartitionersList);
    queueState.setConsumerReadPointer(7);
    // Min unack entry should be based on consumerReadPointer, dequeueEntrySet, reconfigPartitionersList
    assertEquals(7, fifoDequeueStrategy.getMinUnAckedEntry(queueState));

    // Clear dequeued entry set
    queueState.setDequeueEntrySet(new TTQueueOnVCTable.DequeuedEntrySet());
    // Min unack entry should still be min of ClaimedEntryList
    assertEquals(7, fifoDequeueStrategy.getMinUnAckedEntry(queueState));
  }

  private interface QueueConsumerHolder {
    QueueConsumer getQueueConsumer(QueueConsumer consumer);
  }

  @Test
  public void testQueueStateType() throws Exception {
    // QueueConsumer.StateType.UNINITIALIZED
    // This simulates a consumer crash every time
    List<List<Integer>> expected = Lists.newArrayListWithCapacity(3);
    expected.add(Lists.newArrayList(1, 2, 3, 4, 5));
    expected.add(Lists.newArrayList(1, 2, 3, 4, 5));
    expected.add(Lists.newArrayList(6, 7, 8, 9, 10));
    testQueueStateType(expected.iterator(),
                       new QueueConsumerHolder() {
                         @Override
                         public QueueConsumer getQueueConsumer(QueueConsumer consumer) {
                           consumer.setQueueState(null);
                           consumer.setStateType(QueueConsumer.StateType.UNINITIALIZED);
                           return consumer;
                         }
                       });

    // QueueConsumer.StateType.INITIALIZED
    // The consumer does not crash anytime
    expected = Lists.newArrayListWithCapacity(3);
    expected.add(Lists.newArrayList(1, 2, 3, 4, 5));
    expected.add(Lists.newArrayList(6, 7, 8, 9, 10));
    expected.add(Lists.newArrayList(11, 12, 13, 14, 15));
    testQueueStateType(expected.iterator(), new QueueConsumerHolder() {
      @Override
      public QueueConsumer getQueueConsumer(QueueConsumer consumer) {
        return consumer;
      }
    });

    // QueueConsumer.StateType.NOT_FOUND
    // The consumer does not crash anytime, but cached state disappears!
    expected = Lists.newArrayListWithCapacity(3);
    expected.add(Lists.newArrayList(1, 2, 3, 4, 5));
    expected.add(Lists.newArrayList(6, 7, 8, 9, 10));
    expected.add(Lists.newArrayList(11, 12, 13, 14, 15));
    testQueueStateType(expected.iterator(), new QueueConsumerHolder() {
      @Override
      public QueueConsumer getQueueConsumer(QueueConsumer consumer) {
        consumer.setQueueState(null);
        consumer.setStateType(QueueConsumer.StateType.NOT_FOUND);
        return consumer;
      }
    });
  }

  private void testQueueStateType(Iterator<List<Integer>> expectedDequeues, QueueConsumerHolder consumerHolder)
    throws Exception {
    TTQueue queue = createQueue();

    // enqueue 20 entries
    QueueEntry[] entries = new QueueEntry[20];
    for (int i = 1; i <= entries.length; i++) {
      entries[i - 1] = new QueueEntry(Bytes.toBytes(i));
    }
    Transaction t = oracle.startTransaction(true);
    queue.enqueue(entries, t);
    oracle.commitTransaction(t);

    // Dequeue 5 entries in batch, async mode
    QueueConsumer consumer = new StatefulQueueConsumer(0, 0, 1, new QueueConfig(FIFO, false, 5, true));
    queue.configure(consumer, oracle.getReadPointer());
    Transaction t1 = oracle.startTransaction(true);
    DequeueResult result1 = queue.dequeue(consumerHolder.getQueueConsumer(consumer), t1.getReadPointer());
    assertEquals(QueueConsumer.StateType.INITIALIZED, consumer.getStateType());
    assertDequeueEquals(expectedDequeues.next(), result1.getEntries());

    // Dequeue again without acking
    Transaction t2 = oracle.startTransaction(true);
    DequeueResult result2 = queue.dequeue(consumerHolder.getQueueConsumer(consumer), t2.getReadPointer());
    assertEquals(QueueConsumer.StateType.INITIALIZED, consumer.getStateType());
    assertDequeueEquals(expectedDequeues.next(), result2.getEntries());

    // Ack and commit t2
    queue.ack(result2.getEntryPointers(), consumerHolder.getQueueConsumer(consumer), t2);
    oracle.commitTransaction(t2);

    // Dequeue once more
    Transaction t3 = oracle.startTransaction(true);
    DequeueResult result3 = queue.dequeue(consumerHolder.getQueueConsumer(consumer), t3.getReadPointer());
    assertEquals(QueueConsumer.StateType.INITIALIZED, consumer.getStateType());
    assertDequeueEquals(expectedDequeues.next(), result3.getEntries());

    // Depending on consumer stateType we may have result1 == result2, so no point in acking here

    // Abort transactions t1 and t3
    oracle.abortTransaction(t1);
    oracle.removeTransaction(t1);
    oracle.abortTransaction(t3);
    oracle.removeTransaction(t3);
  }

  private void assertDequeueEquals(List<Integer> expected, QueueEntry[] actual) {
    List<Integer> actualList = Lists.newArrayListWithCapacity(actual.length);

    for (QueueEntry anActual : actual) {
      actualList.add(Bytes.toInt(anActual.getData()));
    }

    Assert.assertEquals(expected, actualList);
  }

  private static final Function<QueueEntry, Integer> QueueEntryToInteger =
    new Function<QueueEntry, Integer>() {
      @Override
      public Integer apply(QueueEntry input) {
        return Bytes.toInt(input.getData());
      }
    };

  @Test
  public void testDropInflightEntries() throws Exception {
    TTQueue queue = createQueue();

    // enqueue 20 entries
    QueueEntry[] entries = new QueueEntry[20];
    for (int i = 1; i <= entries.length; i++) {
      entries[i - 1] = new QueueEntry(Bytes.toBytes(i));
    }
    Transaction t = oracle.startTransaction(true);
    queue.enqueue(entries, t);
    oracle.commitTransaction(t);

    List<Integer> expectedDequeues = Lists.newArrayList(1, 2, 3, 4, 5, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
    List<Integer> actualDequeues = Lists.newArrayList();

    // Dequeue 5 entries in batch, sync mode
    QueueConsumer consumer = new StatefulQueueConsumer(0, 0, 1, new QueueConfig(FIFO, true, 5, true));
    queue.configure(consumer, oracle.getReadPointer());
    t = oracle.startTransaction(true);
    DequeueResult result = queue.dequeue(consumer, t.getReadPointer());
    queue.ack(result.getEntryPointers(), consumer, t);
    oracle.commitTransaction(t);
    Iterables.addAll(actualDequeues, Iterables.transform(Arrays.asList(result.getEntries()), QueueEntryToInteger));

    // Dequeue 5 more
    t = oracle.startTransaction(true);
    queue.dequeue(consumer, t.getReadPointer());
    // This time drop the entries
    queue.dropInflightState(consumer, t.getReadPointer());
    oracle.abortTransaction(t);
    oracle.removeTransaction(t);

    // Dequeue the remaining
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    queue.ack(result.getEntryPointers(), consumer, t);
    oracle.commitTransaction(t);
    Iterables.addAll(actualDequeues, Iterables.transform(Arrays.asList(result.getEntries()), QueueEntryToInteger));

    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer, t.getReadPointer());
    queue.ack(result.getEntryPointers(), consumer, t);
    oracle.commitTransaction(t);
    Iterables.addAll(actualDequeues, Iterables.transform(Arrays.asList(result.getEntries()), QueueEntryToInteger));

    Assert.assertEquals(expectedDequeues, actualDequeues);
  }

  @Test
  public void testConfigureGroupsFifo() throws Exception {

    testConfigureGroups(FIFO);

    testConfigureGroups(HASH);

    testConfigureGroups(ROUND_ROBIN);
  }

  private void testConfigureGroups(PartitionerType partitionerType) throws Exception {
    TTQueue queue = createQueue();

    // enqueue 15 entries
    QueueEntry[] entries = new QueueEntry[15];
    for (int i = 1; i <= entries.length; i++) {
      entries[i - 1] = new QueueEntry(ImmutableMap.of(hashKey, i), Bytes.toBytes(i));
    }
    Transaction t = oracle.startTransaction(true);
    queue.enqueue(entries, t);
    oracle.commitTransaction(t);

    // Dequeue entries in batch, sync mode using 2 consumers of different groups
    List<Long> delGroups = queue.configureGroups(Lists.newArrayList(0L, 1L));
    Assert.assertTrue(delGroups.isEmpty()); // There should be no existing groups to delete

    QueueConsumer consumer1 = new StatefulQueueConsumer(0, 0L, 1, "", hashKey,
                                                        new QueueConfig(partitionerType, true, 3, true));
    queue.configure(consumer1, oracle.getReadPointer());
    QueueConsumer consumer2 = new StatefulQueueConsumer(0, 1L, 1, "", hashKey,
                                                        new QueueConfig(partitionerType, true, 3, true));
    queue.configure(consumer2, oracle.getReadPointer());

    List<Integer> consumer1Dequeues = Lists.newArrayList();
    List<Integer> consumer2Dequeues = Lists.newArrayList();

    // Dequeue 3 using both consumers
    t = oracle.startTransaction(true);
    DequeueResult result = queue.dequeue(consumer1, t.getReadPointer());
    queue.ack(result.getEntryPointers(), consumer1, t);
    oracle.commitTransaction(t);
    Iterables.addAll(consumer1Dequeues, Iterables.transform(Arrays.asList(result.getEntries()), QueueEntryToInteger));
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer2, t.getReadPointer());
    queue.ack(result.getEntryPointers(), consumer2, t);
    oracle.commitTransaction(t);
    queue.finalize(result.getEntryPointers(), consumer1, 1, t);
    Iterables.addAll(consumer2Dequeues, Iterables.transform(Arrays.asList(result.getEntries()), QueueEntryToInteger));

    // Now delete consumer2 group
    delGroups = queue.configureGroups(Collections.singletonList(consumer1.getGroupId()));
    Assert.assertEquals(Collections.singletonList(consumer2.getGroupId()), delGroups);

    // Configure consumer1 again
    consumer1 = new StatefulQueueConsumer(0, 0L, 1, "", hashKey, new QueueConfig(partitionerType, true, 3, true));
    queue.configure(consumer1, oracle.getReadPointer());

    // Dequeue 3 more with consuemr1 only
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer1, t.getReadPointer());
    queue.ack(result.getEntryPointers(), consumer1, t);
    oracle.commitTransaction(t);
    queue.finalize(result.getEntryPointers(), consumer1, 1, t);
    Iterables.addAll(consumer1Dequeues, Iterables.transform(Arrays.asList(result.getEntries()), QueueEntryToInteger));

    // Dequeuing with consumer2 should now throw error
    try {
      consumer2 = new StatefulQueueConsumer(0, 1L, 1, new QueueConfig(partitionerType, true, 3, true));
      t = oracle.startTransaction(true);
      result = queue.dequeue(consumer2, t.getReadPointer());
      fail("Dequeue should throw error");
    } catch (OperationException e) {
      oracle.abortTransaction(t);
      oracle.removeTransaction(t);
      if (e.getStatus() != StatusCode.NOT_CONFIGURED) {
        throw e;
      }
    }

    // Run finalize
    t = oracle.startTransaction(true);
    // Note: only 4 will get finalized - max of min committed entries of a txn
    queue.finalize(result.getEntryPointers(), consumer1, 1, t);
    oracle.commitTransaction(t);

    // Now create group1 (consumer2) again
    delGroups = queue.configureGroups(Lists.newArrayList(0L, 1L));
    Assert.assertTrue(delGroups.isEmpty()); // No groups to delete
    consumer1 = new StatefulQueueConsumer(0, 0L, 1, "", hashKey, new QueueConfig(partitionerType, true, 3, true));
    queue.configure(consumer1, oracle.getReadPointer());
    consumer2 = new StatefulQueueConsumer(0, 1L, 1, "", hashKey, new QueueConfig(partitionerType, true, 3, true));
    queue.configure(consumer2, oracle.getReadPointer());

    // Dequeue 3 using both consumers
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer1, t.getReadPointer());
    queue.ack(result.getEntryPointers(), consumer1, t);
    oracle.commitTransaction(t);
    Iterables.addAll(consumer1Dequeues, Iterables.transform(Arrays.asList(result.getEntries()), QueueEntryToInteger));
    t = oracle.startTransaction(true);
    result = queue.dequeue(consumer2, t.getReadPointer());
    queue.ack(result.getEntryPointers(), consumer2, t);
    oracle.commitTransaction(t);
    Iterables.addAll(consumer2Dequeues, Iterables.transform(Arrays.asList(result.getEntries()), QueueEntryToInteger));

    // Consumer 1 should have all 15 entries
    Assert.assertEquals(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9), consumer1Dequeues);
    // Consumer 2 should not have 4 since it would have gotten finalized
    Assert.assertEquals(Lists.newArrayList(1, 2, 3, 5, 6, 7), consumer2Dequeues);
  }

}
