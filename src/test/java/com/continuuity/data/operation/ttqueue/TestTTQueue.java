package com.continuuity.data.operation.ttqueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.continuuity.data.engine.memory.MemoryOVCTable;
import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicTimeOracle;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.table.ReadPointer;

public class TestTTQueue {

  private static final long MAX_TIMEOUT_MS = 10000;
  private static final long DEQUEUE_BLOCK_TIMEOUT_MS = 1;

  private static TimestampOracle timeOracle =
      new MemoryStrictlyMonotonicTimeOracle();

  // TODO: insert fancy ioc hotness here

  private static TTQueue createQueue() {
    return new TTQueueOnVCTable(
        new MemoryOVCTable(
            Bytes.toBytes("TestTTQueueTable")),
            Bytes.toBytes("TestTTQueue"),
            timeOracle, new Configuration());
  }

  @Test
  public void testSingleConsumerSimple() throws Exception {
    final boolean singleEntry = true;
    TTQueue queue = createQueue();
    long dirtyVersion = 1;
    ReadPointer dirtyReadPointer = new MemoryReadPointer(Long.MAX_VALUE);

    byte [] valueOne = Bytes.toBytes("value1");
    byte [] valueTwo = Bytes.toBytes("value2");

    // enqueue two entries
    assertTrue(queue.enqueue(valueOne, dirtyVersion).isSuccess());
    assertTrue(queue.enqueue(valueTwo, dirtyVersion).isSuccess());

    // dequeue it with the single consumer and random partitioner
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueuePartitioner partitioner = new QueuePartitioner.RandomPartitioner();
    QueueConfig config = new QueueConfig(partitioner, singleEntry);
    DequeueResult result = queue.dequeue(consumer, config, dirtyReadPointer);

    // verify we got something and it's the first value
    assertTrue(result.toString(), result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), valueOne));

    // dequeue again without acking, should still get first value
    result = queue.dequeue(consumer, config, dirtyReadPointer);
    assertTrue(result.isSuccess());
    assertTrue("Expected (" + Bytes.toString(valueOne) + ") Got (" +
        Bytes.toString(result.getValue()) + ")",
        Bytes.equals(result.getValue(), valueOne));

    // ack
    assertTrue("Should be able to ack but it failed for " +
        result.getEntryPointer(),
        queue.ack(result.getEntryPointer(), consumer));

    // dequeue, should get second value
    result = queue.dequeue(consumer, config, dirtyReadPointer);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), valueTwo));

    // ack
    assertTrue(queue.ack(result.getEntryPointer(), consumer));

    // verify queue is empty
    result = queue.dequeue(consumer, config, dirtyReadPointer);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testSingleConsumerMulti() throws Exception {
    final boolean singleEntry = false;
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = new MemoryReadPointer(version);

    // enqueue ten entries
    int n=10;
    for (int i=0;i<n;i++) {
      assertTrue(queue.enqueue(Bytes.toBytes(i+1), version).isSuccess());
    }

    // dequeue it with the single consumer and random partitioner
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueuePartitioner partitioner = new QueuePartitioner.RandomPartitioner();
    QueueConfig config = new QueueConfig(partitioner, singleEntry);

    // verify it's the first value
    DequeueResult resultOne = queue.dequeue(consumer, config, readPointer);
    assertTrue(Bytes.equals(resultOne.getValue(), Bytes.toBytes(1)));

    // dequeue again without acking, async mode should get the second value
    DequeueResult resultTwo = queue.dequeue(consumer, config, readPointer);
    assertTrue(Bytes.equals(resultTwo.getValue(), Bytes.toBytes(2)));

    // ack result two
    assertTrue(queue.ack(resultTwo.getEntryPointer(), consumer));

    // dequeue, should get third value
    DequeueResult resultThree = queue.dequeue(consumer, config, readPointer);
    assertTrue(Bytes.equals(resultThree.getValue(), Bytes.toBytes(3)));

    // ack
    assertTrue(queue.ack(resultThree.getEntryPointer(), consumer));

    // dequeue fourth
    DequeueResult resultFour = queue.dequeue(consumer, config, readPointer);
    assertTrue(Bytes.equals(resultFour.getValue(), Bytes.toBytes(4)));

    // dequeue five through ten, and ack them
    for (int i=5; i<11; i++) {
      DequeueResult result = queue.dequeue(consumer, config, readPointer);
      assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(i)));
      assertTrue(queue.ack(result.getEntryPointer(), consumer));
    }

    // verify queue is empty (first and fourth not ackd but still dequeued)
    assertTrue(queue.dequeue(consumer, config, readPointer).isEmpty());

    // second and third ackd, another ack should fail
    assertFalse(queue.ack(resultTwo.getEntryPointer(), consumer));
    assertFalse(queue.ack(resultThree.getEntryPointer(), consumer));
    // first and fourth are not acked, ack should pass
    assertTrue(queue.ack(resultOne.getEntryPointer(), consumer));
    assertTrue(queue.ack(resultFour.getEntryPointer(), consumer));

    // queue still empty
    assertTrue(queue.dequeue(consumer, config, readPointer).isEmpty());
  }

  @Test
  public void testMultipleConsumerMultiTimeouts() throws Exception {
    final boolean singleEntry = false;
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = new MemoryReadPointer(version);

    // enqueue ten entries
    int n=10;
    for (int i=0;i<n;i++) {
      assertTrue(queue.enqueue(Bytes.toBytes(i+1), version).isSuccess());
    }

    // dequeue it with the single consumer and random partitioner
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueuePartitioner partitioner = new QueuePartitioner.RandomPartitioner();
    QueueConfig config = new QueueConfig(partitioner, singleEntry);

    // verify it's the first value
    DequeueResult resultOne = queue.dequeue(consumer, config, readPointer);
    assertTrue(Bytes.equals(resultOne.getValue(), Bytes.toBytes(1)));

    // dequeue again without acking, async mode should get the second value
    DequeueResult resultTwo = queue.dequeue(consumer, config, readPointer);
    assertTrue(Bytes.equals(resultTwo.getValue(), Bytes.toBytes(2)));

    // ack result two
    assertTrue(queue.ack(resultTwo.getEntryPointer(), consumer));

    // dequeue, should get third value
    DequeueResult resultThree = queue.dequeue(consumer, config, readPointer);
    assertTrue(Bytes.equals(resultThree.getValue(), Bytes.toBytes(3)));

    // ack
    assertTrue(queue.ack(resultThree.getEntryPointer(), consumer));

    // dequeue fourth
    DequeueResult resultFour = queue.dequeue(consumer, config, readPointer);
    assertTrue(Bytes.equals(resultFour.getValue(), Bytes.toBytes(4)));

    // dequeue five through ten, and ack them
    for (int i=5; i<11; i++) {
      DequeueResult result = queue.dequeue(consumer, config, readPointer);
      assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(i)));
      assertTrue(queue.ack(result.getEntryPointer(), consumer));
    }

    // verify queue is empty (first and fourth not ackd but still dequeued)
    assertTrue(queue.dequeue(consumer, config, readPointer).isEmpty());

    // second and third ackd, another ack should fail
    assertFalse(queue.ack(resultTwo.getEntryPointer(), consumer));
    assertFalse(queue.ack(resultThree.getEntryPointer(), consumer));

    // queue still empty
    assertTrue(queue.dequeue(consumer, config, readPointer).isEmpty());

    // now set the timeout and sleep for timeout + 1
    long oldTimeout = ((TTQueueOnVCTable)queue).maxAgeBeforeExpirationInMillis;
    ((TTQueueOnVCTable)queue).maxAgeBeforeExpirationInMillis = 50;
    Thread.sleep(51);

    // two dequeues in a row should give values one and four
    DequeueResult resultOneB = queue.dequeue(consumer, config, readPointer);
    assertNotNull(resultOneB);
    assertTrue(resultOneB.isSuccess());
    assertTrue(Bytes.equals(resultOneB.getValue(), Bytes.toBytes(1)));
    DequeueResult resultFourB = queue.dequeue(consumer, config, readPointer);
    assertNotNull(resultFourB);
    assertTrue(resultFourB.isSuccess());
    assertTrue(Bytes.equals(resultFourB.getValue(), Bytes.toBytes(4)));

    // and then queue should be empty again
    assertTrue(queue.dequeue(consumer, config, readPointer).isEmpty());

    // first and fourth are not acked, ack should pass using either result
    assertTrue(queue.ack(resultOne.getEntryPointer(), consumer));
    assertTrue(queue.ack(resultFourB.getEntryPointer(), consumer));

    // using other result version should fail second time
    assertFalse(queue.ack(resultOneB.getEntryPointer(), consumer));
    assertFalse(queue.ack(resultFour.getEntryPointer(), consumer));

    // restore timeout
    ((TTQueueOnVCTable)queue).maxAgeBeforeExpirationInMillis = oldTimeout;
  }

  @Test
  public void testSingleConsumerMultiEntry_Empty_ChangeToSingleConsumerSingleEntry()
      throws Exception {
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = new MemoryReadPointer(version);

    // enqueue 3 entries
    int n=3;
    for (int i=0;i<n;i++) {
      assertTrue(queue.enqueue(Bytes.toBytes(i+1), version).isSuccess());
    }

    // dequeue it with the single consumer and random partitioner
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueuePartitioner partitioner = new QueuePartitioner.RandomPartitioner();
    QueueConfig multiConfig = new QueueConfig(partitioner, false);

    // verify it's the first value
    DequeueResult resultOne = queue.dequeue(consumer, multiConfig, readPointer);
    assertTrue(Bytes.equals(resultOne.getValue(), Bytes.toBytes(1)));

    // dequeue again without acking, multi mode should get the second value
    DequeueResult resultTwo = queue.dequeue(consumer, multiConfig, readPointer);
    assertTrue(Bytes.equals(resultTwo.getValue(), Bytes.toBytes(2)));

    // ack result two
    assertTrue(queue.ack(resultTwo.getEntryPointer(), consumer));

    // dequeue again, multi mode should get the third value
    DequeueResult resultThree = queue.dequeue(consumer, multiConfig, readPointer);
    assertTrue(Bytes.equals(resultThree.getValue(), Bytes.toBytes(3)));

    // ack result three
    assertTrue(queue.ack(resultThree.getEntryPointer(), consumer));

    // one is still not acked, queue is not empty

    // attempt to change to single entry mode should fail
    QueueConfig singleConfig = new QueueConfig(partitioner, true);
    assertTrue(queue.dequeue(consumer, singleConfig, readPointer).isFailure());

    // ack entry one
    assertTrue(queue.ack(resultOne.getEntryPointer(), consumer));

    // though we are empty, the requirement is that you have to actually
    // dequeue until you verify as empty, so trying a change now will fail
    assertTrue(queue.dequeue(consumer, singleConfig, readPointer).isFailure());

    // doing a dequeue with the old mode first will return empty
    assertTrue(queue.dequeue(consumer, multiConfig, readPointer).isEmpty());

    // now we are empty, try to change modes now, should pass and be empty
    DequeueResult result = queue.dequeue(consumer, singleConfig, readPointer);
    assertTrue(result.toString(), result.isEmpty());

  }

  @Test
  public void testSingleConsumerSingleEntryWithInvalid_Empty_ChangeSizeAndToMulti()
      throws Exception {
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = new MemoryReadPointer(version);

    // enqueue four entries
    int n=4;
    EnqueueResult [] results = new EnqueueResult[n];
    for (int i=0;i<n;i++) {
      results[i] = queue.enqueue(Bytes.toBytes(i+1), version);
      assertTrue(results[i].isSuccess());
    }

    // invalidate number 3
    queue.invalidate(results[2].getEntryPointer(), version);

    // dequeue with a single consumer and random partitioner
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueuePartitioner partitioner = new QueuePartitioner.RandomPartitioner();
    QueueConfig multiConfig = new QueueConfig(partitioner, false);
    QueueConfig singleConfig = new QueueConfig(partitioner, true);

    // use single entry first

    // verify it's the first value
    DequeueResult resultOne = queue.dequeue(consumer, singleConfig, readPointer);
    assertTrue(Bytes.equals(resultOne.getValue(), Bytes.toBytes(1)));

    // dequeue again without acking, singleEntry mode should get the first value again
    DequeueResult resultOneB = queue.dequeue(consumer, singleConfig, readPointer);
    assertTrue(Bytes.equals(resultOneB.getValue(), Bytes.toBytes(1)));

    // ack result one
    assertTrue(queue.ack(resultOne.getEntryPointer(), consumer));

    // dequeue again without acking, get second value
    DequeueResult resultTwo = queue.dequeue(consumer, singleConfig, readPointer);
    assertTrue(Bytes.equals(resultTwo.getValue(), Bytes.toBytes(2)));

    // dequeue should give us back the un-ack'd stuff still
    DequeueResult resultTwoB = queue.dequeue(consumer, singleConfig, readPointer);
    assertNotNull(resultTwoB);
    assertTrue("expected 2, actual " + Bytes.toInt(resultTwoB.getValue()),
        Bytes.equals(resultTwoB.getValue(), Bytes.toBytes(2)));

    // same thing again
    DequeueResult resultTwoC = queue.dequeue(consumer, singleConfig, readPointer);
    assertNotNull(resultTwoC);
    assertTrue(Bytes.equals(resultTwoC.getValue(), Bytes.toBytes(2)));

    // ack
    assertTrue(queue.ack(resultTwoB.getEntryPointer(), consumer));

    // dequeue again, should get four not three as it was invalidated
    DequeueResult resultFourA = queue.dequeue(consumer, singleConfig, readPointer);
    assertNotNull(resultFourA);
    assertTrue(Bytes.equals(resultFourA.getValue(), Bytes.toBytes(4)));

    // trying to change to multi now should fail
    assertTrue(queue.dequeue(consumer, multiConfig, readPointer).isFailure());

    // trying to change group size should also fail
    assertTrue(queue.dequeue(
        new QueueConsumer(0, 0, 2), singleConfig, readPointer).isFailure());

    // ack
    assertTrue(queue.ack(resultFourA.getEntryPointer(), consumer));

    // empty
    assertTrue(queue.dequeue(consumer, singleConfig, readPointer).isEmpty());

    // change modes to multi config and two consumer instances
    QueueConsumer [] consumers = new QueueConsumer[] {
        new QueueConsumer(0, 0, 2), new QueueConsumer(1, 0, 2)
    };

    // should be empty with new config, not failure
    assertTrue(queue.dequeue(consumers[0], multiConfig, readPointer).isEmpty());

    // enqueue three entries
    n=3;
    results = new EnqueueResult[n];
    for (int i=0;i<n;i++) {
      results[i] = queue.enqueue(Bytes.toBytes(i+1), version);
      assertTrue(results[i].isSuccess());
    }

    // dequeue two with consumer 0, should get 1 and 2
    DequeueResult resultM0One = queue.dequeue(consumers[0], multiConfig,
        readPointer);
    assertTrue(resultM0One.isSuccess());
    assertTrue(Bytes.equals(resultM0One.getValue(), Bytes.toBytes(1)));
    DequeueResult resultM0Two = queue.dequeue(consumers[0], multiConfig,
        readPointer);
    assertTrue(resultM0Two.isSuccess());
    assertTrue(Bytes.equals(resultM0Two.getValue(), Bytes.toBytes(2)));

    // dequeue one with consumer 1, should get 3
    DequeueResult resultM1Three = queue.dequeue(consumers[1], multiConfig,
        readPointer);
    assertTrue(resultM1Three.isSuccess());
    assertTrue(Bytes.equals(resultM1Three.getValue(), Bytes.toBytes(3)));

    // consumer 0 and consumer 1 should see empty now
    assertTrue(queue.dequeue(consumers[0], multiConfig, readPointer).isEmpty());
    assertTrue(queue.dequeue(consumers[1], multiConfig, readPointer).isEmpty());

    // verify consumer 1 can't ack one of consumer 0s entries and vice versa
    assertFalse(queue.ack(resultM0One.getEntryPointer(), consumers[1]));
    assertFalse(queue.ack(resultM1Three.getEntryPointer(), consumers[0]));

    // ack everything correctly
    assertTrue(queue.ack(resultM0One.getEntryPointer(), consumers[0]));
    assertTrue(queue.ack(resultM0Two.getEntryPointer(), consumers[0]));
    assertTrue(queue.ack(resultM1Three.getEntryPointer(), consumers[1]));

    // both still see empty
    assertTrue(queue.dequeue(consumers[0], multiConfig, readPointer).isEmpty());
    assertTrue(queue.dequeue(consumers[1], multiConfig, readPointer).isEmpty());
  }


  @Test
  public void testSingleConsumerThreaded() throws Exception {
    final boolean singleEntry = true;
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = new MemoryReadPointer(version);

    byte [] valueOne = Bytes.toBytes("value1");
    byte [] valueTwo = Bytes.toBytes("value2");


    // dequeue it with the single consumer and random partitioner
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueuePartitioner partitioner = new QueuePartitioner.RandomPartitioner();
    QueueConfig config = new QueueConfig(partitioner, singleEntry);

    // spawn a thread to dequeue
    QueueDequeuer dequeuer = new QueueDequeuer(queue, consumer, partitioner,
        singleEntry, readPointer);
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
    assertTrue(queue.enqueue(valueOne, version).isSuccess());
    waitForAndAssertCount(1, dequeuer.dequeues);
    waitForAndAssertCount(2, dequeuer.dequeueRunLoop);

    // dequeuer will dequeue, but we should still be able to dequeue!
    result = queue.dequeue(consumer, config, readPointer);
    assertNotNull(result);
    assertTrue(Bytes.equals(result.getValue(), valueOne));

    // dequeuer should also have this loaded
    result = dequeuer.blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
    assertNotNull(result);
    assertTrue(Bytes.equals(result.getValue(), valueOne));

    // ack it!
    queue.ack(result.getEntryPointer(), consumer);

    // trigger another dequeue
    assertTrue(dequeuer.triggerdequeue());
    waitForAndAssertCount(1, dequeuer.dequeues);
    // dequeuer had dequeueped and goes into its next loop
    waitForAndAssertCount(2, dequeuer.dequeueRunLoop);

    // nothing in queue so dequeuer should be empty
    result = dequeuer.blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
    assertNull(result);

    // enqueue
    assertTrue(queue.enqueue(valueTwo, version).isSuccess());
    waitForAndAssertCount(2, dequeuer.dequeues);
    // dequeuer had dequeueped and goes into its next loop
    waitForAndAssertCount(3, dequeuer.dequeueRunLoop);

    // dequeuer should have value2
    result = dequeuer.blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
    assertNotNull(result);
    assertTrue(Bytes.equals(result.getValue(), valueTwo));

    // trigger dequeuer again, should get the same one back
    assertTrue(dequeuer.triggerdequeue());
    waitForAndAssertCount(3, dequeuer.dequeues);
    // dequeuer had dequeueped and goes into its next loop
    waitForAndAssertCount(4, dequeuer.dequeueRunLoop);
    result = dequeuer.blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
    assertNotNull(result);
    assertTrue(Bytes.equals(result.getValue(), valueTwo));

    // ack it!
    queue.ack(result.getEntryPointer(), consumer);

    // verify queue is empty
    assertTrue(queue.dequeue(consumer, config, readPointer).isEmpty());

    // shut down
    dequeuer.shutdown();
  }
  
  // TODO: Fix and enable
  @Test
  public void testMultiConsumerMultiGroup() throws Exception {
    final boolean singleEntry = true;
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = new MemoryReadPointer(version);
    AtomicLong dequeueReturns = ((TTQueueOnVCTable)queue).dequeueReturns;

    // Create 4 consumer groups with 4 consumers each
    int n = 4;
    QueueConsumer [][] consumers = new QueueConsumer[n][];
    for (int i=0; i<n; i++) {
      consumers[i] = new QueueConsumer[n];
      for (int j=0; j<n; j++) {
        consumers[i][j] = new QueueConsumer(j, i, n);
      }
    }

    // values are longs
    byte [][] values = new byte[n*n][];
    for (int i=0; i<n*n; i++) {
      values[i] = Bytes.toBytes((long)i);
    }

    // Make a partitioner that just converts value to long and modulos it
    QueuePartitioner partitioner = new QueuePartitioner() {
      @Override
      public boolean shouldEmit(QueueConsumer consumer, long entryId,
          byte [] value) {
        long val = Bytes.toLong(value);
        return (val % consumer.getGroupSize()) == consumer.getInstanceId();
      }
    };
    QueueConfig config = new QueueConfig(partitioner, singleEntry);

    // Start a dequeuer for every consumer!
    QueueDequeuer [][] dequeuers = new QueueDequeuer[n][];
    for (int i=0; i<n; i++) {
      dequeuers[i] = new QueueDequeuer[n];
      for (int j=0; j<n; j++) {
        dequeuers[i][j] = new QueueDequeuer(queue, consumers[i][j], partitioner,
            singleEntry, readPointer);
        dequeuers[i][j].start();
        assertTrue(dequeuers[i][j].triggerdequeue());
      }
    }

    // No queue dequeue returns yet
    long expectedQueuedequeues = 0;
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // verify everyone is empty
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        assertNull(dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS));
      }
    }

    // no dequeues yet
    long numdequeues = 0L;
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        assertEquals(numdequeues, dequeuers[i][j].dequeues.get());
      }
    }

    // enqueue the first four values
    for (int i=0; i<n; i++) {
      assertTrue(queue.enqueue(values[i], version).isSuccess());
    }

    // wait for 16 more queuedequeue() returns
    expectedQueuedequeues += 16;
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // every dequeuer/consumer should have one result
    numdequeues++;
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        waitForAndAssertCount(numdequeues, dequeuers[i][j].dequeues);
        DequeueResult result = dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
        assertNotNull(result);
        assertEquals(j, Bytes.toLong(result.getValue()));
      }
    }

    // trigger dequeues again, should get the same entries
    numdequeues++;
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        assertTrue(dequeuers[i][j].triggerdequeue());
        waitForAndAssertCount(numdequeues, dequeuers[i][j].dequeues);
      }
    }

    // wait for 16 more queuedequeue() returns
    expectedQueuedequeues += 16;
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // every dequeuer/consumer should have one result
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        DequeueResult result = dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
        assertNotNull(result);
        assertEquals(j, Bytes.toLong(result.getValue()));
      }
    }

    // directly dequeueping should also yield the same result
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        DequeueResult result = queue.dequeue(consumers[i][j], config,
            readPointer);
        assertNotNull(result);
        assertTrue(result.isSuccess());
        assertEquals(j, Bytes.toLong(result.getValue()));
      }
    }

    // wait for 16 more queuedequeue() returns
    expectedQueuedequeues += 16;
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // ack it for groups(0,1) consumers(2,3)
    for (int i=0; i<n/2; i++) {
      for (int j=n/2; j<n; j++) {
        DequeueResult result = queue.dequeue(consumers[i][j], config,
            readPointer);
        assertNotNull(result);
        assertEquals(j, Bytes.toLong(result.getValue()));
        assertTrue(queue.ack(result.getEntryPointer(), consumers[i][j]));
        System.out.println("ACK: i=" + i + ", j=" + j);
      }
    }

    // wait for 4 more queuedequeue() returns
    expectedQueuedequeues += 4;
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // trigger dequeuers
    numdequeues++;
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        waitForAndAssertCount(numdequeues, dequeuers[i][j].dequeueRunLoop);
        assertTrue(dequeuers[i][j].triggerdequeue());
        waitForAndAssertCount(numdequeues, dequeuers[i][j].triggers);
      }
    }

    // wait for 12 more queuedequeue() returns
    expectedQueuedequeues += 12;
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // expect null for groups(0,1) consumers(2,3), same value for others
    // ack everyone not ackd
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        if ((i == 0 || i == 1) && (j == 2 || j == 3)) {
          assertNull(dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS));
        } else {
          System.out.println("dequeue: i=" + i + ", j=" + j);
          DequeueResult result = queue.dequeue(consumers[i][j], config,
              readPointer);
          assertNotNull(result);
          assertEquals(j, Bytes.toLong(result.getValue()));
          assertTrue(queue.ack(result.getEntryPointer(), consumers[i][j]));
          // buffer of dequeuer should still have that result
          waitForAndAssertCount(numdequeues, dequeuers[i][j].dequeues);
          result = dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
          assertNotNull(result);
          assertEquals(j, Bytes.toLong(result.getValue()));
        }
      }
    }

    // wait for 12 more queuedequeue() returns
    expectedQueuedequeues += 12;
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // trigger dequeues again, will get false for groups(0,1) consumers(2,3)
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        boolean dequeueStatus = dequeuers[i][j].triggerdequeue();
        if ((i == 0 || i == 1) && (j == 2 || j == 3)) {
          assertFalse(dequeueStatus);
        } else {
          if (!dequeueStatus) System.out.println("Failed Trigger dequeue {i=" +
              i + ", j=" + j + "}");
          assertTrue("Expected to be able to trigger a dequeue but was not {i=" +
              i + ", j=" + j +"}", dequeueStatus);
        }
      }
    }
    // re-align counters so we can keep testing sanely
    // for groups(0,1) consumers(2,3)
    for (int i=0; i<n/2; i++) {
      for (int j=n/2; j<n; j++) {
        dequeuers[i][j].dequeues.incrementAndGet();
      }
    }

    // everyone should be empty
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        assertNull(dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS));
      }
    }

    // enqueue everything!
    for (int i=0; i<n*n; i++) {
      assertTrue(queue.enqueue(values[i], version).isSuccess());
    }

    // wait for 16 more queuedequeue() wake-ups
    expectedQueuedequeues += 16;
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);
    numdequeues++;

    // dequeue and ack everything.  each consumer should have 4 things!
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        long localdequeues = numdequeues;
        for (int k=0; k<n; k++) {
          waitForAndAssertCount(localdequeues, dequeuers[i][j].dequeues);
          DequeueResult result = dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
          assertNotNull(result);
          assertEquals((long)(k*n)+j, Bytes.toLong(result.getValue()));
          assertTrue("i=" + i + ",j=" + j + ",k=" + k,
              queue.ack(result.getEntryPointer(), consumers[i][j]));
          dequeuers[i][j].triggerdequeue();
          localdequeues++;
        }
      }
    }

    // everyone should be empty
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        assertNull(dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS));
      }
    }

    // everyone should be empty
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        dequeuers[i][j].shutdown();
      }
    }
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
    private final QueueConfig config;
    private final ReadPointer readPointer;

    private final AtomicBoolean dequeueTrigger = new AtomicBoolean(false);

    AtomicLong triggers = new AtomicLong(0);
    AtomicLong dequeues = new AtomicLong(0);
    AtomicLong dequeueRunLoop = new AtomicLong(0);

    private DequeueResult result;

    private boolean keepgoing = true;

    /**
     * @param queue
     * @param consumer
     * @param partitioner
     */
    QueueDequeuer(TTQueue queue, QueueConsumer consumer,
        QueuePartitioner partitioner, boolean singleEntry,
        ReadPointer readPointer) {
      this.queue = queue;
      this.consumer = consumer;
      this.config = new QueueConfig(partitioner, singleEntry);
      this.readPointer = readPointer;
      this.result = null;
    }

    DequeueResult nonBlockDequeue() {
      if (this.result == null) return null;
      DequeueResult ret = this.result;
      this.result = null;
      return ret;
    }

    DequeueResult blockdequeue(long timeout) {
      DequeueResult result = nonBlockDequeue();
      if (result != null) return result;
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
              if (this.keepgoing) e.printStackTrace();
            }
          }
          this.triggers.incrementAndGet();
        }
        DequeueResult result = null;
        while ((result == null || result.isEmpty()) && this.keepgoing) {
          result = this.queue.dequeue(this.consumer, this.config,
              this.readPointer);
          if (result.isFailure()) {
            throw new RuntimeException("Don't expect failures in Popper!");
          }
        }
        this.dequeueTrigger.set(false);
        this.result = result;
        this.dequeues.incrementAndGet();
      }
    }
  }
}
