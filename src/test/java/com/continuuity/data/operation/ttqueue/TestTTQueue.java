package com.continuuity.data.operation.ttqueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicTimeOracle;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.operation.ttqueue.QueueAdmin.QueueMeta;
import com.continuuity.data.table.ReadPointer;

public abstract class TestTTQueue {

  private static final long MAX_TIMEOUT_MS = 10000;
  private static final long DEQUEUE_BLOCK_TIMEOUT_MS = 1;

  protected static TimestampOracle timeOracle =
      new MemoryStrictlyMonotonicTimeOracle();

  private TTQueue createQueue() {
    return createQueue(new CConfiguration());
  }

  protected abstract TTQueue createQueue(CConfiguration conf);

  protected abstract int getNumIterations();

  @Test
  public void testLotsOfAsyncDequeueing() throws Exception {
    TTQueue queue = createQueue();
    long dirtyVersion = 1;

    long startTime = System.currentTimeMillis();

    int numEntries = getNumIterations();

    for (int i=1; i<numEntries+1; i++) {
      queue.enqueue(Bytes.toBytes(i), dirtyVersion);
    }
    System.out.println("Done enqueueing");

    long enqueueStop = System.currentTimeMillis();

    System.out.println("Finished enqueue of " + numEntries + " entries in " +
        (enqueueStop-startTime) + " ms (" +
        (enqueueStop-startTime)/((float)numEntries) + " ms/entry)");

    QueueConsumer consumerSync = new QueueConsumer(0, 0, 1);
    QueueConfig configSync = new QueueConfig(
        new QueuePartitioner.RandomPartitioner(), true);
    for (int i=1; i<numEntries+1; i++) {
      DequeueResult result =
          queue.dequeue(consumerSync, configSync,
              new MemoryReadPointer(timeOracle.getTimestamp()));
      assertTrue(result.isSuccess());
      assertTrue(Bytes.equals(Bytes.toBytes(i), result.getValue()));
      assertTrue(queue.ack(result.getEntryPointer(), consumerSync));
      assertTrue(queue.finalize(result.getEntryPointer(), consumerSync, -1));
      if (i % 100 == 0) System.out.print(".");
      if (i % 1000 == 0) System.out.println(" " + i);
    }

    long dequeueSyncStop = System.currentTimeMillis();

    System.out.println("Finished sync dequeue of " + numEntries + " entries in " +
        (dequeueSyncStop-enqueueStop) + " ms (" +
        (dequeueSyncStop-enqueueStop)/((float)numEntries) + " ms/entry)");

    // Async

    QueueConsumer consumerAsync = new QueueConsumer(0, 2, 1);
    QueueConfig configAsync = new QueueConfig(
        new QueuePartitioner.RandomPartitioner(), false);
    for (int i=1; i<numEntries+1; i++) {
      DequeueResult result =
          queue.dequeue(consumerAsync, configAsync,
              new MemoryReadPointer(timeOracle.getTimestamp()));
      assertTrue(result.isSuccess());
      assertTrue("Expected " + i + ", Actual " + Bytes.toInt(result.getValue()),
          Bytes.equals(Bytes.toBytes(i), result.getValue()));
      if (i % 100 == 0) System.out.print(".");
      if (i % 1000 == 0) System.out.println(" " + i);
    }

    long dequeueAsyncStop = System.currentTimeMillis();

    System.out.println("Finished async dequeue of " + numEntries + " entries in " +
        (dequeueAsyncStop-dequeueSyncStop) + " ms (" +
        (dequeueAsyncStop-dequeueSyncStop)/((float)numEntries) + " ms/entry)");

    // Both queues should be empty for each consumer
    assertTrue(queue.dequeue(consumerSync, configSync,
        new MemoryReadPointer(timeOracle.getTimestamp())).isEmpty());
    assertTrue(queue.dequeue(consumerAsync, configAsync,
        new MemoryReadPointer(timeOracle.getTimestamp())).isEmpty());
  }

  @Test
  public void testGroupIdGen() throws Exception {
    TTQueue queue = createQueue();
    int n = 1024;
    Set<Long> groupids = new HashSet<Long>(n);
    for (int i=0; i<n; i++) {
      long groupid = queue.getGroupID();
      assertFalse(groupids.contains(groupid));
      groupids.add(groupid);
    }
  }

  @Test 
  public void testEvictOnAck_OneGroup() throws Exception {
    final boolean singleEntry = true;
    long dirtyVersion = 1;
    ReadPointer dirtyReadPointer = new MemoryReadPointer(Long.MAX_VALUE);

    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueueConsumer consumer2 = new QueueConsumer(0, 1, 1);
    QueuePartitioner partitioner = new QueuePartitioner.RandomPartitioner();
    QueueConfig config = new QueueConfig(partitioner, singleEntry);
    
    // first try with evict-on-ack off
    TTQueue queueNormal = createQueue();
    int numGroups = -1;
    
    // enqueue 10 things
    for (int i=0; i<10; i++) {
      queueNormal.enqueue(Bytes.toBytes(i), dirtyVersion);
    }
    
    // dequeue/ack/finalize 10 things w/ numGroups=-1
    for (int i=0; i<10; i++) {
      DequeueResult result =
          queueNormal.dequeue(consumer, config, dirtyReadPointer);
      assertTrue(
          queueNormal.ack(result.getEntryPointer(), consumer));
      assertTrue(
          queueNormal.finalize(result.getEntryPointer(), consumer, numGroups));
    }
    
    // dequeue is empty
    assertTrue(
        queueNormal.dequeue(consumer, config, dirtyReadPointer).isEmpty());
    
    // dequeue with new consumer still has entries (expected)
    assertFalse(
        queueNormal.dequeue(consumer2, config, dirtyReadPointer).isEmpty());
    
    // now do it again with evict-on-ack turned on
    TTQueue queueEvict = createQueue();
    numGroups = 1;

    // enqueue 10 things
    for (int i=0; i<10; i++) {
      queueEvict.enqueue(Bytes.toBytes(i), dirtyVersion);
    }
    
    // dequeue/ack/finalize 10 things w/ numGroups=1
    for (int i=0; i<10; i++) {
      DequeueResult result =
          queueEvict.dequeue(consumer, config, dirtyReadPointer);
      assertTrue(
          queueEvict.ack(result.getEntryPointer(), consumer));
      assertTrue(
          queueEvict.finalize(result.getEntryPointer(), consumer, numGroups));
    }
    
    // dequeue is empty
    assertTrue(
        queueEvict.dequeue(consumer, config, dirtyReadPointer).isEmpty());
    
    // dequeue with new consumer IS NOW EMPTY!
    assertTrue(
        queueEvict.dequeue(consumer2, config, dirtyReadPointer).isEmpty());
    
    
  }
  
  @Test
  public void testEvictOnAck_ThreeGroups() throws Exception {
    TTQueue queue = createQueue();
    final boolean singleEntry = true;
    long dirtyVersion = 1;
    ReadPointer dirtyReadPointer = new MemoryReadPointer(Long.MAX_VALUE);

    QueueConsumer consumer1 = new QueueConsumer(0, queue.getGroupID(), 1);
    QueueConsumer consumer2 = new QueueConsumer(0, queue.getGroupID(), 1);
    QueueConsumer consumer3 = new QueueConsumer(0, queue.getGroupID(), 1);
    QueuePartitioner partitioner = new QueuePartitioner.RandomPartitioner();
    QueueConfig config = new QueueConfig(partitioner, singleEntry);
    
    // enable evict-on-ack for 3 groups
    int numGroups = 3;
    
    // enqueue 10 things
    for (int i=0; i<10; i++) {
      queue.enqueue(Bytes.toBytes(i), dirtyVersion);
    }
    
    // dequeue/ack/finalize 10 things w/ group1 and numGroups=3
    for (int i=0; i<10; i++) {
      DequeueResult result =
          queue.dequeue(consumer1, config, dirtyReadPointer);
      assertTrue(Bytes.equals(Bytes.toBytes(i), result.getValue()));
      assertTrue(
          queue.ack(result.getEntryPointer(), consumer1));
      assertTrue(
          queue.finalize(result.getEntryPointer(), consumer1, numGroups));
    }
    
    // dequeue is empty
    assertTrue(
        queue.dequeue(consumer1, config, dirtyReadPointer).isEmpty());
    
    // dequeue with consumer2 still has entries (expected)
    assertFalse(
        queue.dequeue(consumer2, config, dirtyReadPointer).isEmpty());
    
    // dequeue everything with consumer2
    for (int i=0; i<10; i++) {
      DequeueResult result =
          queue.dequeue(consumer2, config, dirtyReadPointer);
      assertTrue(Bytes.equals(Bytes.toBytes(i), result.getValue()));
      assertTrue(
          queue.ack(result.getEntryPointer(), consumer2));
      assertTrue(
          queue.finalize(result.getEntryPointer(), consumer2, numGroups));
    }

    // dequeue is empty
    assertTrue(
        queue.dequeue(consumer2, config, dirtyReadPointer).isEmpty());

    // dequeue with consumer3 still has entries (expected)
    assertFalse(
        queue.dequeue(consumer3, config, dirtyReadPointer).isEmpty());
    
    // dequeue everything except the last entry with consumer3
    for (int i=0; i<9; i++) {
      DequeueResult result =
          queue.dequeue(consumer3, config, dirtyReadPointer);
      assertTrue(Bytes.equals(Bytes.toBytes(i), result.getValue()));
      assertTrue(
          queue.ack(result.getEntryPointer(), consumer3));
      assertTrue(
          queue.finalize(result.getEntryPointer(), consumer3, numGroups));
    }
    
    // now the first 9 entries should have been physically evicted!
    
    // create a new consumer and dequeue, should get the 10th entry!
    QueueConsumer consumer4 = new QueueConsumer(0, queue.getGroupID(), 1);
    DequeueResult result = queue.dequeue(consumer4, config, dirtyReadPointer);
    assertTrue("Expected 9 but was " + Bytes.toInt(result.getValue()),
        Bytes.equals(Bytes.toBytes(9), result.getValue()));
    queue.ack(result.getEntryPointer(), consumer4);
    queue.finalize(result.getEntryPointer(), consumer4, numGroups);
    
    // dequeue again should be empty on consumer4
    assertTrue(
        queue.dequeue(consumer4, config, dirtyReadPointer).isEmpty());
    
    // dequeue is empty for 1 and 2
    assertTrue(
        queue.dequeue(consumer1, config, dirtyReadPointer).isEmpty());
    assertTrue(
        queue.dequeue(consumer2, config, dirtyReadPointer).isEmpty());
    
    // consumer 3 still gets entry 9
    result = queue.dequeue(consumer3, config, dirtyReadPointer);
    assertTrue("Expected 9 but was " + Bytes.toInt(result.getValue()),
        Bytes.equals(Bytes.toBytes(9), result.getValue()));
    queue.ack(result.getEntryPointer(), consumer3);
    // finalize now with numGroups+1
    queue.finalize(result.getEntryPointer(), consumer3, numGroups+1);
    
    // everyone is empty now!
    assertTrue(
        queue.dequeue(consumer1, config, dirtyReadPointer).isEmpty());
    assertTrue(
        queue.dequeue(consumer2, config, dirtyReadPointer).isEmpty());
    assertTrue(
        queue.dequeue(consumer3, config, dirtyReadPointer).isEmpty());
    assertTrue(
        queue.dequeue(consumer4, config, dirtyReadPointer).isEmpty());
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
    assertTrue(queue.finalize(result.getEntryPointer(), consumer, -1));

    // dequeue, should get second value
    result = queue.dequeue(consumer, config, dirtyReadPointer);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), valueTwo));

    // ack
    assertTrue(queue.ack(result.getEntryPointer(), consumer));
    assertTrue(queue.finalize(result.getEntryPointer(), consumer, -1));

    // verify queue is empty
    result = queue.dequeue(consumer, config, dirtyReadPointer);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testSingleConsumerAckSemantics() throws Exception {
    final boolean singleEntry = true;
    CConfiguration conf = new CConfiguration();
    long semiAckedTimeout = 50L;
    conf.setLong("ttqueue.entry.semiacked.max", semiAckedTimeout);
    TTQueue queue = createQueue(conf);
    long dirtyVersion = 1;
    ReadPointer dirtyReadPointer = new MemoryReadPointer(Long.MAX_VALUE);

    byte [] valueSemiAckedTimeout = Bytes.toBytes("semiAckedTimeout");
    byte [] valueSemiAckedToDequeued = Bytes.toBytes("semiAckedToDequeued");
    byte [] valueSemiAckedToAcked = Bytes.toBytes("semiAckedToAcked");

    // enqueue three entries
    assertTrue(queue.enqueue(valueSemiAckedTimeout, dirtyVersion).isSuccess());
    assertTrue(queue.enqueue(valueSemiAckedToDequeued, dirtyVersion).isSuccess());
    assertTrue(queue.enqueue(valueSemiAckedToAcked, dirtyVersion).isSuccess());

    // dequeue with the single consumer and random partitioner
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueuePartitioner partitioner = new QueuePartitioner.RandomPartitioner();
    QueueConfig config = new QueueConfig(partitioner, singleEntry);

    // get the first entry
    DequeueResult result = queue.dequeue(consumer, config, dirtyReadPointer);
    assertTrue(result.toString(), result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), valueSemiAckedTimeout));
    // ack it but that's it
    assertTrue(queue.ack(result.getEntryPointer(), consumer));


    // dequeue again, should get second entry (first is semi-acked)
    result = queue.dequeue(consumer, config, dirtyReadPointer);
    assertTrue(result.toString(), result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), valueSemiAckedToDequeued));
    // ack it, then unack it
    assertTrue(queue.ack(result.getEntryPointer(), consumer));
    assertTrue(queue.unack(result.getEntryPointer(), consumer));


    // dequeue again, should get second entry again
    result = queue.dequeue(consumer, config, dirtyReadPointer);
    assertTrue(result.toString(), result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), valueSemiAckedToDequeued));
    // ack it, then finalize it
    assertTrue(queue.ack(result.getEntryPointer(), consumer));
    assertTrue(queue.finalize(result.getEntryPointer(), consumer, -1));


    // dequeue again, should get third entry
    result = queue.dequeue(consumer, config, dirtyReadPointer);
    assertTrue(result.toString(), result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), valueSemiAckedToAcked));
    // ack it, then finalize it
    assertTrue(queue.ack(result.getEntryPointer(), consumer));
    assertTrue(queue.finalize(result.getEntryPointer(), consumer, -1));

    // queue should be empty
    assertTrue(queue.dequeue(consumer, config, dirtyReadPointer).isEmpty());

    // since there are no pending entries, we can change our config
    QueueConfig newConfig = new QueueConfig(partitioner, !singleEntry);
    assertTrue(queue.dequeue(consumer, newConfig, dirtyReadPointer).isEmpty());

    // now sleep timeout+1 to allow semi-ack to timeout
    Thread.sleep(semiAckedTimeout + 1);

    // queue should be empty still, and both configs should work
    assertTrue(queue.dequeue(consumer, config, dirtyReadPointer).isEmpty());
    assertTrue(queue.dequeue(consumer, newConfig, dirtyReadPointer).isEmpty());
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
    assertTrue(queue.finalize(resultTwo.getEntryPointer(), consumer, -1));

    // dequeue, should get third value
    DequeueResult resultThree = queue.dequeue(consumer, config, readPointer);
    assertTrue(Bytes.equals(resultThree.getValue(), Bytes.toBytes(3)));

    // ack
    assertTrue(queue.ack(resultThree.getEntryPointer(), consumer));
    assertTrue(queue.finalize(resultThree.getEntryPointer(), consumer, -1));

    // dequeue fourth
    DequeueResult resultFour = queue.dequeue(consumer, config, readPointer);
    assertTrue(Bytes.equals(resultFour.getValue(), Bytes.toBytes(4)));

    // dequeue five through ten, and ack them
    for (int i=5; i<11; i++) {
      DequeueResult result = queue.dequeue(consumer, config, readPointer);
      assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(i)));
      assertTrue(queue.ack(result.getEntryPointer(), consumer));
      assertTrue(queue.finalize(result.getEntryPointer(), consumer, -1));
    }

    // verify queue is empty (first and fourth not ackd but still dequeued)
    assertTrue(queue.dequeue(consumer, config, readPointer).isEmpty());

    // second and third ackd, another ack should fail
    assertFalse(queue.ack(resultTwo.getEntryPointer(), consumer));
    assertFalse(queue.ack(resultThree.getEntryPointer(), consumer));
    // first and fourth are not acked, ack should pass
    assertTrue(queue.ack(resultOne.getEntryPointer(), consumer));
    assertTrue(queue.finalize(resultOne.getEntryPointer(), consumer, -1));
    assertTrue(queue.ack(resultFour.getEntryPointer(), consumer));
    assertTrue(queue.finalize(resultFour.getEntryPointer(), consumer, -1));

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
    assertTrue(queue.finalize(resultTwo.getEntryPointer(), consumer, -1));

    // dequeue, should get third value
    DequeueResult resultThree = queue.dequeue(consumer, config, readPointer);
    assertTrue(Bytes.equals(resultThree.getValue(), Bytes.toBytes(3)));

    // ack
    assertTrue(queue.ack(resultThree.getEntryPointer(), consumer));
    assertTrue(queue.finalize(resultThree.getEntryPointer(), consumer, -1));

    // dequeue fourth
    DequeueResult resultFour = queue.dequeue(consumer, config, readPointer);
    assertTrue(Bytes.equals(resultFour.getValue(), Bytes.toBytes(4)));

    // dequeue five through ten, and ack them
    for (int i=5; i<11; i++) {
      DequeueResult result = queue.dequeue(consumer, config, readPointer);
      assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(i)));
      assertTrue(queue.ack(result.getEntryPointer(), consumer));
      assertTrue(queue.finalize(result.getEntryPointer(), consumer, -1));
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
    assertTrue(queue.finalize(resultOne.getEntryPointer(), consumer, -1));
    assertTrue(queue.ack(resultFourB.getEntryPointer(), consumer));
    assertTrue(queue.finalize(resultFourB.getEntryPointer(), consumer, -1));

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
    assertTrue(queue.finalize(resultTwo.getEntryPointer(), consumer, -1));

    // dequeue again, multi mode should get the third value
    DequeueResult resultThree = queue.dequeue(consumer, multiConfig, readPointer);
    assertTrue(Bytes.equals(resultThree.getValue(), Bytes.toBytes(3)));

    // ack result three
    assertTrue(queue.ack(resultThree.getEntryPointer(), consumer));
    assertTrue(queue.finalize(resultThree.getEntryPointer(), consumer, -1));

    // one is still not acked, queue is not empty

    // attempt to change to single entry mode should fail
    QueueConfig singleConfig = new QueueConfig(partitioner, true);
    assertTrue(queue.dequeue(consumer, singleConfig, readPointer).isFailure());

    // ack entry one
    assertTrue(queue.ack(resultOne.getEntryPointer(), consumer));
    assertTrue(queue.finalize(resultOne.getEntryPointer(), consumer, -1));

    // everything is empty now, should be able to change config
    assertTrue(queue.dequeue(consumer, singleConfig, readPointer).isEmpty());

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
    assertTrue(queue.finalize(resultOne.getEntryPointer(), consumer, -1));

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
    assertTrue(queue.finalize(resultTwoB.getEntryPointer(), consumer, -1));

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
    assertTrue(queue.finalize(resultFourA.getEntryPointer(), consumer, -1));

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
    assertTrue(queue.finalize(resultM0One.getEntryPointer(), consumer, -1));
    assertTrue(queue.ack(resultM0Two.getEntryPointer(), consumers[0]));
    assertTrue(queue.finalize(resultM0Two.getEntryPointer(), consumer, -1));
    assertTrue(queue.ack(resultM1Three.getEntryPointer(), consumers[1]));
    assertTrue(queue.finalize(resultM1Three.getEntryPointer(), consumer, -1));

    // both still see empty
    assertTrue(queue.dequeue(consumers[0], multiConfig, readPointer).isEmpty());
    assertTrue(queue.dequeue(consumers[1], multiConfig, readPointer).isEmpty());
  }

  @Test
  public void testSingleConsumerSingleGroup_dynamicReconfig() throws Exception {
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

    // single consumer in this test, switch between single and multi mode
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueuePartitioner partitioner = new QueuePartitioner.RandomPartitioner();
    QueueConfig multiConfig = new QueueConfig(partitioner, false);
    QueueConfig singleConfig = new QueueConfig(partitioner, true);
    
    // use single config first
    QueueConfig config = singleConfig;
    
    // dequeue and ack the first entry, value = 1
    DequeueResult result = queue.dequeue(consumer, config, readPointer);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(1)));
    assertTrue(queue.ack(result.getEntryPointer(), consumer));
    assertTrue(queue.finalize(result.getEntryPointer(), consumer, -1));
    
    // changing configuration to multi should work fine, should get next entry
    // value = 2
    config = multiConfig;
    result = queue.dequeue(consumer, config, readPointer);
    DequeueResult value2result = result;
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(2)));
    // but don't ack yet
    
    // changing configuration back to single should not work (pending entry)
    config = singleConfig;
    result = queue.dequeue(consumer, config, readPointer);
    assertTrue(result.isFailure());
    
    // back to multi should work and give entry value = 3, ack it
    config = multiConfig;
    result = queue.dequeue(consumer, config, readPointer);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(3)));
    assertTrue(queue.ack(result.getEntryPointer(), consumer));
    
    // changing configuration back to single should still not work
    config = singleConfig;
    result = queue.dequeue(consumer, config, readPointer);
    assertTrue(result.isFailure());
    
    // back to multi should work and give entry value = 4, ack it
    config = multiConfig;
    result = queue.dequeue(consumer, config, readPointer);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(4)));
    assertTrue(queue.ack(result.getEntryPointer(), consumer));
    
    // we still have value=2 pending but dequeue will return empty in multiEntry
    result = queue.dequeue(consumer, config, readPointer);
    assertTrue(result.isEmpty());
    
    // but we can't change config because value=2 still pending
    config = singleConfig;
    result = queue.dequeue(consumer, config, readPointer);
    assertTrue(result.isFailure());
    
    // now ack value=2
    assertTrue(queue.ack(value2result.getEntryPointer(), consumer));
    
    // nothing pending and empty, can change config
    config = singleConfig;
    result = queue.dequeue(consumer, config, readPointer);
    assertTrue(result.isEmpty());
    
    // enqueue twice, dequeue/ack once
    queue.enqueue(Bytes.toBytes(5), version);
    queue.enqueue(Bytes.toBytes(6), version);
    result = queue.dequeue(consumer, config, readPointer);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(5)));
    assertTrue(queue.ack(result.getEntryPointer(), consumer));
    assertTrue(queue.finalize(result.getEntryPointer(), consumer, -1));
    
    // change config and dequeue
    config = multiConfig;
    result = queue.dequeue(consumer, config, readPointer);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(6)));
    assertTrue(queue.ack(result.getEntryPointer(), consumer));
    assertTrue(queue.finalize(result.getEntryPointer(), consumer, -1));
    
    // nothing pending and empty, can change config
    config = singleConfig;
    result = queue.dequeue(consumer, config, readPointer);
    assertTrue(result.isEmpty());
    
  }

  @Test
  public void testMultiConsumerSingleGroup_dynamicReconfig() throws Exception {
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = new MemoryReadPointer(version);

    // enqueue one hundred entries
    int n=100;
    EnqueueResult [] results = new EnqueueResult[n];
    for (int i=0;i<n;i++) {
      results[i] = queue.enqueue(Bytes.toBytes(i+1), version);
      assertTrue(results[i].isSuccess());
    }
    // we want to verify at the end of the test we acked every entry
    Set<Integer> acked = new TreeSet<Integer>();

    // two consumers with a hash partitioner, both single mode
    QueueConsumer consumer1 = new QueueConsumer(0, 0, 2);
    QueueConsumer consumer2 = new QueueConsumer(1, 0, 2);
    QueuePartitioner partitioner = new QueuePartitioner.HashPartitioner();
    QueueConfig multiConfig = new QueueConfig(partitioner, false);
    
    // use multi config
    QueueConfig config = multiConfig;
    
    // dequeue all entries for consumer 1 but only ack until the first hole
    boolean ack = true;
    int last = -1;
    Map<Integer,QueueEntryPointer> consumer1unacked =
        new TreeMap<Integer,QueueEntryPointer>();
    while (true) {
      DequeueResult result = queue.dequeue(consumer1, config, readPointer);
      assertFalse(result.isFailure());
      if (result.isEmpty()) break;
      assertTrue(result.isSuccess());
      int value = Bytes.toInt(result.getValue());
      System.out.println("Consumer 1 dequeued value = "+ value);
      if (last > 0 && value != last + 1) ack = false;
      if (ack) {
        assertTrue(queue.ack(result.getEntryPointer(), consumer1));
        assertTrue(queue.finalize(result.getEntryPointer(), consumer1, -1));
        assertTrue(acked.add(value));
        System.out.println("Consumer 1 acked value = "+ value);
        last = value;
      } else {
        consumer1unacked.put(value, result.getEntryPointer());
      }
    }
    
    // everything for consumer 1 is dequeued but not acked, there is a gap
    
    // we should not be able to reconfigure (try to introduce consumer 3)
    QueueConsumer consumer3 = new QueueConsumer(2, 0, 3);
    DequeueResult result = queue.dequeue(consumer3, config, readPointer);
    assertTrue(result.isFailure());
    
    // iterate back over unacked consumer 1 entries and ack everything
    for (Map.Entry<Integer,QueueEntryPointer> entry:
        consumer1unacked.entrySet()) {
      assertTrue(queue.ack(entry.getValue(), consumer1));
      assertTrue(queue.finalize(entry.getValue(), consumer1, -1));
      assertTrue(acked.add(entry.getKey()));
      System.out.println("Consumer 1 acked value = "+ entry.getKey());
    }
    
    // now we can reconfigure to 3 consumers
    result = queue.dequeue(consumer3, config, readPointer);
    assertTrue("Expected success but was " + result, result.isSuccess());
    
    // dequeue/ack all entries with consumer 3
    while (true) {
      assertFalse(result.isFailure());
      if (result.isEmpty()) break;
      assertTrue(result.isSuccess());
      int value = Bytes.toInt(result.getValue());
      assertTrue(queue.ack(result.getEntryPointer(), consumer3));
      assertTrue(queue.finalize(result.getEntryPointer(), consumer3, -1));
      assertTrue(acked.add(value));
      result = queue.dequeue(consumer3, config, readPointer);
    }
    
    // reconfigure again back to 2 and dequeue everything
    Map<Integer,QueueEntryPointer> consumer2unacked =
        new TreeMap<Integer,QueueEntryPointer>();
    while (true) {
      result = queue.dequeue(consumer2, config, readPointer);
      assertFalse(result.isFailure());
      if (result.isEmpty()) break;
      assertTrue(result.isSuccess());
      consumer2unacked.put(Bytes.toInt(result.getValue()),
          result.getEntryPointer());
    }
    
    // should not be able to introduced consumer 4 (pending entries from 2)
    QueueConsumer consumer4 = new QueueConsumer(3, 0, 4);
    result = queue.dequeue(consumer4, config, readPointer);
    assertTrue(result.isFailure());
    
    // ack all entries with consumer 2
    for (Map.Entry<Integer,QueueEntryPointer> entry:
        consumer2unacked.entrySet()) {
      assertTrue(queue.ack(entry.getValue(), consumer2));
      assertTrue(queue.finalize(entry.getValue(), consumer2, -1));
      assertTrue(acked.add(entry.getKey()));
      System.out.println("Consumer 2 acked value = "+ entry.getKey());
    }
    
    // now introduce consumer 4, should be valid but empty
    result = queue.dequeue(consumer4, config, readPointer);
    assertTrue(result.isEmpty()); 
    
    // size of set should be equal to number of entries
    assertEquals(n, acked.size());
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
    assertTrue(queue.finalize(result.getEntryPointer(), consumer, -1));

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
    assertTrue(queue.ack(result.getEntryPointer(), consumer));
    assertTrue(queue.finalize(result.getEntryPointer(), consumer, -1));

    // verify queue is empty
    assertTrue(queue.dequeue(consumer, config, readPointer).isEmpty());

    // shut down
    dequeuer.shutdown();
  }

  @Test
  public void testConcurrentEnqueueDequeue() throws Exception {
    final TTQueue queue = createQueue();
    final long version = timeOracle.getTimestamp();
    final ReadPointer readPointer = new MemoryReadPointer(version);
    AtomicLong dequeueReturns = ((TTQueueOnVCTable)queue).dequeueReturns;

    final int n = getNumIterations();

    // Create and start a thread that dequeues in a loop
    final QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    final QueueConfig config = new QueueConfig(
        new QueuePartitioner.RandomPartitioner(), true);
    final AtomicBoolean stop = new AtomicBoolean(false);
    final Set<byte[]> dequeued = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    final AtomicLong numEmpty = new AtomicLong(0);
    Thread dequeueThread = new Thread() {
      @Override
      public void run() {
        boolean lastSuccess = false;
        while (lastSuccess || !stop.get()) {
          DequeueResult result = queue.dequeue(consumer, config, readPointer);
          if (result.isFailure()) {
            System.out.println("Dequeue failed! " + result);
            return;
          }
          if (result.isSuccess()) {
            dequeued.add(result.getValue());
            assertTrue(queue.ack(result.getEntryPointer(), consumer));
            assertTrue(queue.finalize(result.getEntryPointer(), consumer, -1));
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
        for (int i=0; i<n; i++) {
          EnqueueResult result = queue.enqueue(Bytes.toBytes(i), version);
          assertTrue(result.isSuccess());
        }
      }
    };
    enqueueThread.start();

    // Join the enqueuer
    enqueueThread.join();

    // Tell the dequeuer to stop (once he gets an empty)
    stop.set(true);
    dequeueThread.join();
    System.out.println("DequeueThread is done.  Set size is " +
        dequeued.size() + ", Number of empty returns is " + numEmpty.get());

    // Should have dequeued n entries
    assertEquals(n, dequeued.size());

    // And dequeuedEntries should be >= n
    assertTrue(n <= dequeueReturns.get());
  }

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

    long numEnqueues = 0;
    // enqueue the first four values
    for (int i=0; i<n; i++) {
      assertTrue(queue.enqueue(values[i], version).isSuccess());
      numEnqueues++;
    }

    // wait for n^2 more queuedequeue() returns
    expectedQueuedequeues += (n*n);
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
    expectedQueuedequeues += (n*n);
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
    expectedQueuedequeues += (n*n);
    waitForAndAssertCount(expectedQueuedequeues, dequeueReturns);

    // ack it for groups(0,1) consumers(2,3)
    for (int i=0; i<n/2; i++) {
      for (int j=n/2; j<n; j++) {
        DequeueResult result = queue.dequeue(consumers[i][j], config,
            readPointer);
        assertNotNull(result);
        assertEquals(j, Bytes.toLong(result.getValue()));
        assertTrue(queue.ack(result.getEntryPointer(), consumers[i][j]));
        assertTrue(queue.finalize(result.getEntryPointer(), consumers[i][j],
            -1));
        System.out.println("ACK: i=" + i + ", j=" + j);
      }
    }

    // wait for n more queuedequeue() returns
    expectedQueuedequeues += n;
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

    // wait for (n-1)(n) more queuedequeue() returns
    expectedQueuedequeues += (n*(n-1));
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
          assertTrue(queue.finalize(result.getEntryPointer(), consumers[i][j],
              -1));
          // buffer of dequeuer should still have that result
          waitForAndAssertCount(numdequeues, dequeuers[i][j].dequeues);
          result = dequeuers[i][j].blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
          assertNotNull(result);
          assertEquals(j, Bytes.toLong(result.getValue()));
        }
      }
    }

    // wait for (n*(n-1)) more queuedequeue() returns
    expectedQueuedequeues += (n*(n-1));
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
      numEnqueues++;
    }

    // wait for n^2 more queuedequeue() wake-ups
    expectedQueuedequeues += (n*n);
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
          assertEquals("i=" + i + ", j=" + j + ", k=" + k + ", threadid=" +
              dequeuers[i][j].getId(),
              (long)(k*n)+j, Bytes.toLong(result.getValue()));
          assertTrue("i=" + i + ",j=" + j + ",k=" + k,
              queue.ack(result.getEntryPointer(), consumers[i][j]));
          assertTrue(queue.finalize(result.getEntryPointer(), consumers[i][j],
              -1));
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

    // check on the queue meta
    QueueMeta meta = queue.getQueueMeta();
    assertEquals(numEnqueues, meta.currentWritePointer);
    assertEquals(numEnqueues, meta.globalHeadPointer);
    assertEquals(n, meta.groups.length);
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
            System.out.println("Result failed! (" + result + ")");
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
