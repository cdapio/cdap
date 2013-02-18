package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicTimeOracle;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class TestTTQueue {

  private static final long MAX_TIMEOUT_MS = 10000;
  private static final long DEQUEUE_BLOCK_TIMEOUT_MS = 1;

  protected static TimestampOracle timeOracle =
      new MemoryStrictlyMonotonicTimeOracle();

  private TTQueue createQueue() throws OperationException {
    return createQueue(new CConfiguration());
  }

  protected abstract TTQueue createQueue(CConfiguration conf)
      throws OperationException;

  protected abstract int getNumIterations();

  @Test
  public void testLotsOfAsyncDequeueing() throws Exception {
    TTQueue queue = createQueue();
    long dirtyVersion = 1;

    long startTime = System.currentTimeMillis();

    int numEntries = getNumIterations();

    for (int i=1; i<numEntries+1; i++) {
      queue.enqueue(Bytes.toBytes(i), null, dirtyVersion);
    }
    System.out.println("Done enqueueing");

    long enqueueStop = System.currentTimeMillis();

    System.out.println("Finished enqueue of " + numEntries + " entries in " +
        (enqueueStop-startTime) + " ms (" +
        (enqueueStop-startTime)/((float)numEntries) + " ms/entry)");

    QueueConsumer consumerSync = new QueueConsumer(0, 0, 1, new QueueConfig(PartitionerType.FIFO, true));
    for (int i=1; i<numEntries+1; i++) {
      MemoryReadPointer rp = new MemoryReadPointer(timeOracle.getTimestamp());
      DequeueResult result = queue.dequeue(consumerSync, rp);
      assertTrue(result.isSuccess());
      assertTrue(Bytes.equals(Bytes.toBytes(i), result.getValue()));
      queue.ack(result.getEntryPointer(), consumerSync, rp);
      queue.finalize(result.getEntryPointer(), consumerSync, -1);
      if (i % 100 == 0) System.out.print(".");
      if (i % 1000 == 0) System.out.println(" " + i);
    }

    long dequeueSyncStop = System.currentTimeMillis();

    System.out.println("Finished sync dequeue of " + numEntries + " entries in " +
        (dequeueSyncStop-enqueueStop) + " ms (" +
        (dequeueSyncStop-enqueueStop)/((float)numEntries) + " ms/entry)");

    // Async

    QueueConfig configAsync = new QueueConfig(PartitionerType.FIFO, false);
    QueueConsumer consumerAsync = new QueueConsumer(0, 2, 1, configAsync);
    for (int i=1; i<numEntries+1; i++) {
      DequeueResult result =
          queue.dequeue(consumerAsync, new MemoryReadPointer(timeOracle.getTimestamp()));
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
    assertTrue(queue.dequeue(consumerSync, new MemoryReadPointer(timeOracle.getTimestamp())).isEmpty());
    assertTrue(queue.dequeue(consumerAsync, new MemoryReadPointer(timeOracle.getTimestamp())).isEmpty());
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

  private ReadPointer getDirtyPointer() {
    return new MemoryReadPointer(
        Long.MAX_VALUE, timeOracle.getTimestamp(), null);
  }
  
  private ReadPointer getCleanPointer() {
    return getCleanPointer(timeOracle.getTimestamp());
  }
  
  private ReadPointer getCleanPointer(long now) {
    return new MemoryReadPointer(now + 1, now, null);
  }
  
  @Test
  public void testEvictOnAck_OneGroup() throws Exception {
    long dirtyVersion = 1;
    ReadPointer dirtyReadPointer = getDirtyPointer();

//    QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);
    QueueConsumer consumer2 = new QueueConsumer(0, 1, 1, config);

    // first try with evict-on-ack off
    TTQueue queueNormal = createQueue();
    int numGroups = -1;

    // enqueue 10 things
    for (int i=0; i<10; i++) {
      queueNormal.enqueue(Bytes.toBytes(i), null, dirtyVersion);
    }

    // dequeue/ack/finalize 10 things w/ numGroups=-1
    for (int i=0; i<10; i++) {
      DequeueResult result =
          queueNormal.dequeue(consumer, dirtyReadPointer);
      Assert.assertFalse(result.isEmpty());
      queueNormal.ack(result.getEntryPointer(), consumer, dirtyReadPointer);
      queueNormal.finalize(result.getEntryPointer(), consumer, numGroups);
    }

    // dequeue is empty
    assertTrue(
        queueNormal.dequeue(consumer, dirtyReadPointer).isEmpty());

    // dequeue with new consumer still has entries (expected)
    assertFalse(
        queueNormal.dequeue(consumer2, dirtyReadPointer).isEmpty());

    // now do it again with evict-on-ack turned on
    TTQueue queueEvict = createQueue();
    numGroups = 1;

    // enqueue 10 things
    for (int i=0; i<10; i++) {
      queueEvict.enqueue(Bytes.toBytes(i), null, dirtyVersion);
    }

    // dequeue/ack/finalize 10 things w/ numGroups=1
    for (int i=0; i<10; i++) {
      DequeueResult result =
          queueEvict.dequeue(consumer, dirtyReadPointer);
      queueEvict.ack(result.getEntryPointer(), consumer, dirtyReadPointer);
      queueEvict.finalize(result.getEntryPointer(), consumer, numGroups);
    }

    // dequeue is empty
    assertTrue(
        queueEvict.dequeue(consumer, dirtyReadPointer).isEmpty());

    // dequeue with new consumer IS NOW EMPTY!
    assertTrue(
        queueEvict.dequeue(consumer2, dirtyReadPointer).isEmpty());


  }

  @Test
  public void testEvictOnAck_ThreeGroups() throws Exception {
    TTQueue queue = createQueue();
    final boolean singleEntry = true;
    long dirtyVersion = 1;
    ReadPointer dirtyReadPointer = getDirtyPointer();

    QueueConfig config = new QueueConfig(PartitionerType.FIFO, singleEntry);
    QueueConsumer consumer1 = new QueueConsumer(0, queue.getGroupID(), 1, config);
    QueueConsumer consumer2 = new QueueConsumer(0, queue.getGroupID(), 1, config);
    QueueConsumer consumer3 = new QueueConsumer(0, queue.getGroupID(), 1, config);

    // enable evict-on-ack for 3 groups
    int numGroups = 3;

    // enqueue 10 things
    for (int i=0; i<10; i++) {
      queue.enqueue(Bytes.toBytes(i), null, dirtyVersion);
    }

    // dequeue/ack/finalize 10 things w/ group1 and numGroups=3
    for (int i=0; i<10; i++) {
      DequeueResult result =
          queue.dequeue(consumer1, dirtyReadPointer);
      assertTrue(Bytes.equals(Bytes.toBytes(i), result.getValue()));
      queue.ack(result.getEntryPointer(), consumer1, dirtyReadPointer);
      queue.finalize(result.getEntryPointer(), consumer1, numGroups);
    }

    // dequeue is empty
    assertTrue(
        queue.dequeue(consumer1, dirtyReadPointer).isEmpty());

    // dequeue with consumer2 still has entries (expected)
    assertFalse(
        queue.dequeue(consumer2, dirtyReadPointer).isEmpty());

    // dequeue everything with consumer2
    for (int i=0; i<10; i++) {
      DequeueResult result =
          queue.dequeue(consumer2, dirtyReadPointer);
      assertTrue(Bytes.equals(Bytes.toBytes(i), result.getValue()));
      queue.ack(result.getEntryPointer(), consumer2, dirtyReadPointer);
      queue.finalize(result.getEntryPointer(), consumer2, numGroups);
    }

    // dequeue is empty
    assertTrue(
        queue.dequeue(consumer2, dirtyReadPointer).isEmpty());

    // dequeue with consumer3 still has entries (expected)
    assertFalse(
        queue.dequeue(consumer3, dirtyReadPointer).isEmpty());

    // dequeue everything except the last entry with consumer3
    for (int i=0; i<9; i++) {
      DequeueResult result =
          queue.dequeue(consumer3, dirtyReadPointer);
      assertTrue(Bytes.equals(Bytes.toBytes(i), result.getValue()));
      queue.ack(result.getEntryPointer(), consumer3, dirtyReadPointer);
      queue.finalize(result.getEntryPointer(), consumer3, numGroups);
    }

    // now the first 9 entries should have been physically evicted!

    // create a new consumer and dequeue, should get the 10th entry!
    QueueConsumer consumer4 = new QueueConsumer(0, queue.getGroupID(), 1,config);
    DequeueResult result = queue.dequeue(consumer4, dirtyReadPointer);
    assertTrue("Expected 9 but was " + Bytes.toInt(result.getValue()),
        Bytes.equals(Bytes.toBytes(9), result.getValue()));
    queue.ack(result.getEntryPointer(), consumer4, dirtyReadPointer);
    queue.finalize(result.getEntryPointer(), consumer4, ++numGroups); // numGroups=4

    // TODO: there is some weirdness here.  is the new native queue correct in
    //       behavior or are the old ttqueue implementations correct?
    
    // dequeue again should be empty on consumer4
    assertTrue(
        queue.dequeue(consumer4, dirtyReadPointer).isEmpty());

    // dequeue is empty for 1 and 2
    assertTrue(
        queue.dequeue(consumer1, dirtyReadPointer).isEmpty());
    assertTrue(
        queue.dequeue(consumer2, dirtyReadPointer).isEmpty());

    // consumer 3 still gets entry 9
    result = queue.dequeue(consumer3, dirtyReadPointer);
    assertTrue("Expected 9 but was " + Bytes.toInt(result.getValue()),
        Bytes.equals(Bytes.toBytes(9), result.getValue()));
    queue.ack(result.getEntryPointer(), consumer3, dirtyReadPointer);
    // finalize now with numGroups=4
    queue.finalize(result.getEntryPointer(), consumer3, numGroups);

    // everyone is empty now!
    assertTrue(
        queue.dequeue(consumer1, dirtyReadPointer).isEmpty());
    assertTrue(
        queue.dequeue(consumer2, dirtyReadPointer).isEmpty());
    assertTrue(
        queue.dequeue(consumer3, dirtyReadPointer).isEmpty());
    assertTrue(
        queue.dequeue(consumer4, dirtyReadPointer).isEmpty());
  }

  @Test
  public void testSingleConsumerSimple() throws Exception {
    final boolean singleEntry = true;
    TTQueue queue = createQueue();
    long dirtyVersion = 1;
    ReadPointer dirtyReadPointer = getDirtyPointer();

    byte [] valueOne = Bytes.toBytes("value1");
    byte [] valueTwo = Bytes.toBytes("value2");

    // enqueue two entries
    assertTrue(queue.enqueue(valueOne, null, dirtyVersion).isSuccess());
    assertTrue(queue.enqueue(valueTwo, null, dirtyVersion).isSuccess());

    // dequeue it with the single consumer and FIFO partitioner
    QueueConfig config = new QueueConfig(PartitionerType.FIFO, singleEntry);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);
    DequeueResult result = queue.dequeue(consumer, dirtyReadPointer);

    // verify we got something and it's the first value
    assertTrue(result.toString(), result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), valueOne));

    // dequeue again without acking, should still get first value
    result = queue.dequeue(consumer, dirtyReadPointer);
    assertTrue(result.isSuccess());
    assertTrue("Expected (" + Bytes.toString(valueOne) + ") Got (" +
        Bytes.toString(result.getValue()) + ")",
        Bytes.equals(result.getValue(), valueOne));

    // ack
    queue.ack(result.getEntryPointer(), consumer, dirtyReadPointer);
    queue.finalize(result.getEntryPointer(), consumer, -1);

    // dequeue, should get second value
    result = queue.dequeue(consumer, dirtyReadPointer);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), valueTwo));

    // ack
    queue.ack(result.getEntryPointer(), consumer, dirtyReadPointer);
    queue.finalize(result.getEntryPointer(), consumer, -1);

    // verify queue is empty
    result = queue.dequeue(consumer, dirtyReadPointer);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testSingleConsumerAckSemantics() throws Exception {
    CConfiguration conf = new CConfiguration();
    long semiAckedTimeout = 50L;
    conf.setLong("ttqueue.entry.semiacked.max", semiAckedTimeout);
    TTQueue queue = createQueue(conf);
    long dirtyVersion = 1;
    ReadPointer dirtyReadPointer = getDirtyPointer();

    byte [] valueSemiAckedTimeout = Bytes.toBytes("semiAckedTimeout");
    byte [] valueSemiAckedToDequeued = Bytes.toBytes("semiAckedToDequeued");
    byte [] valueSemiAckedToAcked = Bytes.toBytes("semiAckedToAcked");

    // enqueue three entries
    assertTrue(queue.enqueue(valueSemiAckedTimeout, null, dirtyVersion).isSuccess());
    assertTrue(queue.enqueue(valueSemiAckedToDequeued, null, dirtyVersion).isSuccess());
    assertTrue(queue.enqueue(valueSemiAckedToAcked, null, dirtyVersion).isSuccess());

    // dequeue with the single consumer and FIFO partitioner
    QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);

    // get the first entry
    DequeueResult result = queue.dequeue(consumer, dirtyReadPointer);
    assertTrue(result.toString(), result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), valueSemiAckedTimeout));
    // ack it but that's it
    queue.ack(result.getEntryPointer(), consumer, dirtyReadPointer);


    // dequeue again, should get second entry (first is semi-acked)
    result = queue.dequeue(consumer, dirtyReadPointer);
    assertTrue(result.toString(), result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), valueSemiAckedToDequeued));
    // ack it, then unack it
    queue.ack(result.getEntryPointer(), consumer, dirtyReadPointer);
    queue.unack(result.getEntryPointer(), consumer, dirtyReadPointer);


    // dequeue again, should get second entry again
    result = queue.dequeue(consumer, dirtyReadPointer);
    assertTrue(result.toString(), result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), valueSemiAckedToDequeued));
    // ack it, then finalize it
    queue.ack(result.getEntryPointer(), consumer, dirtyReadPointer);
    queue.finalize(result.getEntryPointer(), consumer, -1);


    // dequeue again, should get third entry
    result = queue.dequeue(consumer, dirtyReadPointer);
    assertTrue(result.toString(), result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), valueSemiAckedToAcked));
    // ack it, then finalize it
    queue.ack(result.getEntryPointer(), consumer, dirtyReadPointer);
    queue.finalize(result.getEntryPointer(), consumer, -1);

    // queue should be empty
    assertTrue(queue.dequeue(consumer, dirtyReadPointer).isEmpty());

    // since there are no pending entries, we can change our config
    QueueConfig newConfig = new QueueConfig(PartitionerType.FIFO, false);
    assertTrue(queue.dequeue(consumer, newConfig, dirtyReadPointer).isEmpty());

    // now sleep timeout+1 to allow semi-ack to timeout
    Thread.sleep(semiAckedTimeout + 1);

    // queue should be empty still, and both configs should work
    assertTrue(queue.dequeue(consumer, config, dirtyReadPointer).isEmpty());
    assertTrue(queue.dequeue(consumer, newConfig, dirtyReadPointer).isEmpty());
  }

  @Test
  public void testSingleConsumerMulti() throws Exception {
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = getCleanPointer(version);

    // enqueue ten entries
    int n=10;
    for (int i=0;i<n;i++) {
      assertTrue(queue.enqueue(Bytes.toBytes(i+1), null, version).isSuccess());
    }

    // dequeue it with the single consumer and FIFO partitioner
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, new QueueConfig(PartitionerType.FIFO, false));

    // verify it's the first value
    DequeueResult resultOne = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultOne.getValue(), Bytes.toBytes(1)));

    // dequeue again without acking, async mode should get the second value
    DequeueResult resultTwo = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultTwo.getValue(), Bytes.toBytes(2)));

    // ack result two
    queue.ack(resultTwo.getEntryPointer(), consumer, readPointer);
    queue.finalize(resultTwo.getEntryPointer(), consumer, -1);

    // dequeue, should get third value
    DequeueResult resultThree = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultThree.getValue(), Bytes.toBytes(3)));

    // ack
    queue.ack(resultThree.getEntryPointer(), consumer, readPointer);
    queue.finalize(resultThree.getEntryPointer(), consumer, -1);

    // dequeue fourth
    DequeueResult resultFour = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultFour.getValue(), Bytes.toBytes(4)));

    // dequeue five through ten, and ack them
    for (int i=5; i<11; i++) {
      DequeueResult result = queue.dequeue(consumer, readPointer);
      assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(i)));
      queue.ack(result.getEntryPointer(), consumer, readPointer);
      queue.finalize(result.getEntryPointer(), consumer, -1);
    }

    // verify queue is empty (first and fourth not ackd but still dequeued)
    assertTrue(queue.dequeue(consumer, readPointer).isEmpty());

    // second and third ackd, another ack should fail
    // second and third ackd, another ack should fail
    try {
      queue.ack(resultTwo.getEntryPointer(), consumer, readPointer);
      fail("ack should fail.");
    } catch (OperationException e) {
      // expected
    }
    try {
      queue.ack(resultThree.getEntryPointer(), consumer, readPointer);
      fail("ack should fail.");
    } catch (OperationException e) {
      // expected
    }

    // first and fourth are not acked, ack should pass
    queue.ack(resultOne.getEntryPointer(), consumer, readPointer);
    queue.finalize(resultOne.getEntryPointer(), consumer, -1);
    queue.ack(resultFour.getEntryPointer(), consumer, readPointer);
    queue.finalize(resultFour.getEntryPointer(), consumer, -1);

    // queue still empty
    assertTrue(queue.dequeue(consumer, readPointer).isEmpty());
  }

  @Test
  public void testMultipleConsumerMultiTimeouts() throws Exception {
    final boolean singleEntry = false;
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = getCleanPointer(version);

    // enqueue ten entries
    int n=10;
    for (int i=0;i<n;i++) {
      assertTrue(queue.enqueue(Bytes.toBytes(i+1), null, version).isSuccess());
    }

    // dequeue it with the single consumer and FIFO partitioner
    QueueConfig config = new QueueConfig(PartitionerType.FIFO, singleEntry);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);

    // verify it's the first value
    DequeueResult resultOne = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultOne.getValue(), Bytes.toBytes(1)));

    // dequeue again without acking, async mode should get the second value
    DequeueResult resultTwo = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultTwo.getValue(), Bytes.toBytes(2)));

    // ack result two
    queue.ack(resultTwo.getEntryPointer(), consumer, readPointer);
    queue.finalize(resultTwo.getEntryPointer(), consumer, -1);

    // dequeue, should get third value
    DequeueResult resultThree = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultThree.getValue(), Bytes.toBytes(3)));

    // ack
    queue.ack(resultThree.getEntryPointer(), consumer, readPointer);
    queue.finalize(resultThree.getEntryPointer(), consumer, -1);

    // dequeue fourth
    DequeueResult resultFour = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultFour.getValue(), Bytes.toBytes(4)));

    // dequeue five through ten, and ack them
    for (int i=5; i<11; i++) {
      DequeueResult result = queue.dequeue(consumer, readPointer);
      assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(i)));
      queue.ack(result.getEntryPointer(), consumer, readPointer);
      queue.finalize(result.getEntryPointer(), consumer, -1);
    }

    // verify queue is empty (first and fourth not ackd but still dequeued)
    assertTrue(queue.dequeue(consumer, readPointer).isEmpty());

    // second and third ackd, another ack should fail
    try {
      queue.ack(resultTwo.getEntryPointer(), consumer, readPointer);
      fail("ack should fail.");
    } catch (OperationException e) {
      // expected
    }
    try {
      queue.ack(resultThree.getEntryPointer(), consumer, readPointer);
      fail("ack should fail.");
    } catch (OperationException e) {
      // expected
    }

    // queue still empty
    assertTrue(queue.dequeue(consumer, readPointer).isEmpty());

    // now set the timeout and sleep for timeout + 1
    long oldTimeout = 0L;
    if (queue instanceof TTQueueOnVCTable) {
      oldTimeout = ((TTQueueOnVCTable)queue).maxAgeBeforeExpirationInMillis;
      ((TTQueueOnVCTable)queue).maxAgeBeforeExpirationInMillis = 50;
    } else if (queue instanceof TTQueueOnHBaseNative) {
      oldTimeout = ((TTQueueOnHBaseNative)queue).expirationConfig
          .getMaxAgeBeforeExpirationInMillis();
      ((TTQueueOnHBaseNative)queue).expirationConfig
          .setMaxAgeBeforeExpirationInMillis(50);
    }
    Thread.sleep(51);

    // two dequeues in a row should give values one and four
    DequeueResult resultOneB = queue.dequeue(consumer, readPointer);
    assertNotNull(resultOneB);
    assertTrue(resultOneB.isSuccess());
    assertTrue(Bytes.equals(resultOneB.getValue(), Bytes.toBytes(1)));
    DequeueResult resultFourB = queue.dequeue(consumer, readPointer);
    assertNotNull(resultFourB);
    assertTrue(resultFourB.isSuccess());
    assertTrue(Bytes.equals(resultFourB.getValue(), Bytes.toBytes(4)));

    // and then queue should be empty again
    assertTrue(queue.dequeue(consumer, readPointer).isEmpty());

    // first and fourth are not acked, ack should pass using either result
    queue.ack(resultOne.getEntryPointer(), consumer, readPointer);
    queue.finalize(resultOne.getEntryPointer(), consumer, -1);
    queue.ack(resultFourB.getEntryPointer(), consumer, readPointer);
    queue.finalize(resultFourB.getEntryPointer(), consumer, -1);

    // using other result version should fail second time
    try {
      queue.ack(resultOneB.getEntryPointer(), consumer, readPointer);
      fail("ack should fail.");
    } catch (OperationException e) {
      // expected
    }
    try {
      queue.ack(resultFour.getEntryPointer(), consumer, readPointer);
      fail("ack should fail.");
    } catch (OperationException e) {
      // expected
    }

    // restore timeout
    if (queue instanceof TTQueueOnVCTable) {
      ((TTQueueOnVCTable)queue).maxAgeBeforeExpirationInMillis = oldTimeout;
    } else if (queue instanceof TTQueueOnHBaseNative) {
      ((TTQueueOnHBaseNative)queue).expirationConfig
          .setMaxAgeBeforeExpirationInMillis(oldTimeout);
    }
  }

  @Test
  public void testSingleConsumerMultiEntry_Empty_ChangeToSingleConsumerSingleEntry()
      throws Exception {
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = getCleanPointer(version);

    // enqueue 3 entries
    int n=3;
    for (int i=0;i<n;i++) {
      assertTrue(queue.enqueue(Bytes.toBytes(i+1), null, version).isSuccess());
    }

    // dequeue it with the single consumer and FIFO partitioner
    QueueConfig multiConfig = new QueueConfig(PartitionerType.FIFO, false);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, multiConfig);

    // verify it's the first value
    DequeueResult resultOne = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultOne.getValue(), Bytes.toBytes(1)));

    // dequeue again without acking, multi mode should get the second value
    DequeueResult resultTwo = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultTwo.getValue(), Bytes.toBytes(2)));

    // ack result two
    queue.ack(resultTwo.getEntryPointer(), consumer, readPointer);
    queue.finalize(resultTwo.getEntryPointer(), consumer, -1);

    // dequeue again, multi mode should get the third value
    DequeueResult resultThree = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultThree.getValue(), Bytes.toBytes(3)));

    // ack result three
    queue.ack(resultThree.getEntryPointer(), consumer, readPointer);
    queue.finalize(resultThree.getEntryPointer(), consumer, -1);

    // one is still not acked, queue is not empty

    // attempt to change to single entry mode should fail
    QueueConfig singleConfig = new QueueConfig(PartitionerType.FIFO, true);
    try {
      queue.dequeue(consumer, singleConfig, readPointer);
      fail("dequeue should fail because it changes single entry mode.");
    } catch (OperationException e) {
      // expected
    }

    // ack entry one
    queue.ack(resultOne.getEntryPointer(), consumer, readPointer);
    queue.finalize(resultOne.getEntryPointer(), consumer, -1);

    // everything is empty now, should be able to change config
    assertTrue(queue.dequeue(consumer, singleConfig, readPointer).isEmpty());

    // now we are empty, try to change modes now, should pass and be empty
    DequeueResult result = queue.dequeue(consumer, singleConfig, readPointer);
    assertTrue(result.toString(), result.isEmpty());

  }

  @Test
  public void testSingleConsumerSingleEntryWithInvalid_Empty_ChangeSizeAndToMulti() throws Exception {
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = getCleanPointer(version);

    // enqueue four entries
    int n=4;
    EnqueueResult [] results = new EnqueueResult[n];
    for (int i=0;i<n;i++) {
      results[i] = queue.enqueue(Bytes.toBytes(i+1), null, version);
      assertTrue(results[i].isSuccess());
    }

    // invalidate number 3
    queue.invalidate(results[2].getEntryPointer(), version);

    // dequeue with a single consumer and FIFO partitioner
    QueueConfig multiConfig = new QueueConfig(PartitionerType.FIFO, false);
    QueueConfig singleConfig = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, singleConfig);

    // use single entry first

    // verify it's the first value
    DequeueResult resultOne = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultOne.getValue(), Bytes.toBytes(1)));

    // dequeue again without acking, singleEntry mode should get the first value again
    DequeueResult resultOneB = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultOneB.getValue(), Bytes.toBytes(1)));

    // ack result one
    queue.ack(resultOne.getEntryPointer(), consumer, readPointer);
    queue.finalize(resultOne.getEntryPointer(), consumer, -1);

    // dequeue again without acking, get second value
    DequeueResult resultTwo = queue.dequeue(consumer, readPointer);
    assertTrue(Bytes.equals(resultTwo.getValue(), Bytes.toBytes(2)));

    // dequeue should give us back the un-ack'd stuff still
    DequeueResult resultTwoB = queue.dequeue(consumer, readPointer);
    assertNotNull(resultTwoB);
    assertTrue("expected 2, actual " + Bytes.toInt(resultTwoB.getValue()),
        Bytes.equals(resultTwoB.getValue(), Bytes.toBytes(2)));

    // same thing again
    DequeueResult resultTwoC = queue.dequeue(consumer, readPointer);
    assertNotNull(resultTwoC);
    assertTrue(Bytes.equals(resultTwoC.getValue(), Bytes.toBytes(2)));

    // ack
    queue.ack(resultTwoB.getEntryPointer(), consumer, readPointer);
    queue.finalize(resultTwoB.getEntryPointer(), consumer, -1);

    // dequeue again, should get four not three as it was invalidated
    DequeueResult resultFourA = queue.dequeue(consumer, readPointer);
    assertNotNull(resultFourA);
    assertTrue("Expected to read 4 but read: " +
          Bytes.toInt(resultFourA.getValue()),
        Bytes.equals(resultFourA.getValue(), Bytes.toBytes(4)));

    // trying to change to multi now should fail
    try {
      queue.dequeue(consumer, multiConfig, readPointer);
      fail("dequeue should fail because it changes to multi mode.");
    } catch (OperationException e) {
      // expected
    }

    // trying to change group size should also fail
    try {
      queue.dequeue(new QueueConsumer(0, 0, 2, singleConfig), readPointer);
      fail("dequeue should fail because it changes group size.");
    } catch (OperationException e) {
      // expected
    }

    // ack
    queue.ack(resultFourA.getEntryPointer(), consumer, readPointer);
    queue.finalize(resultFourA.getEntryPointer(), consumer, -1);

    // empty
    assertTrue(queue.dequeue(consumer, singleConfig, readPointer).isEmpty());

    // change modes to multi config and two consumer instances
    QueueConsumer [] consumers = new QueueConsumer[] {
        new QueueConsumer(0, 0, 2, multiConfig), new QueueConsumer(1, 0, 2, multiConfig)
    };

    // should be empty with new config, not failure
    assertTrue(queue.dequeue(consumers[0], readPointer).isEmpty());

    // enqueue three entries
    n=3;
    results = new EnqueueResult[n];
    for (int i=0;i<n;i++) {
      results[i] = queue.enqueue(Bytes.toBytes(i+1), null, version);
      assertTrue(results[i].isSuccess());
    }

    // dequeue two with consumer 0, should get 1 and 2
    DequeueResult resultM0One = queue.dequeue(consumers[0], readPointer);
    assertTrue(resultM0One.isSuccess());
    assertTrue(Bytes.equals(resultM0One.getValue(), Bytes.toBytes(1)));
    DequeueResult resultM0Two = queue.dequeue(consumers[0], readPointer);
    assertTrue(resultM0Two.isSuccess());
    assertTrue(Bytes.equals(resultM0Two.getValue(), Bytes.toBytes(2)));

    // dequeue one with consumer 1, should get 3
    DequeueResult resultM1Three = queue.dequeue(consumers[1], readPointer);
    assertTrue(resultM1Three.isSuccess());
    assertTrue(Bytes.equals(resultM1Three.getValue(), Bytes.toBytes(3)));

    // consumer 0 and consumer 1 should see empty now
    assertTrue(queue.dequeue(consumers[0], readPointer).isEmpty());
    assertTrue(queue.dequeue(consumers[1], readPointer).isEmpty());

    // verify consumer 1 can't ack one of consumer 0s entries and vice versa
    try {
      queue.ack(resultM0One.getEntryPointer(), consumers[1], readPointer);
      fail("ack should fail with wrong consumer id.");
    } catch (OperationException e) {
      // expected
    }
    try {
      queue.ack(resultM1Three.getEntryPointer(), consumers[0], readPointer);
      fail("ack should fail with wrong consumer id.");
    } catch (OperationException e) {
      // expected
    }

    // ack everything correctly
    queue.ack(resultM0One.getEntryPointer(), consumers[0], readPointer);
    queue.finalize(resultM0One.getEntryPointer(), consumer, -1);
    queue.ack(resultM0Two.getEntryPointer(), consumers[0], readPointer);
    queue.finalize(resultM0Two.getEntryPointer(), consumer, -1);
    queue.ack(resultM1Three.getEntryPointer(), consumers[1], readPointer);
    queue.finalize(resultM1Three.getEntryPointer(), consumer, -1);

    // both still see empty
    assertTrue(queue.dequeue(consumers[0], readPointer).isEmpty());
    assertTrue(queue.dequeue(consumers[1], readPointer).isEmpty());
  }

  @Test
  public void testSingleConsumerSingleGroup_dynamicReconfig() throws Exception {
    //Todo: Please update test case regarding queue configuration
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = getCleanPointer(version);

    // enqueue four entries
    int n=4;
    EnqueueResult [] results = new EnqueueResult[n];
    for (int i=0;i<n;i++) {
      results[i] = queue.enqueue(Bytes.toBytes(i+1), null, version);
      assertTrue(results[i].isSuccess());
    }

    // single consumer in this test, switch between single and multi mode
    QueueConfig multiConfig = new QueueConfig(PartitionerType.FIFO, false);
    QueueConfig singleConfig = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, singleConfig);

    // use single config first

    // dequeue and ack the first entry, value = 1
    DequeueResult result = queue.dequeue(consumer, singleConfig, readPointer);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(1)));
    queue.ack(result.getEntryPointer(), consumer, readPointer);
    queue.finalize(result.getEntryPointer(), consumer, -1);

    // changing configuration to multi should work fine, should get next entry
    // value = 2
    result = queue.dequeue(consumer, multiConfig, readPointer);
    DequeueResult value2result = result;
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(2)));
    // but don't ack yet

    // changing configuration back to single should not work (pending entry)
    try {
      queue.dequeue(consumer, singleConfig, readPointer);
      fail("dequeue should fail because it changes to single mode.");
    } catch (OperationException e) {
      // expected
    }

    // back to multi should work and give entry value = 3, ack it
    result = queue.dequeue(consumer, multiConfig, readPointer);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(3)));
    queue.ack(result.getEntryPointer(), consumer, readPointer);

    // changing configuration back to single should still not work
    try {
      queue.dequeue(consumer, singleConfig, readPointer);
      fail("dequeue should fail because it changes to single mode.");
    } catch (OperationException e) {
      // expected
    }

    // back to multi should work and give entry value = 4, ack it
    result = queue.dequeue(consumer, multiConfig, readPointer);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(4)));
    queue.ack(result.getEntryPointer(), consumer, readPointer);

    // we still have value=2 pending but dequeue will return empty in multiEntry
    result = queue.dequeue(consumer, multiConfig, readPointer);
    assertTrue(result.isEmpty());

    // but we can't change config because value=2 still pending
    try {
      queue.dequeue(consumer, singleConfig, readPointer);
      fail("dequeue should fail because it changes to single mode.");
    } catch (OperationException e) {
      // expected
    }

    // now ack value=2
    queue.ack(value2result.getEntryPointer(), consumer, readPointer);

    // nothing pending and empty, can change config
    result = queue.dequeue(consumer, singleConfig, readPointer);
    assertTrue(result.isEmpty());

    // enqueue twice, dequeue/ack once
    queue.enqueue(Bytes.toBytes(5), null, version);
    queue.enqueue(Bytes.toBytes(6), null, version);
    result = queue.dequeue(consumer, singleConfig, readPointer);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(5)));
    queue.ack(result.getEntryPointer(), consumer, readPointer);
    queue.finalize(result.getEntryPointer(), consumer, -1);

    // change config and dequeue
    result = queue.dequeue(consumer, multiConfig, readPointer);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(6)));
    queue.ack(result.getEntryPointer(), consumer, readPointer);
    queue.finalize(result.getEntryPointer(), consumer, -1);

    // nothing pending and empty, can change config
    result = queue.dequeue(consumer, singleConfig, readPointer);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testMultiConsumerSingleGroup_dynamicReconfig() throws Exception {
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = getCleanPointer(version);

    // enqueue one hundred entries
    int n=100;
    EnqueueResult [] results = new EnqueueResult[n];
    for (int i=0;i<n;i++) {
      results[i] = queue.enqueue(Bytes.toBytes(i+1), null, version);
      assertTrue(results[i].isSuccess());
    }
    // we want to verify at the end of the test we acked every entry
    Set<Integer> acked = new TreeSet<Integer>();

    // use multi config
    PartitionerType hashPartitionerType = PartitionerType.HASH_ON_VALUE;
    QueueConfig config = new QueueConfig(hashPartitionerType, false);
    // two consumers with a hash partitioner, both single mode
    QueueConsumer consumer1 = new QueueConsumer(0, 0, 2, config);
    QueueConsumer consumer2 = new QueueConsumer(1, 0, 2, config);

    // dequeue all entries for consumer 1 but only ack until the first hole
    boolean ack = true;
    int last = -1;
    Map<Integer,QueueEntryPointer> consumer1unacked =
        new TreeMap<Integer,QueueEntryPointer>();
    while (true) {
      DequeueResult result = queue.dequeue(consumer1, readPointer);
      if (result.isEmpty()) break;
      assertTrue(result.isSuccess());
      int value = Bytes.toInt(result.getValue());
      System.out.println("Consumer 1 dequeued value = "+ value);
      if (last > 0 && value != last + 1) ack = false;
      if (ack) {
        queue.ack(result.getEntryPointer(), consumer1, readPointer);
        queue.finalize(result.getEntryPointer(), consumer1, -1);
        assertTrue(acked.add(value));
        System.out.println("Consumer 1 acked value = "+ value);
        last = value;
      } else {
        consumer1unacked.put(value, result.getEntryPointer());
      }
    }

    // everything for consumer 1 is dequeued but not acked, there is a gap

    // we should not be able to reconfigure (try to introduce consumer 3)
    QueueConsumer consumer3 = new QueueConsumer(2, 0, 3, config);
    try {
      queue.dequeue(consumer3, readPointer);
      fail("dequeue should fail because it changes group size.");
    } catch (OperationException e) {
      // expected
    }

    // iterate back over unacked consumer 1 entries and ack everything
    for (Map.Entry<Integer,QueueEntryPointer> entry:
      consumer1unacked.entrySet()) {
      queue.ack(entry.getValue(), consumer1, readPointer);
      queue.finalize(entry.getValue(), consumer1, -1);
      assertTrue(acked.add(entry.getKey()));
      System.out.println("Consumer 1 acked value = "+ entry.getKey());
    }

    // now we can reconfigure to 3 consumers
    DequeueResult result = queue.dequeue(consumer3, readPointer);

    // dequeue/ack all entries with consumer 3
    while (true) {
      if (result.isEmpty()) break;
      assertTrue(result.isSuccess());
      int value = Bytes.toInt(result.getValue());
      queue.ack(result.getEntryPointer(), consumer3, readPointer);
      queue.finalize(result.getEntryPointer(), consumer3, -1);
      assertTrue(acked.add(value));
      result = queue.dequeue(consumer3, readPointer);
    }

    // reconfigure again back to 2 and dequeue everything
    Map<Integer,QueueEntryPointer> consumer2unacked =
        new TreeMap<Integer,QueueEntryPointer>();
    while (true) {
      result = queue.dequeue(consumer2, readPointer);
      if (result.isEmpty()) break;
      assertTrue(result.isSuccess());
      consumer2unacked.put(Bytes.toInt(result.getValue()),
          result.getEntryPointer());
    }

    // should not be able to introduced consumer 4 (pending entries from 2)
    QueueConsumer consumer4 = new QueueConsumer(3, 0, 4, config);
    try {
      queue.dequeue(consumer4, readPointer);
      fail("dequeue should fail because it changes group size.");
    } catch (OperationException e) {
      // expected
    }

    // ack all entries with consumer 2
    for (Map.Entry<Integer,QueueEntryPointer> entry:
      consumer2unacked.entrySet()) {
      queue.ack(entry.getValue(), consumer2, readPointer);
      queue.finalize(entry.getValue(), consumer2, -1);
      assertTrue(acked.add(entry.getKey()));
      System.out.println("Consumer 2 acked value = "+ entry.getKey());
    }

    // now introduce consumer 4, should be valid but empty
    result = queue.dequeue(consumer4, readPointer);
    assertTrue(result.isEmpty());

    // size of set should be equal to number of entries
    assertEquals(n, acked.size());
  }

  @Test
  public void testSingleConsumerThreaded() throws Exception {
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = getCleanPointer(version);

    byte [] valueOne = Bytes.toBytes("value1");
    byte [] valueTwo = Bytes.toBytes("value2");


    // dequeue it with the single consumer and FIFO partitioner
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, new QueueConfig(PartitionerType.FIFO, true));

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
    assertTrue(queue.enqueue(valueOne, null, version).isSuccess());
    waitForAndAssertCount(1, dequeuer.dequeues);
    waitForAndAssertCount(2, dequeuer.dequeueRunLoop);

    // dequeuer will dequeue, but we should still be able to dequeue!
    result = queue.dequeue(consumer, readPointer);
    assertNotNull(result);
    assertTrue(Bytes.equals(result.getValue(), valueOne));

    // dequeuer should also have this loaded
    result = dequeuer.blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
    assertNotNull(result);
    assertTrue(Bytes.equals(result.getValue(), valueOne));

    // ack it!
    queue.ack(result.getEntryPointer(), consumer, readPointer);
    queue.finalize(result.getEntryPointer(), consumer, -1);

    // trigger another dequeue
    assertTrue(dequeuer.triggerdequeue());
    waitForAndAssertCount(1, dequeuer.dequeues);
    // dequeuer had dequeueped and goes into its next loop
    waitForAndAssertCount(2, dequeuer.dequeueRunLoop);

    // nothing in queue so dequeuer should be empty
    result = dequeuer.blockdequeue(DEQUEUE_BLOCK_TIMEOUT_MS);
    assertNull(result);

    // enqueue
    assertTrue(queue.enqueue(valueTwo, null, version).isSuccess());
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
    queue.ack(result.getEntryPointer(), consumer, readPointer);
    queue.finalize(result.getEntryPointer(), consumer, -1);

    // verify queue is empty
    assertTrue(queue.dequeue(consumer, readPointer).isEmpty());

    // shut down
    dequeuer.shutdown();
  }

  @Test
  public void testConcurrentEnqueueDequeue() throws Exception {
    final TTQueue queue = createQueue();
    final long version = timeOracle.getTimestamp();
    final ReadPointer readPointer = getCleanPointer(version);
    AtomicLong dequeueReturns = null;
    if (queue instanceof TTQueueOnVCTable) {
      dequeueReturns = ((TTQueueOnVCTable)queue).dequeueReturns;
    } else if (queue instanceof TTQueueOnHBaseNative) {
      dequeueReturns = ((TTQueueOnHBaseNative)queue).dequeueReturns;
    } else if (queue instanceof TTQueueNewOnVCTable) {
      dequeueReturns = ((TTQueueNewOnVCTable)queue).dequeueReturns;
    }

    assertNotNull(dequeueReturns);

    final int n = getNumIterations();

    // Create and start a thread that dequeues in a loop
    final QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    final QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);
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
            dequeued.add(result.getValue());
            try {
              queue.ack(result.getEntryPointer(), consumer, readPointer);
              queue.finalize(result.getEntryPointer(), consumer, -1);
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
        for (int i=0; i<n; i++) {
          try {
            queue.enqueue(Bytes.toBytes(i), null, version);
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


  // TODO revive this test when hash partitioning is working.
  // TODO This used to use long_mod, but I deleted that partitioner
  // TODO This does not work with round/robin nor fifo - can't guarantee that entry#i has value i
  @Test @Ignore
  public void testMultiConsumerMultiGroup() throws Exception {
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = getCleanPointer(version);
    AtomicLong dequeueReturns = null;
    if (queue instanceof TTQueueOnVCTable) {
      dequeueReturns = ((TTQueueOnVCTable)queue).dequeueReturns;
    } else if (queue instanceof TTQueueOnHBaseNative) {
      dequeueReturns = ((TTQueueOnHBaseNative)queue).dequeueReturns;
    } else if (queue instanceof TTQueueNewOnVCTable) {
      dequeueReturns = ((TTQueueNewOnVCTable)queue).dequeueReturns;
    }

    assertNotNull(dequeueReturns);

    // Create 4 consumer groups with 4 consumers each
    int n = 4;
    QueueConsumer [][] consumers = new QueueConsumer[n][];
    for (int i=0; i<n; i++) {
      consumers[i] = new QueueConsumer[n];
      for (int j=0; j<n; j++) {
        consumers[i][j] = new QueueConsumer(j, i, n, new QueueConfig(PartitionerType.ROUND_ROBIN, true));
      }
    }

    // values are longs
    byte [][] values = new byte[n*n][];
    for (int i=0; i<n*n; i++) {
      values[i] = Bytes.toBytes((long)i);
    }


    // Start a dequeuer for every consumer!
    QueueDequeuer [][] dequeuers = new QueueDequeuer[n][];
    for (int i=0; i<n; i++) {
      dequeuers[i] = new QueueDequeuer[n];
      for (int j=0; j<n; j++) {
        dequeuers[i][j] = new QueueDequeuer(queue, consumers[i][j], readPointer);
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

    @SuppressWarnings("unused")
    long numEnqueues = 0;
    // enqueue the first four values
    for (int i=0; i<n; i++) {
      assertTrue(queue.enqueue(values[i], null, version).isSuccess());
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
        DequeueResult result = queue.dequeue(consumers[i][j], readPointer);
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
        DequeueResult result = queue.dequeue(consumers[i][j], readPointer);
        assertNotNull(result);
        assertEquals(j, Bytes.toLong(result.getValue()));
        queue.ack(result.getEntryPointer(), consumers[i][j], readPointer);
        queue.finalize(result.getEntryPointer(), consumers[i][j], -1);
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
          DequeueResult result = queue.dequeue(consumers[i][j], readPointer);
          assertNotNull(result);
          assertEquals(j, Bytes.toLong(result.getValue()));
          queue.ack(result.getEntryPointer(), consumers[i][j], readPointer);
          queue.finalize(result.getEntryPointer(), consumers[i][j], -1);
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
      assertTrue(queue.enqueue(values[i], null, version).isSuccess());
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
          queue.ack(result.getEntryPointer(), consumers[i][j], readPointer);
          queue.finalize(result.getEntryPointer(), consumers[i][j], -1);
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
}
