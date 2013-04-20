package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.ttqueue.TTQueueNewOnVCTable.DequeueEntry;
import com.continuuity.data.operation.ttqueue.TTQueueNewOnVCTable.DequeuedEntrySet;
import com.continuuity.data.operation.ttqueue.TTQueueNewOnVCTable.TransientWorkingSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public abstract class TestTTQueueNew extends TestTTQueue {

  private static final int MAX_CRASH_DEQUEUE_TRIES = 10;

  protected void updateCConfiguration(CConfiguration conf) {
    conf.setLong(TTQueueNewOnVCTable.TTQUEUE_EVICT_INTERVAL_SECS, 0); // Setting evict interval to be zero seconds for testing, so that evictions can be asserted immediately in tests.
    conf.setInt(TTQueueNewOnVCTable.TTQUEUE_MAX_CRASH_DEQUEUE_TRIES, MAX_CRASH_DEQUEUE_TRIES);
  }

  // Test DequeueEntry
  @Test
  public void testDequeueEntryEncode() throws Exception {
    DequeueEntry expectedEntry = new DequeueEntry(1, 2);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    expectedEntry.encode(new BinaryEncoder(bos));
    byte[] encodedValue = bos.toByteArray();

    DequeueEntry actualEntry = DequeueEntry.decode(new BinaryDecoder(new ByteArrayInputStream(encodedValue)));
    assertEquals(expectedEntry.getEntryId(), actualEntry.getEntryId());
    assertEquals(expectedEntry.getTries(), actualEntry.getTries());
    assertEquals(expectedEntry, actualEntry);
  }

  @Test
  public void testDequeueEntryEquals() throws Exception {
    // DequeueEntry is only compared using entryId, tries is ignored
    assertEquals(new DequeueEntry(12, 10), new DequeueEntry(12));
    assertEquals(new DequeueEntry(12, 10), new DequeueEntry(12, 10));
    assertEquals(new DequeueEntry(12, 10), new DequeueEntry(12, 15));

    assertNotEquals(new DequeueEntry(12, 10), new DequeueEntry(13, 10));
    assertNotEquals(new DequeueEntry(12, 10), new DequeueEntry(13, 11));
  }

  @Test
  public void testDequeueEntryCompare() throws Exception {
    // DequeueEntry is compared only on entryId, tries is ignored
    SortedSet<DequeueEntry> sortedSet = new TreeSet<DequeueEntry>();
    sortedSet.add(new DequeueEntry(5, 1));
    sortedSet.add(new DequeueEntry(2, 3));
    sortedSet.add(new DequeueEntry(1, 2));
    sortedSet.add(new DequeueEntry(3, 2));
    sortedSet.add(new DequeueEntry(4, 3));
    sortedSet.add(new DequeueEntry(4, 5));
    sortedSet.add(new DequeueEntry(0, 2));

    int expected = 0;
    for(Iterator<DequeueEntry> iterator = sortedSet.iterator(); iterator.hasNext(); ) {
      assertEquals(expected++, iterator.next().getEntryId());
    }
  }

  @Test
  public void testQueueEntrySetEncode() throws Exception {
    final int MAX = 10;
    DequeuedEntrySet expectedEntrySet = new DequeuedEntrySet();
    for(int i = 0; i < MAX; ++i) {
      expectedEntrySet.add(new DequeueEntry(i, i % 2));
    }

    assertEquals(MAX, expectedEntrySet.size());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    expectedEntrySet.encode(new BinaryEncoder(bos));
    byte[] encodedValue = bos.toByteArray();

    DequeuedEntrySet actualEntrySet = DequeuedEntrySet.decode(new BinaryDecoder(new ByteArrayInputStream(encodedValue)));
    assertEquals(expectedEntrySet.size(), actualEntrySet.size());
    for(int i = 0; i < MAX; ++i) {
      DequeueEntry expectedEntry = expectedEntrySet.min();
      expectedEntrySet.remove(expectedEntry.getEntryId());

      DequeueEntry actualEntry = actualEntrySet.min();
      actualEntrySet.remove(actualEntry.getEntryId());

      assertEquals(expectedEntry.getEntryId(), actualEntry.getEntryId());
      assertEquals(expectedEntry.getTries(), actualEntry.getTries());
    }
  }

  @Test
  public void testQueueEntrySet() throws Exception {
    final int MAX = 10;
    DequeuedEntrySet entrySet = new DequeuedEntrySet();
    List<Long> expectedEntryIds = Lists.newArrayListWithCapacity(MAX);
    List<DequeueEntry> expectedEntryList = Lists.newArrayListWithCapacity(MAX);
    Set<Long> expectedDroppedEntries = Sets.newHashSetWithExpectedSize(MAX);

    for(int i = 0; i < MAX; ++i) {
      entrySet.add(new DequeueEntry(i, i %2));
      expectedEntryIds.add((long) i);
      expectedEntryList.add(new DequeueEntry(i, i % 2));

      if(i % 2 == 1) {
        expectedDroppedEntries.add((long) i);
      }
    }

    // Verify created lists
    assertEquals(MAX, entrySet.size());
    assertEquals(MAX, expectedEntryIds.size());
    assertEquals(MAX, expectedEntryList.size());
    assertEquals(MAX/2, expectedDroppedEntries.size());

    // Verify QueueEntrySet
    assertEquals(expectedEntryIds, entrySet.getEntryIds());
    assertEquals(expectedEntryList, entrySet.getEntryList());

    Set<Long> actualDroppedEntries = entrySet.startNewTry(1);
    assertEquals(expectedDroppedEntries, actualDroppedEntries);

    List<Long> actualRemainingEntries = Lists.newArrayListWithCapacity(MAX);
    for(int i = 0; i < MAX; ++i) {
      if(i % 2 == 0) {
        actualRemainingEntries.add((long) i);
      }
    }
    assertEquals(actualRemainingEntries, entrySet.getEntryIds());
  }

  @Test
  public void testWorkingEntryList() {
    final int MAX = 10;
    TTQueueNewOnVCTable.TransientWorkingSet transientWorkingSet = new TransientWorkingSet(Lists.<Long>newArrayList(), Collections.EMPTY_MAP);
    assertFalse(transientWorkingSet.hasNext());

    List<Long> workingSet = Lists.newArrayList();
    Map<Long, byte[]> cache = Maps.newHashMap();
    for(long i = 0; i < MAX; ++i) {
      workingSet.add(i);
      cache.put(i, Bytes.toBytes(i));
    }
    transientWorkingSet = new TransientWorkingSet(workingSet, cache);

    for(int i = 0; i < MAX; ++i) {
      assertTrue(transientWorkingSet.hasNext());
      assertEquals(new DequeueEntry(i), transientWorkingSet.peekNext());
      assertEquals(new DequeueEntry(i), transientWorkingSet.next());
    }
    assertFalse(transientWorkingSet.hasNext());
  }

/*  @Override
  @Test
  public void testEvictOnAck_ThreeGroups() throws Exception {
    // Note: for now only consumer with consumerId 0 and groupId 0 can run the evict.
    TTQueue queue = createQueue();
    final boolean singleEntry = true;
    long dirtyVersion = getDirtyWriteVersion();
    ReadPointer dirtyReadPointer = getDirtyPointer();

    QueueConfig config = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, singleEntry);
    QueueConsumer consumer1 = new QueueConsumer(0, 0, 1, config);
    QueueConsumer consumer2 = new QueueConsumer(0, 1, 1, config);
    QueueConsumer consumer3 = new QueueConsumer(0, 2, 1, config);

    // enable evict-on-ack for 3 groups
    int numGroups = 3;

    // enqueue 10 things
    for (int i=0; i<10; i++) {
      queue.enqueue(new QueueEntry(Bytes.toBytes(i)), dirtyVersion);
    }

    // Create consumers for checking if eviction happened
    QueueConsumer consumerCheck9thPos = new QueueConsumer(0, 3, 1, config);
    QueueConsumer consumerCheck10thPos = new QueueConsumer(0, 4, 1, config);
    // Move the consumers to 9th and 10 pos
    for(int i = 0; i < 8; i++) {
      DequeueResult result = queue.dequeue(consumerCheck9thPos, dirtyReadPointer);
      assertEquals(i, Bytes.toInt(result.getEntry().getData()));
      queue.ack(result.getEntryPointer(), consumerCheck9thPos, dirtyReadPointer);
      queue.finalize(result.getEntryPointer(), consumerCheck9thPos, -1, dirtyReadPointer.getMaximum()); // No evict
    }
    for(int i = 0; i < 9; i++) {
      DequeueResult result = queue.dequeue(consumerCheck10thPos, dirtyReadPointer);
      assertEquals(i, Bytes.toInt(result.getEntry().getData()));
      queue.ack(result.getEntryPointer(), consumerCheck10thPos, dirtyReadPointer);
      queue.finalize(result.getEntryPointer(), consumerCheck10thPos, -1, dirtyReadPointer.getMaximum()); // No evict
    }


    // dequeue/ack/finalize 8 things w/ group1 and numGroups=3
    for (int i=0; i<8; i++) {
      DequeueResult result =
        queue.dequeue(consumer1, dirtyReadPointer);
      assertEquals(i, Bytes.toInt(result.getEntry().getData()));
      queue.ack(result.getEntryPointer(), consumer1, dirtyReadPointer);
      queue.finalize(result.getEntryPointer(), consumer1, numGroups, dirtyReadPointer.getMaximum());
    }

    // dequeue is not empty, as 9th and 10th entries is still available
    assertFalse(
      queue.dequeue(consumer1, dirtyReadPointer).isEmpty());

    // dequeue with consumer2 still has entries (expected)
    assertFalse(
      queue.dequeue(consumer2, dirtyReadPointer).isEmpty());

    // dequeue everything with consumer2
    for (int i=0; i<10; i++) {
      DequeueResult result =
        queue.dequeue(consumer2, dirtyReadPointer);
      assertEquals(i, Bytes.toInt(result.getEntry().getData()));
      queue.ack(result.getEntryPointer(), consumer2, dirtyReadPointer);
      queue.finalize(result.getEntryPointer(), consumer2, numGroups, dirtyReadPointer.getMaximum());
    }

    // dequeue is empty
    assertTrue(
      queue.dequeue(consumer2, dirtyReadPointer).isEmpty());

    // dequeue with consumer3 still has entries (expected)
    assertFalse(
      queue.dequeue(consumer3, dirtyReadPointer).isEmpty());

    // dequeue everything consumer3
    for (int i=0; i<10; i++) {
      DequeueResult result =
        queue.dequeue(consumer3, dirtyReadPointer);
      assertEquals(i, Bytes.toInt(result.getEntry().getData()));
      queue.ack(result.getEntryPointer(), consumer3, dirtyReadPointer);
      queue.finalize(result.getEntryPointer(), consumer3, numGroups, dirtyReadPointer.getMaximum());
    }

    // dequeue with consumer3 is empty
    assertTrue(
      queue.dequeue(consumer3, dirtyReadPointer).isEmpty());

    // Verify 9th and 10th entries are still present
    DequeueResult result = queue.dequeue(consumerCheck9thPos, dirtyReadPointer);
    assertEquals(8, Bytes.toInt(result.getEntry().getData()));
    result = queue.dequeue(consumerCheck10thPos, dirtyReadPointer);
    assertEquals(9, Bytes.toInt(result.getEntry().getData()));

    // dequeue with consumer 1, should get 8 (9th entry)
    result = queue.dequeue(consumer1, dirtyReadPointer);
    assertEquals(8, Bytes.toInt(result.getEntry().getData()));
    queue.ack(result.getEntryPointer(), consumer1, dirtyReadPointer);
    queue.finalize(result.getEntryPointer(), consumer1, numGroups, dirtyReadPointer.getMaximum());

    // now the first 9 entries should have been physically evicted!
    // since the 9th entry does not exist anymore, exception will be thrown
    try {
    result = queue.dequeue(consumerCheck9thPos, dirtyReadPointer);
    fail("Dequeue should fail");
    } catch (OperationException e) {
      assertEquals(StatusCode.INTERNAL_ERROR, e.getStatus());
      result = null;
    }
    assertNull(result);
    result = queue.dequeue(consumerCheck10thPos, dirtyReadPointer);
    assertEquals(9, Bytes.toInt(result.getEntry().getData()));

    // dequeue with consumer 1, should get 9 (10th entry)
    result = queue.dequeue(consumer1, dirtyReadPointer);
    assertEquals(9, Bytes.toInt(result.getEntry().getData()));
    queue.ack(result.getEntryPointer(), consumer1, dirtyReadPointer);
    queue.finalize(result.getEntryPointer(), consumer1, numGroups, dirtyReadPointer.getMaximum());

    // Consumer 1 should be empty now
    assertTrue(queue.dequeue(consumer1, dirtyReadPointer).isEmpty());

    // Now 10th entry should be evicted too!
    try {
    result = queue.dequeue(consumerCheck10thPos, dirtyReadPointer);
    fail("Dequeue should fail");
    } catch (OperationException e) {
      assertEquals(StatusCode.INTERNAL_ERROR, e.getStatus());
      result = null;
    }
    assertNull(result);
  } */

  @Test
  public void testSingleConsumerWithHashPartitioning() throws Exception {
    final String HASH_KEY = "hashKey";
    final boolean singleEntry = true;
    final int numQueueEntries = 88;
    final int numConsumers = 4;
    final int consumerGroupId = 0;
    TTQueue queue = createQueue();
    long dirtyVersion = getDirtyWriteVersion();

    // enqueue some entries
    for (int i = 0; i < numQueueEntries; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes("value" + i % numConsumers));
      queueEntry.addPartitioningKey(HASH_KEY, i);
      assertTrue(queue.enqueue(queueEntry, dirtyVersion).isSuccess());
    }
    // dequeue it with HASH partitioner
    QueueConfig config = new QueueConfig(QueuePartitioner.PartitionerType.HASH, singleEntry);

    QueueConsumer[] consumers = new QueueConsumer[numConsumers];
    for (int i = 0; i < numConsumers; i++) {
      consumers[i] = new QueueConsumer(i, consumerGroupId, numConsumers, "group1", HASH_KEY, config);
    }

    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries);

    // enqueue some more entries
    for (int i = numQueueEntries; i < numQueueEntries * 2; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes("value" + i % numConsumers));
      queueEntry.addPartitioningKey(HASH_KEY, i);
      assertTrue(queue.enqueue(queueEntry, dirtyVersion).isSuccess());
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
    long dirtyVersion = getDirtyWriteVersion();

    // enqueue some entries
    for (int i = 0; i < numQueueEntries; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes("value" + (i + 1) % numConsumers));
      assertTrue(queue.enqueue(queueEntry, dirtyVersion).isSuccess());
    }

    // dequeue it with ROUND_ROBIN partitioner
    QueueConfig config = new QueueConfig(QueuePartitioner.PartitionerType.ROUND_ROBIN, singleEntry);

    QueueConsumer[] consumers = new QueueConsumer[numConsumers];
    for (int i = 0; i < numConsumers; i++) {
      consumers[i] = new QueueConsumer(i, consumerGroupId, numConsumers, "group1", config);
    }

    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries);

    // enqueue some more entries
    for (int i = numQueueEntries; i < numQueueEntries * 2; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes("value" + (i + 1) % numConsumers));
      assertTrue(queue.enqueue(queueEntry, dirtyVersion).isSuccess());
    }

    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries);
  }

  private void dequeuePartitionedEntries(TTQueue queue, QueueConsumer[] consumers, int numConsumers, int totalEnqueues) throws Exception {
    ReadPointer dirtyReadPointer = getDirtyPointer();
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
        queue.ack(result.getEntryPointer(), consumers[i], dirtyReadPointer);
        queue.finalize(result.getEntryPointer(), consumers[i], -1, dirtyReadPointer.getMaximum());

        // dequeue, should get second value
        result = queue.dequeue(consumers[i], dirtyReadPointer);
        assertTrue("Consumer:" + i + " Iteration:" + j, result.isSuccess());
        assertEquals("value" + i, Bytes.toString(result.getEntry().getData()));

        // ack
        queue.ack(result.getEntryPointer(), consumers[i], dirtyReadPointer);
        queue.finalize(result.getEntryPointer(), consumers[i], -1, dirtyReadPointer.getMaximum());
      }

      // verify queue is empty
      DequeueResult result = queue.dequeue(consumers[i], dirtyReadPointer);
      assertTrue(result.isEmpty());
    }
  }

  @Test
  public void testSingleStatefulConsumerWithHashPartitioning() throws Exception {
    final String HASH_KEY = "hashKey";
    final boolean singleEntry = true;
    final int numQueueEntries = 264; // Choose a number that leaves a reminder when divided by batchSize, and be big enough so that it forms a few batches
    final int numConsumers = 4;
    final int consumerGroupId = 0;
    TTQueue queue = createQueue();
    long dirtyVersion = getDirtyWriteVersion();

    // enqueue some entries
    for (int i = 0; i < numQueueEntries; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes(i));
      queueEntry.addPartitioningKey(HASH_KEY, i);
      assertTrue(queue.enqueue(queueEntry, dirtyVersion).isSuccess());
    }
    // dequeue it with HASH partitioner
    // TODO: test with more batch sizes
    QueueConfig config = new QueueConfig(QueuePartitioner.PartitionerType.HASH, singleEntry, 29);

    StatefulQueueConsumer[] consumers = new StatefulQueueConsumer[numConsumers];
    for (int i = 0; i < numConsumers; i++) {
      consumers[i] = new StatefulQueueConsumer(i, consumerGroupId, numConsumers, "group1", HASH_KEY, config);
    }

    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries, 0, QueuePartitioner.PartitionerType.HASH);
    System.out.println("Round 1 dequeue done");

    // enqueue some more entries
    for (int i = numQueueEntries; i < numQueueEntries * 2; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes(i));
      queueEntry.addPartitioningKey(HASH_KEY, i);
      assertTrue(queue.enqueue(queueEntry, dirtyVersion).isSuccess());
    }
    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries, numQueueEntries, QueuePartitioner.PartitionerType.HASH);
    System.out.println("Round 2 dequeue done");
  }

  @Test
  public void testSingleStatefulConsumerWithRoundRobinPartitioning() throws Exception {
    final boolean singleEntry = true;
    final int numQueueEntries = 264; // Choose a number that doesn't leave a reminder when divided by batchSize, and be big enough so that it forms a few batches
    final int numConsumers = 4;
    final int consumerGroupId = 0;
    TTQueue queue = createQueue();
    long dirtyVersion = getDirtyWriteVersion();

    // enqueue some entries
    for (int i = 0; i < numQueueEntries; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes(i + 1));
      assertTrue(queue.enqueue(queueEntry, dirtyVersion).isSuccess());
    }

    // dequeue it with ROUND_ROBIN partitioner
    // TODO: test with more batch sizes
    QueueConfig config = new QueueConfig(QueuePartitioner.PartitionerType.ROUND_ROBIN, singleEntry, 11);

    StatefulQueueConsumer[] consumers = new StatefulQueueConsumer[numConsumers];
    for (int i = 0; i < numConsumers; i++) {
      consumers[i] = new StatefulQueueConsumer(i, consumerGroupId, numConsumers, "group1", config);
    }

    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries, 0, QueuePartitioner.PartitionerType.ROUND_ROBIN);

    // enqueue some more entries
    for (int i = numQueueEntries; i < numQueueEntries * 2; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes(i + 1));
      assertTrue(queue.enqueue(queueEntry, dirtyVersion).isSuccess());
    }

    // dequeue and verify
    dequeuePartitionedEntries(queue, consumers, numConsumers, numQueueEntries, numQueueEntries, QueuePartitioner.PartitionerType.ROUND_ROBIN);
  }

  private void dequeuePartitionedEntries(TTQueue queue, StatefulQueueConsumer[] consumers, int numConsumers,
                                         int numQueueEntries, int startQueueEntry, QueuePartitioner.PartitionerType partitionerType) throws Exception {
    ReadPointer dirtyReadPointer = getDirtyPointer();
    for (int consumer = 0; consumer < numConsumers; consumer++) {
      for (int entry = 0; entry < numQueueEntries / (2 * numConsumers); entry++) {
        DequeueResult result = queue.dequeue(consumers[consumer], dirtyReadPointer);
        // verify we got something and it's the first value
        assertTrue(result.toString(), result.isSuccess());
        int expectedValue = startQueueEntry + consumer + (2 * entry * numConsumers);
        if(partitionerType == QueuePartitioner.PartitionerType.ROUND_ROBIN) {
          if(consumer == 0) {
            // Adjust the expected value for consumer 0
            expectedValue += numConsumers;
          }
        }
        System.out.println(String.format("Consumer-%d entryid=%d value=%s expectedValue=%s",
                  consumer, result.getEntryPointer().getEntryId(), Bytes.toInt(result.getEntry().getData()), expectedValue));
        assertEquals(expectedValue, Bytes.toInt(result.getEntry().getData()));
        // dequeue again without acking, should still get first value
        result = queue.dequeue(consumers[consumer], dirtyReadPointer);
        assertTrue(result.isSuccess());
        assertEquals(expectedValue, Bytes.toInt(result.getEntry().getData()));

        // ack
        queue.ack(result.getEntryPointer(), consumers[consumer], dirtyReadPointer);
        queue.finalize(result.getEntryPointer(), consumers[consumer], -1, dirtyReadPointer.getMaximum());

        // dequeue, should get second value
        result = queue.dequeue(consumers[consumer], dirtyReadPointer);
        assertTrue("Consumer:" + consumer + " Entry:" + entry, result.isSuccess());
        expectedValue += numConsumers;
        System.out.println(String.format("Consumer-%d entryid=%d value=%s expectedValue=%s",
                  consumer, result.getEntryPointer().getEntryId(), Bytes.toInt(result.getEntry().getData()), expectedValue));
        assertEquals(expectedValue, Bytes.toInt(result.getEntry().getData()));

        // ack
        queue.ack(result.getEntryPointer(), consumers[consumer], dirtyReadPointer);
        queue.finalize(result.getEntryPointer(), consumers[consumer], -1, dirtyReadPointer.getMaximum());
      }

      // verify queue is empty
      DequeueResult result = queue.dequeue(consumers[consumer], dirtyReadPointer);
      assertTrue(result.isEmpty());
    }
  }

  @Test
  public void testSingleStatefulConsumerWithFifo() throws Exception {
    final boolean singleEntry = true;
    final int numQueueEntries = 200;  // Make sure numQueueEntries % batchSize == 0 && numQueueEntries % numConsumers == 0
    final int numConsumers = 4;
    final int consumerGroupId = 0;
    TTQueue queue = createQueue();
    long dirtyVersion = getDirtyWriteVersion();

    // enqueue some entries
    for (int i = 0; i < numQueueEntries; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes(i));
      assertTrue(queue.enqueue(queueEntry, dirtyVersion).isSuccess());
    }

    // dequeue it with ROUND_ROBIN partitioner
    // TODO: test with more batch sizes
    QueueConfig config = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, singleEntry, 10);

    StatefulQueueConsumer[] statefulQueueConsumers = new StatefulQueueConsumer[numConsumers];
    QueueConsumer[] queueConsumers = new QueueConsumer[numConsumers];
    for (int i = 0; i < numConsumers; i++) {
      statefulQueueConsumers[i] = new StatefulQueueConsumer(i, consumerGroupId, numConsumers, "group1", config);
      queueConsumers[i] = new QueueConsumer(i, consumerGroupId, numConsumers, "group1", config);
    }

    // dequeue and verify
    dequeueFifoEntries(queue, statefulQueueConsumers, numConsumers, numQueueEntries, 0);

    // enqueue some more entries
    for (int i = numQueueEntries; i < 2 * numQueueEntries; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes(i));
      assertTrue(queue.enqueue(queueEntry, dirtyVersion).isSuccess());
    }

    // dequeue and verify
    dequeueFifoEntries(queue, statefulQueueConsumers, numConsumers, numQueueEntries, numQueueEntries);

    // Run with stateless QueueConsumer
    for (int i = 2 * numQueueEntries; i < 3 * numQueueEntries; i++) {
      QueueEntry queueEntry = new QueueEntry(Bytes.toBytes(i));
      assertTrue(queue.enqueue(queueEntry, dirtyVersion).isSuccess());
    }

    // dequeue and verify
    dequeueFifoEntries(queue, queueConsumers, numConsumers, numQueueEntries, 2 * numQueueEntries);

  }

  private void dequeueFifoEntries(TTQueue queue, QueueConsumer[] consumers, int numConsumers,
                                  int numQueueEntries, int startQueueEntry) throws Exception {
    ReadPointer dirtyReadPointer = getDirtyPointer();
    int expectedValue = startQueueEntry;
    for (int consumer = 0; consumer < numConsumers; consumer++) {
      for (int entry = 0; entry < numQueueEntries / (2 * numConsumers); entry++, ++expectedValue) {
        DequeueResult result = queue.dequeue(consumers[consumer], dirtyReadPointer);
        // verify we got something and it's the first value
        assertTrue(String.format("Consumer=%d, entry=%d, %s", consumer, entry, result.toString()), result.isSuccess());
//        System.out.println(String.format("Consumer-%d entryid=%d value=%s expectedValue=%s",
//                  consumer, result.getEntryPointer().getEntryId(), Bytes.toInt(result.getEntry().getData()), expectedValue));
        assertEquals(expectedValue, Bytes.toInt(result.getEntry().getData()));
        // dequeue again without acking, should still get first value
        result = queue.dequeue(consumers[consumer], dirtyReadPointer);
        assertTrue(result.isSuccess());
        assertEquals(expectedValue, Bytes.toInt(result.getEntry().getData()));

        // ack
        queue.ack(result.getEntryPointer(), consumers[consumer], dirtyReadPointer);
        queue.finalize(result.getEntryPointer(), consumers[consumer], -1, dirtyReadPointer.getMaximum());

        // dequeue, should get second value
        result = queue.dequeue(consumers[consumer], dirtyReadPointer);
        assertTrue(result.isSuccess());
        ++expectedValue;
//        System.out.println(String.format("Consumer-%d entryid=%d value=%s expectedValue=%s",
//                  consumer, result.getEntryPointer().getEntryId(), Bytes.toInt(result.getEntry().getData()), expectedValue));
        assertEquals(expectedValue, Bytes.toInt(result.getEntry().getData()));

        // ack
        queue.ack(result.getEntryPointer(), consumers[consumer], dirtyReadPointer);
        queue.finalize(result.getEntryPointer(), consumers[consumer], -1, dirtyReadPointer.getMaximum());
      }
    }

    // verify queue is empty for all consumers
    for(int consumer = 0; consumer < numConsumers; ++consumer) {
      DequeueResult result = queue.dequeue(consumers[consumer], dirtyReadPointer);
      assertTrue(result.isEmpty());
    }
  }

  @Test
  public void testMaxCrashDequeueTries() throws Exception {
    TTQueue queue = createQueue();

    assertTrue(queue.enqueue(new QueueEntry(Bytes.toBytes(1)), getDirtyWriteVersion()).isSuccess());
    assertTrue(queue.enqueue(new QueueEntry(Bytes.toBytes(2)), getDirtyWriteVersion()).isSuccess());

    // dequeue it with FIFO partitioner, single entry mode
    QueueConfig config = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true);

    for(int tries = 0; tries <= MAX_CRASH_DEQUEUE_TRIES; ++tries) {
      // Simulate consumer crashing by sending in empty state every time and not acking the entry
      DequeueResult result = queue.dequeue(new StatefulQueueConsumer(0, 0, 1, "", config), getDirtyPointer());
      assertTrue(result.isSuccess());
      assertEquals(1, Bytes.toInt(result.getEntry().getData()));
    }

    // After max tries, the entry will be ignored
    StatefulQueueConsumer statefulQueueConsumer = new StatefulQueueConsumer(0, 0, 1, "", config);
    for(int tries = 0; tries <= MAX_CRASH_DEQUEUE_TRIES + 10; ++tries) {
      // No matter how many times a dequeue is repeated with state, the same entry needs to be returned
      DequeueResult result = queue.dequeue(statefulQueueConsumer, getDirtyPointer());
      assertTrue(result.isSuccess());
      assertEquals(2, Bytes.toInt(result.getEntry().getData()));
    }
    DequeueResult result = queue.dequeue(statefulQueueConsumer, getDirtyPointer());
    assertTrue(result.isSuccess());
    assertEquals(2, Bytes.toInt(result.getEntry().getData()));
    queue.ack(result.getEntryPointer(), statefulQueueConsumer, getDirtyPointer());

    result = queue.dequeue(statefulQueueConsumer, getDirtyPointer());
    assertTrue(result.isEmpty());
  }

  // Tests that do not work on NewTTQueue

  /**
   * Currently not working.  Will be fixed in ENG-???.
   */
  @Override
  @Test
  @Ignore
  public void testSingleConsumerSingleEntryWithInvalid_Empty_ChangeSizeAndToMulti() {
  }

  @Override
  @Test
  @Ignore
  public void testSingleConsumerMultiEntry_Empty_ChangeToSingleConsumerSingleEntry() {
  }

  @Override
  @Test
  @Ignore
  public void testSingleConsumerSingleGroup_dynamicReconfig() {
  }

  @Override
  @Test
  @Ignore
  public void testMultiConsumerSingleGroup_dynamicReconfig() {
  }

  @Override
  @Test
  @Ignore
  public void testMultipleConsumerMultiTimeouts() {
  }

  @Override @Test @Ignore
  public void testMultiConsumerMultiGroup() {}

  @Override
  @Test
  @Ignore
  public void testSingleConsumerAckSemantics() {
  }

  @Override
  @Test @Ignore
  public void testSingleConsumerWithHashValuePartitioning() throws Exception {
  }
}
