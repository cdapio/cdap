package com.continuuity.data.operation.executor.omid;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.Operation;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueuePartitioner.PartitionerType;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;
import com.continuuity.data.operation.ttqueue.TTQueueOnVCTable;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.operation.ttqueue.admin.GetGroupID;
import com.continuuity.data.operation.ttqueue.admin.GetQueueInfo;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigure;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.util.OperationUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class TestOmidExecutorLikeAFlow {

  private OmidTransactionalOperationExecutor executor;

  private OVCTableHandle handle;

  private static List<WriteOperation> batch(WriteOperation ... ops) {
    return Arrays.asList(ops);
  }

  static OperationContext context = OperationUtil.DEFAULT;

  @BeforeClass
  public static void initializeClass() {
    OmidTransactionalOperationExecutor.maxDequeueRetries = 200;
    OmidTransactionalOperationExecutor.dequeueRetrySleep = 5;
  }

  @Before
  public void initializeBefore() {
    this.executor = getOmidExecutor();
    this.handle = getTableHandle();
  }

  protected abstract OmidTransactionalOperationExecutor getOmidExecutor();
  
  protected abstract OVCTableHandle getTableHandle();

  protected abstract int getNumIterations();

  /**
   * Every subclass should implement this to verify that injection works and uses the correct table type
   */
  @Test
  public abstract void testInjection();

  @Test
  public void testGetGroupIdAndGetGroupMeta() throws Exception {
    OmidTransactionalOperationExecutor.disableQueuePayloads = true;
    byte [] queueName = Bytes.toBytes("testGetGroupIdAndGetGroupMeta");

    long groupid = this.executor.execute(context, new GetGroupID(queueName));
    assertEquals(1L, groupid);

    QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer consumer = new QueueConsumer(0, groupid, 1, config);
    this.executor.execute(context, new QueueConfigure(queueName, consumer));

    this.executor.commit(context, new QueueEnqueue(queueName, new QueueEntry(queueName)));
    this.executor.execute(context,
        new QueueDequeue(queueName, consumer, config));

    OperationResult<QueueInfo> meta;
    meta = this.executor.execute(context, new GetQueueInfo(queueName));

    assertFalse(meta.isEmpty());
    // TODO do something useful with this
    /*
    assertEquals(1L, meta.getValue().getCurrentWritePointer());
    assertEquals(1L, meta.getValue().getGlobalHeadPointer());
    assertEquals(1, meta.getValue().getGroups().length);
    assertEquals(1L, meta.getValue().getGroups()[0].getHead().getEntryId());
    */
    OmidTransactionalOperationExecutor.disableQueuePayloads = false;
  }

  static final byte [] kvcol = Operation.KV_COL;

  @Test
  public void testClearFabric() throws Exception {
    OmidTransactionalOperationExecutor.disableQueuePayloads = true;
    byte [] queueName = Bytes.toBytes("queue://testClearFabric_queue");
    byte [] streamName = Bytes.toBytes("stream://testClearFabric_stream");
    byte [] keyAndValue = Bytes.toBytes("testClearFabric");

    // clear first to catch ENG-375
    this.executor.execute(context, new ClearFabric());

    long groupid = this.executor.execute(context, new GetGroupID(queueName));
    assertEquals(1L, groupid);

    QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer qConsumer = new QueueConsumer(0, groupid, 1, config);
    QueueConsumer sConsumer = new QueueConsumer(0, groupid, 1, config);

    // configure queue and stream
    this.executor.execute(context, new QueueConfigure(queueName, qConsumer));
    this.executor.execute(context, new QueueConfigure(streamName, sConsumer));

    // enqueue to queue, stream, and write data
    this.executor.commit(context, new QueueEnqueue(queueName, new QueueEntry(queueName)));
    this.executor.commit(context, new QueueEnqueue(streamName, new QueueEntry(streamName)));
    this.executor.commit(context, new Write(keyAndValue, kvcol, keyAndValue));

    // verify it can all be read
    assertTrue(this.executor.execute(context,
        new QueueDequeue(queueName, qConsumer, config)).isSuccess());
    assertTrue(this.executor.execute(context,
        new QueueDequeue(streamName, sConsumer, config)).isSuccess());
    assertArrayEquals(keyAndValue, this.executor.execute(context,
        new Read(keyAndValue, kvcol)).getValue().get(kvcol));

    // and it can be read twice
    assertTrue(this.executor.execute(context,
        new QueueDequeue(queueName, qConsumer, config)).isSuccess());
    assertTrue(this.executor.execute(context,
        new QueueDequeue(streamName, sConsumer, config)).isSuccess());
    assertArrayEquals(keyAndValue, this.executor.execute(context,
        new Read(keyAndValue, kvcol)).getValue().get(kvcol));

    // but if we clear the fabric they all disappear
    this.executor.execute(context, new ClearFabric());

    // configure queue and stream again
    this.executor.execute(context, new QueueConfigure(queueName, qConsumer));
    this.executor.execute(context, new QueueConfigure(streamName, sConsumer));
    // everything is gone!
    assertTrue(this.executor.execute(context,
        new QueueDequeue(queueName, qConsumer, config)).isEmpty());
    assertTrue(this.executor.execute(context,
        new QueueDequeue(streamName, sConsumer, config)).isEmpty());
    OperationResult<Map<byte[],byte[]>> result =
      this.executor.execute(context, new Read(keyAndValue, kvcol));
    assertTrue(result.isEmpty() || null == result.getValue().get(kvcol));
    OmidTransactionalOperationExecutor.disableQueuePayloads = false;
  }

  @Test
  public void testStandaloneSimpleDequeue() throws Exception {
    OmidTransactionalOperationExecutor.disableQueuePayloads = true;
    
    byte [] queueName = Bytes.toBytes("standaloneDequeue");
    QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);
    this.executor.execute(context, new QueueConfigure(queueName, consumer));

    // Queue should be empty
    QueueDequeue dequeue = new QueueDequeue(queueName, consumer, config);
    DequeueResult result = this.executor.execute(context, dequeue);
    assertTrue(result.isEmpty());

    // Write to the queue
    this.executor.commit(context, Collections.singletonList(
      (WriteOperation) new QueueEnqueue(queueName, new QueueEntry(Bytes.toBytes(1L)))));

    // DequeuePayload entry just written
    dequeue = new QueueDequeue(queueName, consumer, config);
    result = this.executor.execute(context, dequeue);
    assertDequeueResultSuccess(result, Bytes.toBytes(1L));

    // DequeuePayload again should give same entry back
    dequeue = new QueueDequeue(queueName, consumer, config);
    result = this.executor.execute(context, dequeue);
    assertDequeueResultSuccess(result, Bytes.toBytes(1L));

    // Ack it
    this.executor.commit(context, Collections.singletonList((WriteOperation) new QueueAck(queueName, result.getEntryPointer(), consumer)));

    // Queue should be empty again
    dequeue = new QueueDequeue(queueName, consumer, config);
    result = this.executor.execute(context, dequeue);
    assertTrue(result.isEmpty());
    
    OmidTransactionalOperationExecutor.disableQueuePayloads = false;
  }

  private void assertDequeueResultSuccess(DequeueResult result, byte[] bytes) {
    assertNotNull("DequeuePayload result was unexpectedly null", result);
    assertTrue("Expected dequeue result to be successful but was " + result,
        result.isSuccess());
    assertTrue("Expected value (" + Bytes.toStringBinary(bytes) + ") but got " +
        "(" + Bytes.toStringBinary(result.getEntry().getData()) + ")",
        Bytes.equals(result.getEntry().getData(), bytes));
    assertEquals("Incorrect length", bytes.length, result.getEntry().getData().length);
  }

  @Test
  public void testUserReadOwnWritesAndWritesStableSorted() throws Exception {

    byte [] key = Bytes.toBytes("testUWSSkey");

    // Write value = 1
    this.executor.commit(context, batch(new Write(key, kvcol, Bytes.toBytes(1L))));

    // Verify value = 1
    assertArrayEquals(Bytes.toBytes(1L),
        this.executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol));

    // Create batch with increment and compareAndSwap
    // first try (CAS(1->3),Increment(3->4))
    // (will fail if operations are reordered)
    this.executor.commit(context, batch(new CompareAndSwap(key, kvcol, Bytes.toBytes(1L), Bytes.toBytes(3L)),
                                        new Increment(key, kvcol, 1L)));

    // verify value = 4
    // (value = 2 if no ReadOwnWrites)
    byte [] value = this.executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol);
    assertEquals(4L, Bytes.toLong(value));

    // Create another batch with increment and compareAndSwap, change order
    // second try (Increment(4->5),CAS(5->1))
    // (will fail if operations are reordered or if no ReadOwnWrites)
    this.executor.commit(context, batch(new Increment(key, kvcol, 1L),
                                        new CompareAndSwap(key, kvcol, Bytes.toBytes(5L), Bytes.toBytes(1L))));

    // verify value = 1
    value = this.executor.execute(context, new Read(key, kvcol)).getValue().get(kvcol);
    assertEquals(1L, Bytes.toLong(value));
  }

  @Test
  public void testWriteBatchJustAck() throws Exception {
    OmidTransactionalOperationExecutor.disableQueuePayloads = true;
    
    byte [] queueName = Bytes.toBytes("testWriteBatchJustAck");

    TTQueueOnVCTable.TRACE = true;
    QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);
    this.executor.execute(context, new QueueConfigure(queueName, consumer));

    // Queue should be empty
    QueueDequeue dequeue = new QueueDequeue(queueName, consumer, config);
    DequeueResult result = this.executor.execute(context, dequeue);
    assertTrue(result.isEmpty());

    // Write to the queue
    this.executor.commit(context, batch(new QueueEnqueue(queueName, new QueueEntry(Bytes.toBytes(1L)))));

    // DequeuePayload entry just written
    dequeue = new QueueDequeue(queueName, consumer, config);
    result = this.executor.execute(context, dequeue);
    assertDequeueResultSuccess(result, Bytes.toBytes(1L));

    TTQueueOnVCTable.TRACE = false;

    // Ack it
    this.executor.commit(context, batch(new QueueAck(queueName, result.getEntryPointer(), consumer)));

    // Can't ack it again
    try {
      this.executor.commit(context, batch(new QueueAck(queueName, result.getEntryPointer(), consumer)));
      fail("Expecting OperationException for repeated ack.");
    } catch (OperationException e) {
      // expected
    }

    // Queue should be empty again
    dequeue = new QueueDequeue(queueName, consumer, config);
    result = this.executor.execute(context, dequeue);
    assertTrue(result.isEmpty());
    
    OmidTransactionalOperationExecutor.disableQueuePayloads = false;
  }

  @Test
  public void testWriteBatchWithMultiWritesMultiEnqueuesPlusSuccessfulAck()
      throws Exception {
    OmidTransactionalOperationExecutor.disableQueuePayloads = true;

    // Verify operations are re-ordered
    // Verify user write operations are stable sorted

    QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);

    // One source queue
    byte [] srcQueueName = Bytes.toBytes("testAckRollback_srcQueue1");
    // Source queue entry
    byte [] srcQueueValue = Bytes.toBytes("srcQueueValue");

    // Two dest queues
    byte [] destQueueOne = Bytes.toBytes("testAckRollback_destQueue1");
    byte [] destQueueTwo = Bytes.toBytes("testAckRollback_destQueue2");
    // Dest queue values
    byte [] destQueueOneVal = Bytes.toBytes("destValue1");
    byte [] destQueueTwoVal = Bytes.toBytes("destValue2");

    // Data key
    byte [] dataKey = Bytes.toBytes("datakey");
    long expectedVal;

    // Configure queues
    this.executor.execute(context, new QueueConfigure(srcQueueName, consumer));
    this.executor.execute(context, new QueueConfigure(destQueueOne, consumer));
    this.executor.execute(context, new QueueConfigure(destQueueTwo, consumer));

    // Go!

    // Add an entry to source queue
    this.executor.commit(context, new QueueEnqueue(srcQueueName, new QueueEntry(srcQueueValue)));

    // DequeuePayload one entry from source queue
    DequeueResult srcDequeueResult = this.executor.execute(context,
        new QueueDequeue(srcQueueName, consumer, config));
    assertTrue(srcDequeueResult.isSuccess());
    assertTrue(Bytes.equals(srcQueueValue, srcDequeueResult.getEntry().getData()));

    // Create batch of writes
    List<WriteOperation> writes = new ArrayList<WriteOperation>();

    // Add increment operation
    writes.add(new Increment(dataKey, kvcol, 1));

    // Add an ack of entry one in source queue
    writes.add(new QueueAck(srcQueueName,
        srcDequeueResult.getEntryPointer(), consumer));

    // Add a push to dest queue one
    writes.add(new QueueEnqueue(destQueueOne, new QueueEntry(destQueueOneVal)));

    // Add a compare-and-swap
    writes.add(new CompareAndSwap(
        dataKey, kvcol, Bytes.toBytes(1L), Bytes.toBytes(10L)));

    // Add a push to dest queue two
    writes.add(new QueueEnqueue(destQueueTwo, new QueueEntry(destQueueTwoVal)));

    // Add another user increment operation
    writes.add(new Increment(dataKey, kvcol, 3));
    expectedVal = 13L;

    // Commit batch successfully
    this.executor.commit(context, writes);

    // Verify value from operations was done in order
    assertEquals(expectedVal, Bytes.toLong(
        this.executor.execute(context, new Read(dataKey, kvcol)).getValue().get(kvcol)));

    // DequeuePayload from both dest queues, verify, ack
    DequeueResult destDequeueResult = this.executor.execute(context,
        new QueueDequeue(destQueueOne, consumer, config));
    assertDequeueResultSuccess(destDequeueResult, destQueueOneVal);
    this.executor.commit(context, new QueueAck(destQueueOne, destDequeueResult.getEntryPointer(), consumer));
    destDequeueResult = this.executor.execute(context,
        new QueueDequeue(destQueueTwo, consumer, config));
    assertDequeueResultSuccess(destDequeueResult, destQueueTwoVal);
    this.executor.commit(context, new QueueAck(destQueueTwo, destDequeueResult.getEntryPointer(), consumer));

    // Dest queues should be empty
    destDequeueResult = this.executor.execute(context,
        new QueueDequeue(destQueueOne, consumer, config));
    assertTrue(destDequeueResult.isEmpty());
    destDequeueResult = this.executor.execute(context,
        new QueueDequeue(destQueueTwo, consumer, config));
    assertTrue(destDequeueResult.isEmpty());

    OmidTransactionalOperationExecutor.disableQueuePayloads = false;
  }

  @Test
  public void
  testWriteBatchWithMultiWritesMultiEnqueuesPlusUnsuccessfulAckRollback()
      throws Exception {
    OmidTransactionalOperationExecutor.disableQueuePayloads = true;

    QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);

    // One source queue
    byte [] srcQueueName = Bytes.toBytes("AAtestAckRollback_srcQueue1");
    // Source queue entry
    byte [] srcQueueValue = Bytes.toBytes("AAsrcQueueValue");

    // Two dest queues
    byte [] destQueueOne = Bytes.toBytes("AAtestAckRollback_destQueue1");
    byte [] destQueueTwo = Bytes.toBytes("AAtestAckRollback_destQueue2");
    // Dest queue values
    byte [] destQueueOneVal = Bytes.toBytes("AAdestValue1");
    byte [] destQueueTwoVal = Bytes.toBytes("AAdestValue2");

    // Three keys we will increment
    byte [][] dataKeys = new byte [][] {
        Bytes.toBytes(1), Bytes.toBytes(2), Bytes.toBytes(3)
    };
    long [] expectedVals = new long [] { 0L, 0L, 0L };

    // Go!

    // Configure queues
    this.executor.execute(context, new QueueConfigure(srcQueueName, consumer));
    this.executor.execute(context, new QueueConfigure(destQueueOne, consumer));
    this.executor.execute(context, new QueueConfigure(destQueueTwo, consumer));

    // Add an entry to source queue
    this.executor.commit(context, new QueueEnqueue(srcQueueName, new QueueEntry(srcQueueValue)));

    // DequeuePayload one entry from source queue
    DequeueResult srcDequeueResult = this.executor.execute(context,
        new QueueDequeue(srcQueueName, consumer, config));
    assertTrue(srcDequeueResult.isSuccess());
    assertTrue(Bytes.equals(srcQueueValue, srcDequeueResult.getEntry().getData()));

    // Create batch of writes
    List<WriteOperation> writes = new ArrayList<WriteOperation>();

    // Add two user increment operations
    writes.add(new Increment(dataKeys[0], kvcol, 1));
    writes.add(new Increment(dataKeys[1], kvcol, 2));
    // Update expected vals (this batch will be successful)
    expectedVals[0] = 1L;
    expectedVals[1] = 2L;

    // Add an ack of entry one in source queue
    writes.add(new QueueAck(srcQueueName,
        srcDequeueResult.getEntryPointer(), consumer));

    // Add two pushes to two dest queues
    writes.add(new QueueEnqueue(destQueueOne, new QueueEntry(destQueueOneVal)));
    writes.add(new QueueEnqueue(destQueueTwo, new QueueEntry(destQueueTwoVal)));

    // Add another user increment operation
    writes.add(new Increment(dataKeys[2], kvcol, 3));
    expectedVals[2] = 3L;

    // Commit batch successfully
    this.executor.commit(context, writes);

    // Verify three values from increment operations
    for (int i=0; i<3; i++) {
      assertEquals(expectedVals[i], Bytes.toLong(
          this.executor.execute(context, new Read(dataKeys[i], kvcol)).getValue().get(kvcol)));
    }

    // DequeuePayload from both dest queues, verify, ack
    DequeueResult destDequeueResult = this.executor.execute(context,
        new QueueDequeue(destQueueOne, consumer, config));
    assertTrue(destDequeueResult.isSuccess());
    assertTrue(Bytes.equals(destQueueOneVal, destDequeueResult.getEntry().getData()));

    this.executor.commit(context, new QueueAck(destQueueOne, destDequeueResult.getEntryPointer(), consumer));
    destDequeueResult = this.executor.execute(context,
        new QueueDequeue(destQueueTwo, consumer, config));

    assertTrue(destDequeueResult.isSuccess());
    assertTrue(Bytes.equals(destQueueTwoVal, destDequeueResult.getEntry().getData()));

    this.executor.commit(context, new QueueAck(destQueueTwo, destDequeueResult.getEntryPointer(), consumer));

    // Dest queues should be empty
    destDequeueResult = this.executor.execute(context,
        new QueueDequeue(destQueueOne, consumer, config));
    assertTrue(destDequeueResult.isEmpty());
    destDequeueResult = this.executor.execute(context,
        new QueueDequeue(destQueueTwo, consumer, config));
    assertTrue(destDequeueResult.isEmpty());


    // Create another batch of writes
    writes = new ArrayList<WriteOperation>();

    // Add one user increment operation
    writes.add(new Increment(dataKeys[0], kvcol, 1));
    // Don't change expected, this will fail

    // Add an ack of entry one in source queue (we already ackd, should fail)
    writes.add(new QueueAck(srcQueueName,
        srcDequeueResult.getEntryPointer(), consumer));

    // Add two pushes to two dest queues
    writes.add(new QueueEnqueue(destQueueOne, new QueueEntry(destQueueOneVal)));
    writes.add(new QueueEnqueue(destQueueTwo, new QueueEntry(destQueueTwoVal)));

    // Add another user increment operation
    writes.add(new Increment(dataKeys[2], kvcol, 3));

    // Commit batch, should fail
    try {
      this.executor.commit(context, writes);
      fail("Expected OperationException");
    } catch (OperationException e) {
      // expected
    }


    // All values from increments should be the same as before
    for (int i=0; i<3; i++) {
      assertEquals(expectedVals[i], Bytes.toLong(
          this.executor.execute(context, new Read(dataKeys[i], kvcol)).getValue().get(kvcol)));
    }

    // Dest queues should still be empty
    destDequeueResult = this.executor.execute(context,
        new QueueDequeue(destQueueOne, consumer, config));
    assertTrue(destDequeueResult.isEmpty());
    destDequeueResult = this.executor.execute(context,
        new QueueDequeue(destQueueTwo, consumer, config));
    assertTrue(destDequeueResult.isEmpty());

    OmidTransactionalOperationExecutor.disableQueuePayloads = false;
  }

  @Test
  public void testLotsOfEnqueuesThenDequeues() throws Exception {
    OmidTransactionalOperationExecutor.disableQueuePayloads = true;

    byte [] queueName = Bytes.toBytes("queue_testLotsOfEnqueuesThenDequeues");
    int numEntries = getNumIterations();

    long startTime = System.currentTimeMillis();

    for (int i=1; i<numEntries+1; i++) {
      byte [] entry = Bytes.toBytes(i);
      this.executor.commit(context, new QueueEnqueue(queueName, new QueueEntry(entry)));
    }

    long enqueueStop = System.currentTimeMillis();

    System.out.println("Finished enqueue of " + numEntries + " entries in " +
        (enqueueStop-startTime) + " ms (" +
        (enqueueStop-startTime)/((float)numEntries) + " ms/entry)");

    // First consume them all in sync mode

    QueueConfig configOne = new QueueConfig(PartitionerType.FIFO, true);
    // Configure queue
    QueueConsumer consumerOne = new StatefulQueueConsumer(0, 0, 1, configOne);
    this.executor.execute(context, new QueueConfigure(queueName, consumerOne));
    for (int i=1; i<numEntries+1; i++) {
      DequeueResult result = this.executor.execute(context, new QueueDequeue(queueName, consumerOne, configOne));
      assertTrue(result.isSuccess());
      assertEquals(i, Bytes.toInt(result.getEntry().getData()));
      this.executor.commit(context, new QueueAck(queueName, result.getEntryPointer(), consumerOne));
      if (i % 100 == 0) System.out.print(".");
      if (i % 1000 == 0) System.out.println(" " + i);
    }

    long dequeueSyncStop = System.currentTimeMillis();

    System.out.println("Finished sync dequeue of " + numEntries +
        " entries in " + (dequeueSyncStop-enqueueStop) + " ms (" +
        (dequeueSyncStop-enqueueStop)/((float)numEntries) + " ms/entry)");

    // Now consume them all in async mode, no ack
    QueueConfig configTwo = new QueueConfig(PartitionerType.FIFO, false);
    QueueConsumer consumerTwo = new StatefulQueueConsumer(0, 2, 1, configTwo);
    this.executor.execute(context, new QueueConfigure(queueName, consumerTwo));
    for (int i=1; i<numEntries+1; i++) {
      DequeueResult result = this.executor.execute(context, new QueueDequeue(queueName, consumerTwo, configTwo));
      assertTrue(result.isSuccess());
      assertTrue("Expected " + i + ", Actual " + Bytes.toInt(result.getEntry().getData()),
          Bytes.equals(Bytes.toBytes(i), result.getEntry().getData()));
      if (i % 100 == 0) System.out.print(".");
      if (i % 1000 == 0) System.out.println(" " + i);
    }

    long dequeueAsyncStop = System.currentTimeMillis();

    System.out.println("Finished async dequeue of " + numEntries +
        " entries in " + (dequeueAsyncStop-dequeueSyncStop) + " ms (" +
        (dequeueAsyncStop-dequeueSyncStop)/((float)numEntries) + " ms/entry)");

    // Both queues should be empty for each consumer
    assertTrue(this.executor.execute(context, new QueueDequeue(queueName, consumerOne, configOne)).isEmpty());
    assertTrue(this.executor.execute(context, new QueueDequeue(queueName, consumerTwo, configTwo)).isEmpty());
    
    OmidTransactionalOperationExecutor.disableQueuePayloads = false;
  }

  @Test
  public void testConcurrentEnqueueDequeue() throws Exception {

    OmidTransactionalOperationExecutor.disableQueuePayloads = true;

    final OmidTransactionalOperationExecutor executorFinal = this.executor;
    final int n = getNumIterations();
    final byte [] queueName = Bytes.toBytes("testConcurrentEnqueueDequeue");

    // Create and start a thread that dequeues in a loop
    final QueueConfig config = new QueueConfig(PartitionerType.FIFO, true);
    final QueueConsumer consumer = new QueueConsumer(0, 0, 1, config);
    this.executor.execute(context, new QueueConfigure(queueName, consumer));
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
            result = executorFinal.execute(context, new QueueDequeue(queueName, consumer, config));
          } catch (OperationException e) {
            System.out.println("DequeuePayload failed! " + e.getMessage());
            return;
          }
          if (result.isSuccess()) {
            dequeued.add(result.getEntry().getData());
            try {
              executorFinal.commit(context, new QueueAck(queueName, result.getEntryPointer(), consumer));
              lastSuccess = true;
            } catch (OperationException e) {
              fail("Exception for QueueAck");
            }
          } else {
            numEmpty.incrementAndGet();
            lastSuccess = false;
          }
        }
        if (dequeued.size() < n) {
          System.out.println("Dequeuer stopped before it finished!");
          long lastEntryId = dequeued.size();
          System.out.println("Last success was entry id " + lastEntryId);
          try {
            printQueueInfo(queueName, 0);
            printEntryInfo(queueName, lastEntryId);
            printEntryInfo(queueName, lastEntryId+1);
            printEntryInfo(queueName, lastEntryId+2);
          } catch (OperationException e) {
            fail();
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
            executorFinal.commit(context, new QueueEnqueue(queueName, new QueueEntry(Bytes.toBytes(i))));
          } catch (OperationException e) {
            fail("Exception for QueueEnqueue " + i);
          }
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
    assertEquals(n, dequeued.size());  // fails expected:<100> but was:<63>
    
    OmidTransactionalOperationExecutor.disableQueuePayloads = false;
  }

  final byte [] threadedQueueName = Bytes.toBytes("threadedQueue");

  @Test
  public void testThreadedProducersAndThreadedConsumers() throws Exception {
    OmidTransactionalOperationExecutor.disableQueuePayloads = true;

    long MAX_TIMEOUT = 30000;
    //    OmidTransactionalOperationExecutor.maxDequeueRetries = 100;
    //    OmidTransactionalOperationExecutor.dequeueRetrySleep = 1;
    ConcurrentSkipListMap<byte[], byte[]> enqueuedMap =
      new ConcurrentSkipListMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    ConcurrentSkipListMap<byte[], byte[]> dequeuedMapOne =
      new ConcurrentSkipListMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    ConcurrentSkipListMap<byte[], byte[]> dequeuedMapTwo = new
      ConcurrentSkipListMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);

    AtomicBoolean producersDone = new AtomicBoolean(false);

    long startTime = System.currentTimeMillis();

    // Create P producer threads, each inserts N queue entries
    int p = 5;
    int n = getNumIterations();
    Producer [] producers = new Producer[p];
    for (int i=0;i<p;i++) {
      producers[i] = new Producer(i, n, enqueuedMap);
    }

    // Create (P*2) consumer threads, two groups of (P)
    // Use synchronous execution first
    Consumer [] consumerGroupOne = new Consumer[p];
    Consumer [] consumerGroupTwo = new Consumer[p];
    QueueConfig config=new QueueConfig(PartitionerType.FIFO, true);

    for (int i=0;i<p;i++) {
      consumerGroupOne[i]=new Consumer(new QueueConsumer(i, 0, p, config),
                                       config, dequeuedMapOne, producersDone);
      this.executor.execute(context,
                            new QueueConfigure(
                              TestOmidExecutorLikeAFlow.this.threadedQueueName, consumerGroupOne[i].consumer
                            ));
    }

    for (int i=0;i<p;i++) {
      consumerGroupTwo[i]=new Consumer(new QueueConsumer(i, 1, p, config),
                                       config, dequeuedMapTwo, producersDone);
      this.executor.execute(context,
                            new QueueConfigure(
                              TestOmidExecutorLikeAFlow.this.threadedQueueName, consumerGroupTwo[i].consumer
                            ));
    }

    // Let the producing begin!
    System.out.println("Starting producers");
    for (int i=0; i<p; i++) producers[i].start();
    long expectedDequeues = p * n;

    long startConsumers = System.currentTimeMillis();

    // Start consumers!
    System.out.println("Starting consumers");
    for (int i=0; i<p; i++) consumerGroupOne[i].start();
    for (int i=0; i<p; i++) consumerGroupTwo[i].start();

    // Wait for producers to finish
    System.out.println("Waiting for producers to finish");
    long start = System.currentTimeMillis();
    for (int i=0; i<p; i++) producers[i].join(MAX_TIMEOUT);
    long stop = System.currentTimeMillis();
    System.out.println("Producers done");
    if (stop - start >= MAX_TIMEOUT)
      fail("Timed out waiting for producers");
    producersDone.set(true);

    // Verify producers produced correct number
    assertEquals(p * n , enqueuedMap.size());
    System.out.println("Producers correctly enqueued " +
                         enqueuedMap.size() + " entries");

    long producerTime = System.currentTimeMillis();
    System.out.println("" + p + " producers generated " + (n*p) +
                         " total queue entries in " + (producerTime - startTime) +
                         " millis (" + ((producerTime - startTime)/((float)(n*p))) +
                         " ms/enqueue)");

    // Wait for consumers to finish
    System.out.println("Waiting for consumers to finish");
    start = System.currentTimeMillis();
    for (int i=0; i<p; i++) consumerGroupOne[i].join(MAX_TIMEOUT);
    for (int i=0; i<p; i++) consumerGroupTwo[i].join(MAX_TIMEOUT);
    stop = System.currentTimeMillis();
    System.out.println("Consumers done!");
    if (stop - start >= MAX_TIMEOUT)
      fail("Timed out waiting for consumers");

    long stopTime = System.currentTimeMillis();
    System.out.println("" + (p*2) + " consumers dequeued " +
                         (expectedDequeues*2) +
                         " total queue entries in " + (stopTime - startConsumers) +
                         " millis (" + ((stopTime - startConsumers)/((float)
      (expectedDequeues*2))) + " ms/dequeue)");

    // Each group should total <expectedDequeues>

    long groupOneTotal = 0;
    long groupTwoTotal = 0;
    for (int i=0; i<p; i++) {
      groupOneTotal += consumerGroupOne[i].dequeued;
      groupTwoTotal += consumerGroupTwo[i].dequeued;
    }
    if (expectedDequeues != groupOneTotal) {
      System.out.println("Group One: totalDequeues=" + groupOneTotal
                           + ", DequeuedMap.size=" + dequeuedMapOne.size());
      for (byte [] dequeuedValue : dequeuedMapOne.values()) {
        enqueuedMap.remove(dequeuedValue);
      }
      System.out.println("After removing dequeued entries, there" +
                           " are " + enqueuedMap.size() + " remaining entries " +
                           "produced");
      for (byte [] enqueuedValue : enqueuedMap.values()) {
        System.out.println("EnqueuedNotDequeued: instanceid=" +
                             Bytes.toInt(enqueuedValue) + ", entrynum=" +
                             Bytes.toInt(enqueuedValue, 4));
      }
      printQueueInfo(this.threadedQueueName, 0);
    }
    assertEquals(expectedDequeues, groupOneTotal);
    if (expectedDequeues != groupTwoTotal) {
      System.out.println("Group Two: totalDequeues=" + groupTwoTotal
                           + ", DequeuedMap.size=" + dequeuedMapTwo.size());
      for (byte [] dequeuedValue : dequeuedMapTwo.values()) {
        enqueuedMap.remove(dequeuedValue);
      }
      System.out.println("After removing dequeued entries, there " +
                           "are " + enqueuedMap.size() + " remaining entries " +
                           "produced");
      for (byte [] enqueuedValue : enqueuedMap.values()) {
        System.out.println("EnqueuedNotDequeued: instanceid=" +
                             Bytes.toInt(enqueuedValue) + ", entrynum=" +
                             Bytes.toInt(enqueuedValue, 4));
      }
      printQueueInfo(this.threadedQueueName, 1);
    }
    assertEquals(expectedDequeues, groupTwoTotal);

    OmidTransactionalOperationExecutor.disableQueuePayloads = false;
  }

  private TTQueueTable getQueueTable() throws OperationException {
    return this.handle.getQueueTable(Bytes.toBytes("queues"));
  }

  private void printQueueInfo(byte[] queueName, int groupId)
      throws OperationException {
    System.out.println(getQueueTable().getGroupInfo(queueName, groupId));
  }

  private void printEntryInfo(byte[] queueName, long entryId)
      throws OperationException {
    System.out.println(getQueueTable().getEntryInfo(queueName, entryId));
  }

  class Producer extends Thread {
    int instanceid;
    int numentries;
    ConcurrentSkipListMap<byte[], byte[]> enqueuedMap;
    Producer(int instanceid, int numentries,
        ConcurrentSkipListMap<byte[], byte[]> enqueuedMap) {
      this.instanceid = instanceid;
      this.numentries = numentries;
      this.enqueuedMap = enqueuedMap;
      System.out.println("Producer " + instanceid + " will enqueue " +
          numentries + " entries");
    }
    @Override
    public void run() {
      System.out.println("Producer " + this.instanceid + " running");
      for (int i=0; i<this.numentries; i++) {
        try {
          QueueEntry entry = new QueueEntry(Bytes.add(Bytes.toBytes(this.instanceid), Bytes.toBytes(i)));
          TestOmidExecutorLikeAFlow.this.executor.commit(
            context, new QueueEnqueue(TestOmidExecutorLikeAFlow.this.threadedQueueName, entry));
          this.enqueuedMap.put(entry.getData(), entry.getData());
        } catch (OperationException e) {
          e.printStackTrace();
          fail("OperationException for EnqueuePayload" + e.getMessage());
        }
      }
      System.out.println("Producer " + this.instanceid + " done");
    }
  }

  class Consumer extends Thread {
    QueueConsumer consumer;
    QueueConfig config;
    long dequeued = 0;
    ConcurrentSkipListMap<byte[], byte[]> dequeuedMap;
    AtomicBoolean producersDone;
    Consumer(QueueConsumer consumer, QueueConfig config,
        ConcurrentSkipListMap<byte[], byte[]> dequeuedMap,
        AtomicBoolean producersDone) {
      this.consumer = consumer;
      this.config = config;
      this.dequeuedMap = dequeuedMap;
      this.producersDone = producersDone;
    }
    @Override
    public void run() {
      while (true) {
        boolean localProducersDone = this.producersDone.get();
        QueueDequeue dequeue =
            new QueueDequeue(TestOmidExecutorLikeAFlow.this.threadedQueueName,
                this.consumer, this.config);
        try {
          DequeueResult result = null;
          try {
            result = TestOmidExecutorLikeAFlow.this.
                executor.execute(context, dequeue);
          } catch (OperationException e) {
            e.printStackTrace();
            fail("DequeuePayload failed: " + e.getMessage());
          }
          if (result.isSuccess() && this.config.isSingleEntry()) {
            try {
              TestOmidExecutorLikeAFlow.this.executor.commit(context, new QueueAck(TestOmidExecutorLikeAFlow.this.threadedQueueName, result.getEntryPointer(), this.consumer));
            } catch (OperationException e) {
              e.printStackTrace();
              fail("OperationException for Ack" + e.getMessage());
            }
          }
          if (result.isSuccess()) {
            this.dequeued++;
            this.dequeuedMap.put(result.getEntry().getData(), result.getEntry().getData());
          } else if (result.isEmpty() && localProducersDone) {
            System.out.println(this.consumer.toString() + " finished after " +
                this.dequeued + " dequeues, consumer thread exiting");
            return;
          } else if (result.isEmpty() && !localProducersDone) {
            System.out.println(this.consumer.toString() + " empty but waiting");
            Thread.sleep(1);
          } else {
            fail("What is this?");
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }
  }
}
