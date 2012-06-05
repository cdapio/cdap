package com.continuuity.data.operation.executor.omid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.SyncReadTimeoutException;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.operation.type.WriteOperation;
import com.continuuity.data.runtime.DataFabricInMemoryModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestOmidExecutorLikeAFlow {

  @SuppressWarnings("unused")
  private TimestampOracle timeOracle;

  @SuppressWarnings("unused")
  private TransactionOracle oracle;

  @SuppressWarnings("unused")
  private Configuration conf = new CConfiguration();

  private OmidTransactionalOperationExecutor executor;

  private OVCTableHandle handle;


  private static List<WriteOperation> batch(WriteOperation ... ops) {
    return Arrays.asList(ops);
  }

  @Before
  public void initialize() {
    Injector injector = Guice.createInjector(new DataFabricInMemoryModule());

    timeOracle = injector.getInstance(TimestampOracle.class);
    executor = injector.getInstance(OmidTransactionalOperationExecutor.class);
    oracle = executor.getOracle();
    handle = executor.getTableHandle();
  }

  @Test
  public void testStandaloneSimpleDequeue() throws Exception {

    byte [] queueName = Bytes.toBytes("standaloneDequeue");
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueueConfig config = new QueueConfig(
        new QueuePartitioner.RandomPartitioner(), true);

    // Queue should be empty
    QueueDequeue dequeue = new QueueDequeue(queueName, consumer, config);
    DequeueResult result = this.executor.execute(dequeue);
    assertTrue(result.isEmpty());

    // Write to the queue
    assertTrue(this.executor.execute(Arrays.asList(new WriteOperation [] {
        new QueueEnqueue(queueName, Bytes.toBytes(1L))
    })).isSuccess());

    // Dequeue entry just written
    dequeue = new QueueDequeue(queueName, consumer, config);
    result = this.executor.execute(dequeue);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(1L)));

    // Dequeue again should give same entry back
    dequeue = new QueueDequeue(queueName, consumer, config);
    result = this.executor.execute(dequeue);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(1L)));

    // Ack it
    assertTrue(this.executor.execute(Arrays.asList(new WriteOperation [] {
        new QueueAck(queueName, result.getEntryPointer(), consumer)
    })).isSuccess());

    // Queue should be empty again
    dequeue = new QueueDequeue(queueName, consumer, config);
    result = this.executor.execute(dequeue);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testUserReadOwnWritesAndWritesStableSorted() throws Exception {

    byte [] key = Bytes.toBytes("testUWSSkey");

    // Write value = 1
    this.executor.execute(batch(new Write(key, Bytes.toBytes(1L))));

    // Verify value = 1
    assertTrue(Bytes.equals(Bytes.toBytes(1L),
        this.executor.execute(new Read(key))));

    // Create batch with increment and compareAndSwap
    // first try (CAS(1->3),Increment(3->4))
    // (will fail if operations are reordered)
    assertTrue(this.executor.execute(batch(
        new CompareAndSwap(key, Bytes.toBytes(1L), Bytes.toBytes(3L)),
        new Increment(key, 1L))).isSuccess());

    // verify value = 4
    // (value = 2 if no ReadOwnWrites)
    byte [] value = this.executor.execute(new Read(key));
    assertEquals(4L, Bytes.toLong(value));

    // Create another batch with increment and compareAndSwap, change order
    // second try (Increment(4->5),CAS(5->1))
    // (will fail if operations are reordered or if no ReadOwnWrites)
    assertTrue(this.executor.execute(batch(new Increment(key, 1L),
        new CompareAndSwap(key, Bytes.toBytes(5L), Bytes.toBytes(1L)))).
        isSuccess());

    // verify value = 1
    value = this.executor.execute(new Read(key));
    assertEquals(1L, Bytes.toLong(value));
  }

  @Test
  public void testWriteBatchJustAck() throws Exception {

    byte [] queueName = Bytes.toBytes("testWriteBatchJustAck");
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueueConfig config = new QueueConfig(
        new QueuePartitioner.RandomPartitioner(), true);

    // Queue should be empty
    QueueDequeue dequeue = new QueueDequeue(queueName, consumer, config);
    DequeueResult result = this.executor.execute(dequeue);
    assertTrue(result.isEmpty());

    // Write to the queue
    assertTrue(this.executor.execute(batch(new QueueEnqueue(queueName,
        Bytes.toBytes(1L)))).isSuccess());

    // Dequeue entry just written
    dequeue = new QueueDequeue(queueName, consumer, config);
    result = this.executor.execute(dequeue);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(1L)));

    // Ack it
    assertTrue(this.executor.execute(batch(new QueueAck(queueName,
        result.getEntryPointer(), consumer))).isSuccess());

    // Can't ack it again
    assertFalse(this.executor.execute(batch(new QueueAck(queueName,
        result.getEntryPointer(), consumer))).isSuccess());

    // Queue should be empty again
    dequeue = new QueueDequeue(queueName, consumer, config);
    result = this.executor.execute(dequeue);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testWriteBatchWithMultiWritesMultiEnqueuesPlusSuccessfulAck()
      throws Exception {

    // Verify operations are re-ordered
    // Verify user write operations are stable sorted

    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueueConfig config = new QueueConfig(
        new QueuePartitioner.RandomPartitioner(), true);

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
    long expectedVal = 0L;

    // Go!

    // Add an entry to source queue
    assertTrue(this.executor.execute(batch(
        new QueueEnqueue(srcQueueName, srcQueueValue))).isSuccess());

    // Dequeue one entry from source queue
    DequeueResult srcDequeueResult = this.executor.execute(
        new QueueDequeue(srcQueueName, consumer, config));
    assertTrue(srcDequeueResult.isSuccess());
    assertTrue(Bytes.equals(srcQueueValue, srcDequeueResult.getValue()));

    // Create batch of writes
    List<WriteOperation> writes = new ArrayList<WriteOperation>();

    // Add increment operation
    writes.add(new Increment(dataKey, 1));
    expectedVal = 1L;

    // Add an ack of entry one in source queue
    writes.add(new QueueAck(srcQueueName,
        srcDequeueResult.getEntryPointer(), consumer));

    // Add a push to dest queue one
    writes.add(new QueueEnqueue(destQueueOne, destQueueOneVal));

    // Add a compare-and-swap
    writes.add(new CompareAndSwap(dataKey, Bytes.toBytes(1L), Bytes.toBytes(10L)));
    expectedVal = 10L;

    // Add a push to dest queue two
    writes.add(new QueueEnqueue(destQueueTwo, destQueueTwoVal));

    // Add another user increment operation
    writes.add(new Increment(dataKey, 3));
    expectedVal = 13L;

    // Commit batch successfully
    assertTrue(this.executor.execute(writes).isSuccess());

    // Verify value from operations was done in order
    assertEquals(expectedVal,
        Bytes.toLong(this.executor.execute(new Read(dataKey))));

    // Dequeue from both dest queues, verify, ack
    DequeueResult destDequeueResult = this.executor.execute(
        new QueueDequeue(destQueueOne, consumer, config));
    assertTrue(destDequeueResult.isSuccess());
    assertTrue(Bytes.equals(destQueueOneVal, destDequeueResult.getValue()));
    assertTrue(this.executor.execute(batch(
        new QueueAck(destQueueOne,
            destDequeueResult.getEntryPointer(), consumer))).isSuccess());
    destDequeueResult = this.executor.execute(
        new QueueDequeue(destQueueTwo, consumer, config));
    assertTrue(destDequeueResult.isSuccess());
    assertTrue(Bytes.equals(destQueueTwoVal, destDequeueResult.getValue()));
    assertTrue(this.executor.execute(batch(
        new QueueAck(destQueueTwo,
            destDequeueResult.getEntryPointer(), consumer))).isSuccess());

    // Dest queues should be empty
    destDequeueResult = this.executor.execute(
        new QueueDequeue(destQueueOne, consumer, config));
    assertTrue(destDequeueResult.isEmpty());
    destDequeueResult = this.executor.execute(
        new QueueDequeue(destQueueTwo, consumer, config));
    assertTrue(destDequeueResult.isEmpty());

  }

  @Test
  public void testWriteBatchWithMultiWritesMultiEnqueuesPlusUnsuccessfulAckRollback()
      throws Exception {

    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueueConfig config = new QueueConfig(
        new QueuePartitioner.RandomPartitioner(), true);

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

    // Three keys we will increment
    byte [][] dataKeys = new byte [][] {
        Bytes.toBytes(1), Bytes.toBytes(2), Bytes.toBytes(3)
    };
    long [] expectedVals = new long [] { 0L, 0L, 0L };

    // Go!

    // Add an entry to source queue
    assertTrue(this.executor.execute(batch(
        new QueueEnqueue(srcQueueName, srcQueueValue))).isSuccess());

    // Dequeue one entry from source queue
    DequeueResult srcDequeueResult = this.executor.execute(
        new QueueDequeue(srcQueueName, consumer, config));
    assertTrue(srcDequeueResult.isSuccess());
    assertTrue(Bytes.equals(srcQueueValue, srcDequeueResult.getValue()));

    // Create batch of writes
    List<WriteOperation> writes = new ArrayList<WriteOperation>();

    // Add two user increment operations
    writes.add(new Increment(dataKeys[0], 1));
    writes.add(new Increment(dataKeys[1], 2));
    // Update expected vals (this batch will be successful)
    expectedVals[0] = 1L;
    expectedVals[1] = 2L;

    // Add an ack of entry one in source queue
    writes.add(new QueueAck(srcQueueName,
        srcDequeueResult.getEntryPointer(), consumer));

    // Add two pushes to two dest queues
    writes.add(new QueueEnqueue(destQueueOne, destQueueOneVal));
    writes.add(new QueueEnqueue(destQueueTwo, destQueueTwoVal));

    // Add another user increment operation
    writes.add(new Increment(dataKeys[2], 3));
    expectedVals[2] = 3L;

    // Commit batch successfully
    assertTrue(this.executor.execute(writes).isSuccess());

    // Verify three values from increment operations
    for (int i=0; i<3; i++) {
      assertEquals(expectedVals[i],
          Bytes.toLong(this.executor.execute(new Read(dataKeys[i]))));
    }

    // Dequeue from both dest queues, verify, ack
    DequeueResult destDequeueResult = this.executor.execute(
        new QueueDequeue(destQueueOne, consumer, config));
    assertTrue(destDequeueResult.isSuccess());
    assertTrue(Bytes.equals(destQueueOneVal, destDequeueResult.getValue()));
    assertTrue(this.executor.execute(batch(
        new QueueAck(destQueueOne,
            destDequeueResult.getEntryPointer(), consumer))).isSuccess());
    destDequeueResult = this.executor.execute(
        new QueueDequeue(destQueueTwo, consumer, config));
    assertTrue(destDequeueResult.isSuccess());
    assertTrue(Bytes.equals(destQueueTwoVal, destDequeueResult.getValue()));
    assertTrue(this.executor.execute(batch(
        new QueueAck(destQueueTwo,
            destDequeueResult.getEntryPointer(), consumer))).isSuccess());

    // Dest queues should be empty
    destDequeueResult = this.executor.execute(
        new QueueDequeue(destQueueOne, consumer, config));
    assertTrue(destDequeueResult.isEmpty());
    destDequeueResult = this.executor.execute(
        new QueueDequeue(destQueueTwo, consumer, config));
    assertTrue(destDequeueResult.isEmpty());


    // Create another batch of writes
    writes = new ArrayList<WriteOperation>();

    // Add one user increment operation
    writes.add(new Increment(dataKeys[0], 1));
    // Don't change expected, this will fail

    // Add an ack of entry one in source queue (we already ackd, should fail)
    writes.add(new QueueAck(srcQueueName,
        srcDequeueResult.getEntryPointer(), consumer));

    // Add two pushes to two dest queues
    writes.add(new QueueEnqueue(destQueueOne, destQueueOneVal));
    writes.add(new QueueEnqueue(destQueueTwo, destQueueTwoVal));

    // Add another user increment operation
    writes.add(new Increment(dataKeys[2], 3));

    // Commit batch, should fail
    assertFalse(this.executor.execute(writes).isSuccess());


    // All values from increments should be the same as before
    for (int i=0; i<3; i++) {
      assertEquals(expectedVals[i],
          Bytes.toLong(this.executor.execute(new Read(dataKeys[i]))));
    }

    // Dest queues should still be empty
    destDequeueResult = this.executor.execute(
        new QueueDequeue(destQueueOne, consumer, config));
    assertTrue(destDequeueResult.isEmpty());
    destDequeueResult = this.executor.execute(
        new QueueDequeue(destQueueTwo, consumer, config));
    assertTrue(destDequeueResult.isEmpty());
  }

  final byte [] threadedQueueName = Bytes.toBytes("threadedQueue");

  @Test
  public void testThreadedProducersAndThreadedConsumers() throws Exception {

    long MAX_TIMEOUT = 30000;
    OmidTransactionalOperationExecutor.MAX_DEQUEUE_RETRIES = 100;
    OmidTransactionalOperationExecutor.DEQUEUE_RETRY_SLEEP = 1;
    ConcurrentSkipListMap<byte[], byte[]> enqueuedMap =
        new ConcurrentSkipListMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    ConcurrentSkipListMap<byte[], byte[]> dequeuedMapOne =
        new ConcurrentSkipListMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
        ConcurrentSkipListMap<byte[], byte[]> dequeuedMapTwo =
            new ConcurrentSkipListMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);

    AtomicBoolean producersDone = new AtomicBoolean(false);

    long startTime = System.currentTimeMillis();

    // Create P producer threads, each inserts N queue entries
    int p = 5;
    int n = 2000;
    Producer [] producers = new Producer[p];
    for (int i=0;i<p;i++) {
      producers[i] = new Producer(i, n, enqueuedMap);
    }

    // Create (P*2) consumer threads, two groups of (P)
    // Use synchronous execution first
    Consumer [] consumerGroupOne = new Consumer[p];
    Consumer [] consumerGroupTwo = new Consumer[p];
    for (int i=0;i<p;i++) {
      consumerGroupOne[i] = new Consumer(new QueueConsumer(i, 0, p),
          new QueueConfig(new QueuePartitioner.RandomPartitioner(), true),
          dequeuedMapOne, producersDone);
    }
    for (int i=0;i<p;i++) {
      consumerGroupTwo[i] = new Consumer(new QueueConsumer(i, 1, p),
          new QueueConfig(new QueuePartitioner.RandomPartitioner(), true),
          dequeuedMapTwo, producersDone);
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
    if (stop - start >= MAX_TIMEOUT) fail("Timed out waiting for producers");
    producersDone.set(true);

    // Verify producers produced correct number
    assertEquals(p * n , enqueuedMap.size());
    System.out.println("Producers correctly enqueued " + enqueuedMap.size() +
        " entries");

    long producerTime = System.currentTimeMillis();
    System.out.println("" + p + " producers generated " + (n*p) + " total " +
        "queue entries in " + (producerTime - startTime) + " millis (" +
        ((producerTime-startTime)/((float)(n*p))) + " ms/enqueue)");

    // Wait for consumers to finish
    System.out.println("Waiting for consumers to finish");
    start = System.currentTimeMillis();
    for (int i=0; i<p; i++) consumerGroupOne[i].join(MAX_TIMEOUT);
    for (int i=0; i<p; i++) consumerGroupTwo[i].join(MAX_TIMEOUT);
    stop = System.currentTimeMillis();
    System.out.println("Consumers done!");
    if (stop - start >= MAX_TIMEOUT) fail("Timed out waiting for consumers");

    long stopTime = System.currentTimeMillis();
    System.out.println("" + (p*2) + " consumers dequeued " +
        (expectedDequeues*2) +
        " total queue entries in " + (stopTime - startConsumers) + " millis (" +
        ((stopTime-startConsumers)/((float)(expectedDequeues*2))) +
        " ms/dequeue)");
    
    // Each group should total <expectedDequeues>

    long groupOneTotal = 0;
    long groupTwoTotal = 0;
    for (int i=0; i<p; i++) {
      groupOneTotal += consumerGroupOne[i].dequeued;
      groupTwoTotal += consumerGroupTwo[i].dequeued;
    }
    if (expectedDequeues != groupOneTotal) {
      System.out.println("Group One: totalDequeues=" + groupOneTotal +
          ", DequeuedMap.size=" + dequeuedMapOne.size());
      for (byte [] dequeuedValue : dequeuedMapOne.values()) {
        enqueuedMap.remove(dequeuedValue);
      }
      System.out.println("After removing dequeued entries, there are " +
          enqueuedMap.size() + " remaining entries produced");
      for (byte [] enqueuedValue : enqueuedMap.values()) {
        System.out.println("EnqueuedNotDequeued: instanceid=" +
            Bytes.toInt(enqueuedValue) + ", entrynum=" +
            Bytes.toInt(enqueuedValue, 4));
      }
      printQueueInfo(threadedQueueName, 0);
    }
    assertEquals(expectedDequeues, groupOneTotal);
    if (expectedDequeues != groupTwoTotal) {
      System.out.println("Group Two: totalDequeues=" + groupTwoTotal +
          ", DequeuedMap.size=" + dequeuedMapTwo.size());
      for (byte [] dequeuedValue : dequeuedMapTwo.values()) {
        enqueuedMap.remove(dequeuedValue);
      }
      System.out.println("After removing dequeued entries, there are " +
          enqueuedMap.size() + " remaining entries produced");
      for (byte [] enqueuedValue : enqueuedMap.values()) {
        System.out.println("EnqueuedNotDequeued: instanceid=" +
            Bytes.toInt(enqueuedValue) + ", entrynum=" +
            Bytes.toInt(enqueuedValue, 4));
      }
      printQueueInfo(threadedQueueName, 1);
    }
    assertEquals(expectedDequeues, groupTwoTotal);
  }

  private void printQueueInfo(byte[] queueName, int groupId) {
    TTQueueTable table = this.handle.getQueueTable(Bytes.toBytes("queues"));
    System.out.println(table.getInfo(queueName, groupId));
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
          byte [] entry = Bytes.add(Bytes.toBytes(this.instanceid),
              Bytes.toBytes(i));
          assertTrue(TestOmidExecutorLikeAFlow.this.executor.execute(
              Arrays.asList(new WriteOperation [] {
                  new QueueEnqueue(TestOmidExecutorLikeAFlow.this.threadedQueueName,
                      entry)})).isSuccess());
          this.enqueuedMap.put(entry, entry);
        } catch (OmidTransactionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
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
      boolean gotEmptyAndDone = false;
      while (true) {
        boolean localProducersDone = producersDone.get();
        QueueDequeue dequeue =
            new QueueDequeue(TestOmidExecutorLikeAFlow.this.threadedQueueName, this.consumer, this.config);
        try {
          DequeueResult result = TestOmidExecutorLikeAFlow.this.executor.execute(dequeue);
          if (result.isSuccess() && this.config.isSingleEntry()) {
            assertTrue(TestOmidExecutorLikeAFlow.this.executor.execute(
                Arrays.asList(new WriteOperation [] {
                    new QueueAck(TestOmidExecutorLikeAFlow.this.threadedQueueName,
                        result.getEntryPointer(), this.consumer)})).isSuccess());
          }
          if (result.isSuccess()) {
            this.dequeued++;
            this.dequeuedMap.put(result.getValue(), result.getValue());
            if (gotEmptyAndDone) {
              System.out.println("Got empty+done then got another dequeue after sleep!");
              gotEmptyAndDone = false;
            }
          } else if (result.isFailure()) {
            fail("Dequeue failed " + result);
          } else if (result.isEmpty() && localProducersDone) {
            if (gotEmptyAndDone) {
              System.out.println("Empty and done looped, result empty again");
              return;
            } else {
              System.out.println(this.consumer.toString() + " finished after " +
                  this.dequeued + " dequeues, sleeping and retrying dequeue");
              Thread.sleep(100);
              gotEmptyAndDone = true;
            }
          } else if (result.isEmpty() && !localProducersDone) {
            System.out.println(this.consumer.toString() + " empty but waiting");
            Thread.sleep(1);
          } else {
            fail("What is this?");
          }
        } catch (SyncReadTimeoutException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        } catch (OmidTransactionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }
  }
}
