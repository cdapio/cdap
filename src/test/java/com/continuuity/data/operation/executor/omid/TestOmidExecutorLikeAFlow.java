package com.continuuity.data.operation.executor.omid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicOracle;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Increment;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.executor.omid.memory.MemoryOracle;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.data.operation.type.WriteOperation;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.table.handles.SimpleOVCTableHandle;

public class TestOmidExecutorLikeAFlow {

  // TODO: Pluggable/injectable of this stuff
  
  private final TimestampOracle timeOracle = new MemoryStrictlyMonotonicOracle();
  private final TransactionOracle oracle = new MemoryOracle(this.timeOracle);
  private final Configuration conf = new Configuration();
  private final OVCTableHandle handle =
      new SimpleOVCTableHandle(this.timeOracle, this.conf);
  private final OmidTransactionalOperationExecutor executor =
      new OmidTransactionalOperationExecutor(this.oracle, this.handle);

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
    this.executor.execute(Arrays.asList(new WriteOperation [] {
        new Write(key, Bytes.toBytes(1L))}));

    // Verify value = 1
    assertTrue(Bytes.equals(Bytes.toBytes(1L),
        this.executor.execute(new Read(key))));

    // Create batch with increment and compareAndSwap
    // first try (CAS(1->3),Increment(3->4))
    // (will fail if operations are reordered)
    assertTrue(this.executor.execute(Arrays.asList(new WriteOperation [] {
        new CompareAndSwap(key, Bytes.toBytes(1L), Bytes.toBytes(3L)),
        new Increment(key, 1L)
    })).isSuccess());

    // verify value = 4
    // (value = 2 if no ReadOwnWrites)
    byte [] value = this.executor.execute(new Read(key));
    assertEquals(4L, Bytes.toLong(value));

    // Create another batch with increment and compareAndSwap, change order
    // second try (Increment(4->5),CAS(5->1))
    // (will fail if operations are reordered or if no ReadOwnWrites)
    assertTrue(this.executor.execute(Arrays.asList(new WriteOperation [] {
        new Increment(key, 1L),
        new CompareAndSwap(key, Bytes.toBytes(5L), Bytes.toBytes(1L))
    })).isSuccess());

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
    assertTrue(this.executor.execute(Arrays.asList(new WriteOperation [] {
        new QueueEnqueue(queueName, Bytes.toBytes(1L))
    })).isSuccess());

    // Dequeue entry just written
    dequeue = new QueueDequeue(queueName, consumer, config);
    result = this.executor.execute(dequeue);
    assertTrue(result.isSuccess());
    assertTrue(Bytes.equals(result.getValue(), Bytes.toBytes(1L)));

    // Ack it
    assertTrue(this.executor.execute(Arrays.asList(new WriteOperation [] {
        new QueueAck(queueName, result.getEntryPointer(), consumer)
    })).isSuccess());

    // Can't ack it again
    assertFalse(this.executor.execute(Arrays.asList(new WriteOperation [] {
        new QueueAck(queueName, result.getEntryPointer(), consumer)
    })).isSuccess());
    
    // Queue should be empty again
    dequeue = new QueueDequeue(queueName, consumer, config);
    result = this.executor.execute(dequeue);
    assertTrue(result.isEmpty());
  }

  @Test @Ignore
  public void testWriteBatchWithMultiWritesMultiEnqueuesPlusSuccessfulAck()
      throws Exception {

    // Verify it reorders queue ops appropriately and does a stable sort
    // of normal writes
  }


  @Test @Ignore
  public void testWriteBatchWithMultiWritesMultiEnqueuesPlusUnsuccessfulAckRollback()
      throws Exception {



    // Add two entries in source queue

    // Dequeue one entry from source queue

    // Create batch of writes

    // Add two user increment operations

    // Add an ack of entry one in source queue

    // Add two pushes to two dest queues

    // Add another user increment operation
    
    
    

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


  }
}
