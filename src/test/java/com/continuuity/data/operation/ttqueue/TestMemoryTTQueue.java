package com.continuuity.data.operation.ttqueue;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.memory.MemoryOVCTable;
import com.continuuity.data.operation.executor.ReadPointer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestMemoryTTQueue extends TestTTQueue {

  @Override
  protected TTQueue createQueue(CConfiguration conf) {
    return new TTQueueOnVCTable(
        new MemoryOVCTable(Bytes.toBytes("TestMemoryTTQueue")),
        Bytes.toBytes("TestTTQueue"),
        TestTTQueue.timeOracle, conf);
  }

  @Override
  protected int getNumIterations() {
    return 250;
  }

  @Test @Ignore
  public void testSingleConsumerPlenty() throws Exception {
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = getCleanPointer(version);
    long nanoStart=System.nanoTime();
    int m=1000;
    for (int j=0; j<m; j++) {
      System.out.println("Run no. "+j);
      // enqueue thousand entries
      int n=10000;
      for (int i=0;i<n;i++) {
        boolean success = queue.enqueue(Bytes.toBytes(i), version).isSuccess();
        if (!success) System.out.println("Enqueue failure!");
      }
      long nanoEnqueue=System.nanoTime();
      // dequeue it with the single consumer and FIFO partitioner
      QueueConsumer consumer = new QueueConsumer(0, 0, 1, new QueueConfig(QueuePartitioner.PartitionerType.FIFO, false));
      // dequeue thousand, and ack them
      for (int i=0; i<n; i++) {
        DequeueResult result = queue.dequeue(consumer, readPointer);
        // System.out.println("Dequeue value "+Bytes.toInt(result.getValue()));
        if (Bytes.equals(result.getValue(), Bytes.toBytes(i)) == false) {
          System.out.println("Dequeue failure!");
        }
        queue.ack(result.getEntryPointer(), consumer, readPointer);
        queue.finalize(result.getEntryPointer(), consumer, 1, readPointer.getMaximum());
      }
      long nanoDequeue=System.nanoTime();
      double avgEnqueue=(nanoEnqueue-nanoStart)/n;
      double avgDequeue=(nanoDequeue-nanoEnqueue)/n;
      System.out.println("Total time in milli: "+(nanoDequeue-nanoStart)/1000000);
      System.out.println("Single avg enqueue time in nano: "+avgEnqueue);
      System.out.println("Single avg dequeue time in nano: "+avgDequeue);
      // verify queue is empty
      boolean isEmpty = queue.dequeue(consumer, readPointer).isEmpty();
      if (!isEmpty) System.out.println("Dequeue problem!");
      assertTrue(queue.dequeue(consumer, readPointer).isEmpty());
    }
  }

  @Test @Ignore
  public void testMultiConsumerPlenty() throws Exception {
    TTQueue queue = createQueue();
    long version = timeOracle.getTimestamp();
    ReadPointer readPointer = getCleanPointer(version);
    long nanoStart=System.nanoTime();
    int m=10000;
    for (int j=0; j<m; j++) {
      System.out.println("Run no. "+j);
      // enqueue thousand entries
      int n=10000;
      for (int i=0;i<n;i++) {
        boolean success = queue.enqueue(Bytes.toBytes(i), version).isSuccess();
        if (!success) System.out.println("Enqueue failure!");
      }
      long nanoEnqueue=System.nanoTime();
      // dequeue it with the single consumer and FIFO partitioner
      QueueConsumer consumer1 = new QueueConsumer(0, 0, 1, new QueueConfig(QueuePartitioner.PartitionerType.FIFO, false));
      QueueConsumer consumer2 = new QueueConsumer(0, 1, 1, new QueueConfig(QueuePartitioner.PartitionerType.FIFO, false));
      // dequeue thousand, and ack them
      for (int i=0; i<n; i++) {
        DequeueResult result = queue.dequeue(consumer1, readPointer);
        // System.out.println("Dequeue value "+Bytes.toInt(result.getValue()));
        if (Bytes.equals(result.getValue(), Bytes.toBytes(i)) == false) {
          System.out.println("Dequeue failure!");
        }
        queue.ack(result.getEntryPointer(), consumer1, readPointer);
        queue.finalize(result.getEntryPointer(), consumer1, 2, readPointer.getMaximum());
      }
      if (j == 65) {
        System.out.println();
      }
      if (j > 0) { // don't dequeue with second consumer in the first round
        /// so that the second consumer is n entries behind and clears shards
        for (int i=0; i<n; i++) {
          DequeueResult result = queue.dequeue(consumer2, readPointer);
          // System.out.println("Dequeue value "+Bytes.toInt(result.getValue()));
          if (result == null || result.isEmpty() || result.getEntry() == null) {
            System.out.println();
          }
          if (Bytes.equals(result.getValue(), Bytes.toBytes(i)) == false) {
            System.out.println("Dequeue failure!");
          }
          queue.ack(result.getEntryPointer(), consumer2, readPointer);
          queue.finalize(result.getEntryPointer(), consumer2, 2, readPointer.getMaximum());
        }
      }
      long nanoDequeue=System.nanoTime();
      double avgEnqueue=(nanoEnqueue-nanoStart)/n;
      double avgDequeue=(nanoDequeue-nanoEnqueue)/n;
      System.out.println("Total time in milli: "+(nanoDequeue-nanoStart)/1000000);
      System.out.println("Single avg enqueue time in nano: "+avgEnqueue);
      System.out.println("Single avg dequeue time in nano: "+avgDequeue);
      // verify queue is empty
      boolean isEmpty = queue.dequeue(consumer1, readPointer).isEmpty();
      if (!isEmpty) System.out.println("Dequeue problem!");
    }
  }
}

