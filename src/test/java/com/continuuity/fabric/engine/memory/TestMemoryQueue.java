package com.continuuity.fabric.engine.memory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.continuuity.fabric.operations.queues.QueueConsumer;
import com.continuuity.fabric.operations.queues.QueueEntry;
import com.continuuity.fabric.operations.queues.QueuePartitioner;

public class TestMemoryQueue {

  @Test
  public void testSingleConsumerSimple() throws Exception {
    MemoryQueue queue = new MemoryQueue();

    byte [] valueOne = Bytes.toBytes("value1");
    byte [] valueTwo = Bytes.toBytes("value2");

    // push two entries
    assertTrue(queue.push(valueOne));
    assertTrue(queue.push(valueTwo));

    // pop it with the single consumer and random partitioner
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueuePartitioner partitioner = new QueuePartitioner.RandomPartitioner();
    QueueEntry entry = queue.pop(consumer, partitioner);

    // verify it's the first value
    assertTrue(Bytes.equals(entry.getValue(), valueOne));

    // pop again without acking, should still get first value
    entry = queue.pop(consumer, partitioner);
    assertTrue(Bytes.equals(entry.getValue(), valueOne));

    // ack
    assertTrue(queue.ack(entry));

    // pop, should get second value
    entry = queue.pop(consumer, partitioner);
    assertTrue(Bytes.equals(entry.getValue(), valueTwo));

    // ack
    assertTrue(queue.ack(entry));
  }

  @Test
  public void testSingleConsumerThreaded() throws Exception {
    MemoryQueue queue = new MemoryQueue();

    byte [] valueOne = Bytes.toBytes("value1");
    byte [] valueTwo = Bytes.toBytes("value2");


    // pop it with the single consumer and random partitioner
    QueueConsumer consumer = new QueueConsumer(0, 0, 1);
    QueuePartitioner partitioner = new QueuePartitioner.RandomPartitioner();
    
    // spawn a thread to pop
    QueuePopper popper = new QueuePopper(queue, consumer, partitioner);
    popper.start();
    
    // popper should be empty
    assertNull(popper.nonBlockPop());
    
    // trigger a pop
    assertTrue(popper.triggerPop());

    // nothing in queue so popper should be empty
    QueueEntry entry = popper.blockPop(100);
    assertNull(entry);

    // push
    assertTrue(queue.push(valueOne));
    assertNotNull(queue.pop(consumer, partitioner));
    
    Thread.sleep(1000);
    // popper should hit now
    entry = popper.blockPop(100);
    assertNotNull(entry);
    assertTrue(Bytes.equals(entry.getValue(), valueOne));
    
    // shut down
    popper.shutdown();
  }

  static class QueuePopper extends Thread {
    
    private final MemoryQueue queue;
    private final QueueConsumer consumer;
    private final QueuePartitioner partitioner;
    
    private AtomicBoolean popTrigger = new AtomicBoolean(false);
    
    private QueueEntry entry;
    
    private boolean keepgoing = true;
    
    /**
     * @param queue
     * @param consumer
     * @param partitioner
     */
    QueuePopper(MemoryQueue queue, QueueConsumer consumer,
        QueuePartitioner partitioner) {
      this.queue = queue;
      this.consumer = consumer;
      this.partitioner = partitioner;
      this.entry = null;
    }

    QueueEntry nonBlockPop() {
      if (this.entry == null) return null;
      QueueEntry ret = this.entry;
      this.entry = null;
      return ret;
    }
    
    QueueEntry blockPop(long timeout) {
      QueueEntry entry = nonBlockPop();
      if (entry != null) return entry;
      long cur = System.currentTimeMillis();
      long end = cur + timeout;
      while (entry == null && cur < end) {
        synchronized (popTrigger) {
          try {
            popTrigger.wait(timeout);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        entry = nonBlockPop();
        cur = System.currentTimeMillis();
      }
      return entry;
    }
    /**
     * trigger the thread to pop.
     * @return true if a pop was triggered, false if not
     */
    boolean triggerPop() {
      synchronized (popTrigger) {
        if (popTrigger.get()) {
          return false;
        }
        popTrigger.set(true);
        popTrigger.notifyAll();
        return true;
      }
    }
    
    public void shutdown() {
      this.keepgoing = false;
      this.interrupt();
    }

    @Override
    public void run() {
      while (keepgoing) {
        synchronized (popTrigger) {
          while (!popTrigger.get() && keepgoing) {
            try {
              popTrigger.wait();
            } catch (InterruptedException e) {
              if (keepgoing) e.printStackTrace();
            }
          }
          popTrigger.set(false);
        }
        if (this.entry != null) continue;
        QueueEntry entry = null;
        while (entry == null && keepgoing) {
          try {
            entry = queue.pop(consumer, partitioner);
          } catch (InterruptedException e) {
            if (keepgoing) e.printStackTrace();
          }
        }
        this.entry = entry;
      }
    }
  }
  @Test
  public void testHashMapBehavior() {
    byte [] value = Bytes.toBytes("value");
    Map<Integer,byte[]> map = new HashMap<Integer,byte[]>();
    assertFalse(map.containsKey(1));
    assertNull(map.get(1));
    map.put(1, value);
    assertTrue(map.containsKey(1));
    map.put(1, null);
    assertTrue(map.containsKey(1));
    assertNull(map.get(1));
  }
}
