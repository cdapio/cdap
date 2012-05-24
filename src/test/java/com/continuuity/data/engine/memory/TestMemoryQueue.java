package com.continuuity.data.engine.memory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.continuuity.data.operation.queue.QueueConfig;
import com.continuuity.data.operation.queue.QueueConsumer;
import com.continuuity.data.operation.queue.QueueEntry;
import com.continuuity.data.operation.queue.QueuePartitioner;

public class TestMemoryQueue {

  private static final long MAX_TIMEOUT_MS = 20;
  private static final long POP_BLOCK_TIMEOUT_MS = 1;
  private static final boolean sync = true;
  private static final boolean drain = false;
  
  @Test
  public void testSingleConsumerSimple() throws Exception {
    MemoryQueue queue = new MemoryQueue();

    byte [] valueOne = Bytes.toBytes("value1");
    byte [] valueTwo = Bytes.toBytes("value2");

    // push two entries
    assertTrue(queue.push(valueOne));
    assertTrue(queue.push(valueTwo));

    // pop it with the single consumer and random partitioner
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, sync, drain);
    QueuePartitioner partitioner = new QueuePartitioner.RandomPartitioner();
    QueueConfig config = new QueueConfig(partitioner, drain);
    QueueEntry entry = queue.pop(consumer, config, drain);

    // verify it's the first value
    assertTrue(Bytes.equals(entry.getValue(), valueOne));

    // pop again without acking, should still get first value
    entry = queue.pop(consumer, config, drain);
    assertTrue(Bytes.equals(entry.getValue(), valueOne));

    // ack
    assertTrue(queue.ack(entry));

    // pop, should get second value
    entry = queue.pop(consumer, config, drain);
    assertTrue(Bytes.equals(entry.getValue(), valueTwo));

    // ack
    assertTrue(queue.ack(entry));
    
    // verify queue is empty
    queue.sync = false;
    assertNull(queue.pop(consumer, config, drain));
    queue.sync = true;
  }

  @Test
  public void testSingleConsumerThreaded() throws Exception {
    MemoryQueue queue = new MemoryQueue();

    byte [] valueOne = Bytes.toBytes("value1");
    byte [] valueTwo = Bytes.toBytes("value2");


    // pop it with the single consumer and random partitioner
    QueueConsumer consumer = new QueueConsumer(0, 0, 1, sync, drain);
    QueuePartitioner partitioner = new QueuePartitioner.RandomPartitioner();
    QueueConfig config = new QueueConfig(partitioner, drain);
    
    // spawn a thread to pop
    QueuePopper popper = new QueuePopper(queue, consumer, partitioner);
    popper.start();
    
    // popper should be empty
    assertNull(popper.nonBlockPop());
    
    // trigger a pop
    assertTrue(popper.triggerPop());

    // nothing in queue so popper should be empty
    QueueEntry entry = popper.blockPop(POP_BLOCK_TIMEOUT_MS);
    assertNull(entry);

    // push
    assertTrue(queue.push(valueOne));
    
    // popper will pop, but we should still be able to pop!
    entry = queue.pop(consumer, config, drain);
    assertNotNull(entry);
    assertTrue(Bytes.equals(entry.getValue(), valueOne));
    
    // popper should also have this loaded
    entry = popper.blockPop(POP_BLOCK_TIMEOUT_MS);
    assertNotNull(entry);
    assertTrue(Bytes.equals(entry.getValue(), valueOne));

    // ack it!
    queue.ack(entry);
    
    // trigger another pop
    assertTrue(popper.triggerPop());

    // nothing in queue so popper should be empty
    entry = popper.blockPop(POP_BLOCK_TIMEOUT_MS);
    assertNull(entry);

    // push
    assertTrue(queue.push(valueTwo));
    
    // popper should have value2
    entry = popper.blockPop(POP_BLOCK_TIMEOUT_MS);
    assertNotNull(entry);
    assertTrue(Bytes.equals(entry.getValue(), valueTwo));
    
    // trigger popper again, should get the same one back
    assertTrue(popper.triggerPop());
    entry = popper.blockPop(POP_BLOCK_TIMEOUT_MS);
    assertNotNull(entry);
    assertTrue(Bytes.equals(entry.getValue(), valueTwo));

    // ack it!
    queue.ack(entry);
    
    // verify queue is empty
    queue.sync = false;
    assertNull(queue.pop(consumer, config, drain));
    queue.sync = true;
    
    // shut down
    popper.shutdown();
  }

  @Test
  public void testMultiConsumerMultiGroup() throws Exception {
    MemoryQueue queue = new MemoryQueue();

    // Create 4 consumer groups with 4 consumers each
    int n = 4;
    QueueConsumer [][] consumers = new QueueConsumer[n][];
    for (int i=0; i<n; i++) {
      consumers[i] = new QueueConsumer[n];
      for (int j=0; j<n; j++) {
        consumers[i][j] = new QueueConsumer(j, i, n, sync, drain);
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
      public boolean shouldEmit(QueueConsumer consumer, QueueEntry entry) {
        long val = Bytes.toLong(entry.getValue());
        return (val % consumer.getGroupSize()) == consumer.getConsumerId();
      }
    };
    QueueConfig config = new QueueConfig(partitioner, drain);
    
    // Start a popper for every consumer!
    QueuePopper [][] poppers = new QueuePopper[n][];
    for (int i=0; i<n; i++) {
      poppers[i] = new QueuePopper[n];
      for (int j=0; j<n; j++) {
        poppers[i][j] = new QueuePopper(queue, consumers[i][j], partitioner);
        poppers[i][j].start();
        assertTrue(poppers[i][j].triggerPop());
      }
    }
    
    // verify everyone is empty
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        assertNull(poppers[i][j].blockPop(POP_BLOCK_TIMEOUT_MS));
      }
    }
    
    // no pops yet
    long numPops = 0L;
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        assertEquals(numPops, poppers[i][j].pops.get());
      }
    }
    
    // push the first four values
    for (int i=0; i<n; i++) {
      assertTrue(queue.push(values[i]));
    }
    
    // wait for 16 queuePop() wake-ups
    long expectedQueuePops = 16;
    waitForAndAssertCount(expectedQueuePops, queue.wakeUps);
    
    // every popper/consumer should have one entry
    numPops++;
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        waitForAndAssertCount(numPops, poppers[i][j].pops);
        QueueEntry entry = poppers[i][j].blockPop(POP_BLOCK_TIMEOUT_MS);
        assertNotNull(entry);
        assertEquals((long)j, Bytes.toLong(entry.getValue()));
      }
    }
    
    // trigger pops again, should get the same entries
    numPops++;
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        assertTrue(poppers[i][j].triggerPop());
        waitForAndAssertCount(numPops, poppers[i][j].pops);
      }
    }
    
    // every popper/consumer should have one entry
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        QueueEntry entry = poppers[i][j].blockPop(POP_BLOCK_TIMEOUT_MS);
        assertNotNull(entry);
        assertEquals((long)j, Bytes.toLong(entry.getValue()));
      }
    }
    
    // directly popping should also yield the same entry
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        QueueEntry entry = queue.pop(consumers[i][j], config, drain);
        assertNotNull(entry);
        assertEquals((long)j, Bytes.toLong(entry.getValue()));
      }
    }
    
    // ack it for groups(0,1) consumers(2,3)
    for (int i=0; i<n/2; i++) {
      for (int j=n/2; j<n; j++) {
        QueueEntry entry = queue.pop(consumers[i][j], config, drain);
        assertNotNull(entry);
        assertEquals((long)j, Bytes.toLong(entry.getValue()));
        assertTrue(queue.ack(entry));
        System.out.println("ACK: i=" + i + ", j=" + j);
      }
    }
    
    // trigger poppers
    numPops++;
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        waitForAndAssertCount(numPops, poppers[i][j].popRunLoop);
        assertTrue(poppers[i][j].triggerPop());
        waitForAndAssertCount(numPops, poppers[i][j].triggers);
      }
    }
    
    // expect null for groups(0,1) consumers(2,3), same value for others
    // ack everyone not ackd
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        if ((i == 0 || i == 1) && (j == 2 || j == 3)) {
          assertNull(poppers[i][j].blockPop(POP_BLOCK_TIMEOUT_MS));
        } else {
          System.out.println("POP: i=" + i + ", j=" + j);
          QueueEntry entry = queue.pop(consumers[i][j], config, drain);
          assertNotNull(entry);
          assertEquals((long)j, Bytes.toLong(entry.getValue()));
          assertTrue(queue.ack(entry));
          // buffer of popper should still have that entry
          waitForAndAssertCount(numPops, poppers[i][j].pops);
          entry = poppers[i][j].blockPop(POP_BLOCK_TIMEOUT_MS);
          assertNotNull(entry);
          assertEquals((long)j, Bytes.toLong(entry.getValue()));
        }
      }
    }
    
    // trigger pops again, will get false for groups(0,1) consumers(2,3)
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        boolean popStatus = poppers[i][j].triggerPop();
        if ((i == 0 || i == 1) && (j == 2 || j == 3)) {
          assertFalse(popStatus);
        } else {
          if (!popStatus) System.out.println("Failed Trigger Pop {i=" +
              i + ", j=" + j + "}");
          assertTrue("Expected to be able to trigger a pop but was not {i=" +
              i + ", j=" + j +"}", popStatus);
        }
      }
    }
    // re-align counters so we can keep testing sanely
    // for groups(0,1) consumers(2,3)
    for (int i=0; i<n/2; i++) {
      for (int j=n/2; j<n; j++) {
        poppers[i][j].pops.incrementAndGet();
      }
    }
    
    // everyone should be empty
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        assertNull(poppers[i][j].blockPop(POP_BLOCK_TIMEOUT_MS));
      }
    }
    
    // push everything!
    for (int i=0; i<n*n; i++) {
      assertTrue(queue.push(values[i]));
    }
    
    // wait for 16 more queuePop() wake-ups
    expectedQueuePops += 16;
    waitForAndAssertCount(expectedQueuePops, queue.wakeUps);
    numPops++;
    
    // pop and ack everything.  each consumer should have 4 things!
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        long localPops = numPops;
        for (int k=0; k<n; k++) {
          waitForAndAssertCount(localPops, poppers[i][j].pops);
          QueueEntry entry = poppers[i][j].blockPop(POP_BLOCK_TIMEOUT_MS);
          assertNotNull(entry);
          assertEquals((long)(k*n)+j, Bytes.toLong(entry.getValue()));
          assertTrue(queue.ack(entry));
          poppers[i][j].triggerPop();
          localPops++;
        }
      }
    }

    // everyone should be empty
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        assertNull(poppers[i][j].blockPop(POP_BLOCK_TIMEOUT_MS));
      }
    }
    
    // everyone should be empty
    for (int i=0; i<n; i++) {
      for (int j=0; j<n; j++) {
        poppers[i][j].shutdown();
      }
    }
  }
  
  private void waitForAndAssertCount(long expected, AtomicLong wakeUps) {
    long start = System.currentTimeMillis();
    long end = start + MAX_TIMEOUT_MS;
    while (expected > wakeUps.get() &&
        System.currentTimeMillis() < end) {
      try {
        Thread.sleep(POP_BLOCK_TIMEOUT_MS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    assertEquals("Exiting waitForWakeUps (expected=" + expected + ", " +
        "actual=" + wakeUps.get(), expected, wakeUps.get());
  }

  class QueuePopper extends Thread {
    
    private final MemoryQueue queue;
    private final QueueConsumer consumer;
    private final QueueConfig config;
    
    private AtomicBoolean popTrigger = new AtomicBoolean(false);
    
    AtomicLong triggers = new AtomicLong(0);
    AtomicLong pops = new AtomicLong(0);
    AtomicLong popRunLoop = new AtomicLong(0);
    
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
      this.config = new QueueConfig(partitioner, drain);
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
        this.entry = null;
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
          popRunLoop.incrementAndGet();
          while (!popTrigger.get() && keepgoing) {
            try {
              popTrigger.wait();
            } catch (InterruptedException e) {
              if (keepgoing) e.printStackTrace();
            }
          }
          triggers.incrementAndGet();
        }
        QueueEntry entry = null;
        while (entry == null && keepgoing) {
          try {
            entry = queue.pop(consumer, config, drain);
          } catch (InterruptedException e) {
            if (keepgoing) e.printStackTrace();
          }
        }
        popTrigger.set(false);
        this.entry = entry;
        pops.incrementAndGet();
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
