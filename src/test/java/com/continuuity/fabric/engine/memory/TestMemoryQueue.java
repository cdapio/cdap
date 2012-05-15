package com.continuuity.fabric.engine.memory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.continuuity.fabric.operations.queues.QueueConsumer;
import com.continuuity.fabric.operations.queues.QueueEntry;
import com.continuuity.fabric.operations.queues.QueuePartitioner;

public class TestMemoryQueue {

  @Test
  public void testSingleConsumer() throws Exception {
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
//    assertTrue(queue.ack(entry));
//
//    // pop, should get second value
//    entry = queue.pop(consumer, partitioner);
//    assertTrue(Bytes.equals(entry.getValue(), valueTwo));
//
//    // ack
//    assertTrue(queue.ack(entry));

//    // spawn a thread to pop
//    QueuePopper popper = new QueuePopper(queue, consumer, partitioner);
//    popper.pop();
//
//    // nothing in queue so popper should be empty
//    assertTrue(popper.isEmpty());
//
//
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
