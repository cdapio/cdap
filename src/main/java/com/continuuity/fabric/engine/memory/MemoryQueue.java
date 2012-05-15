package com.continuuity.fabric.engine.memory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.continuuity.fabric.operations.queues.QueueConsumer;
import com.continuuity.fabric.operations.queues.QueueEntry;
import com.continuuity.fabric.operations.queues.QueuePartitioner;

public class MemoryQueue {

  private Entry head;
  private Entry tail;

  /** Used for locking, generating ids, and tracking the total queue size */
  private final AtomicLong entryIds = new AtomicLong(0);

  /** Map from GroupID to GroupHead */
  private final Map<Integer,Entry> consumerGroupHeads =
      new HashMap<Integer,Entry>();

  public MemoryQueue() {
    this.head = null;
    this.tail = null;
  }

  public boolean push(byte [] value) {
    Entry entry = new Entry(value, entryIds.incrementAndGet());
    synchronized(this.entryIds) {
      if (this.head == null) {
        this.head = entry;
        this.tail = entry;
      } else {
        this.tail.setNext(entry);
        this.tail = entry;
      }
      this.entryIds.notifyAll();
    }
    return true;
  }

  public QueueEntry pop(QueueConsumer consumer, QueuePartitioner partitioner)
  throws InterruptedException {
    // Anything in the queue at all?  Wait for a push if so
    if (head == null) {
      waitForPush();
      return pop(consumer, partitioner);
    }
    
    // Determine the current head for this consumer group
    Entry groupHead = null;
    if (!this.consumerGroupHeads.containsKey(consumer.getGroupId())) {
      // This group has not consumed before, set group head = head
      this.consumerGroupHeads.put(consumer.getConsumerId(), head);
      groupHead = head;
    } else {
      groupHead = this.consumerGroupHeads.get(consumer.getConsumerId());
    }
    
    // If groupHead is null, we are at the end, wait for a push
    if (groupHead == null) {
      waitForPush();
      return pop(consumer, partitioner);
    }
    
    // Iterate entries to see if we should emit one
    while (groupHead != null) {
      QueueEntry entry = groupHead.makeQueueEntry();
      if (partitioner.shouldEmit(consumer, entry)) {
        entry.setConsumer(consumer);
        return entry;
      }
      groupHead = groupHead.getNext();
    }
    
    // Didn't find anything.  Wait and try again.
    waitForPush();
    return pop(consumer, partitioner);
  }

  public boolean ack(QueueEntry entry) {

    
    // TODO Auto-generated method stub
    return false;
  }

  private void waitForPush() throws InterruptedException {
    synchronized (this.entryIds) {
      long start = this.entryIds.get();
      while (this.entryIds.get() == start) {
        this.entryIds.wait();
      }
    }
  }

  class Entry {
    private final byte [] value;
    private final long id;
    private Entry next;
    Entry(byte [] value, long id) {
      this.value = value;
      this.id = id;
      this.next = null;
    }
    public byte [] getValue() {
      return this.value;
    }
    public long getId() {
      return this.id;
    }
    public void setNext(Entry entry) {
      this.next = entry;
    }
    public Entry getNext() {
      return this.next;
    }
    public QueueEntry makeQueueEntry() {
      return new QueueEntry(this.value, this.id);
    }
  }
}
