package com.continuuity.fabric.engine.memory;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.continuuity.fabric.operations.queues.QueueConsumer;
import com.continuuity.fabric.operations.queues.QueueEntry;
import com.continuuity.fabric.operations.queues.QueuePartitioner;

public class MemoryQueue {

  /** Maximum default timeout of 5 minutes (can be changed by tests) */
  static long TIMEOUT = 5 * 60 * 1000;
  
  Entry head;
  Entry tail;

  /** Used for locking, generating ids, and tracking the total queue size */
  private final AtomicLong entryIds = new AtomicLong(0);

  /** Map from GroupID to GroupHead */
  private final ConcurrentHashMap<Integer,ConsumerGroup> consumerGroups =
      new ConcurrentHashMap<Integer,ConsumerGroup>();

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
    
    // Determine the current state for this consumer group
    ConsumerGroup group = null;
    if (!this.consumerGroups.containsKey(consumer.getGroupId())) {
      // This group has not consumed before, set group head = head
      group = new ConsumerGroup(consumer.getGroupId());
      group.setHead(head);
      ConsumerGroup existingGroup = this.consumerGroups.putIfAbsent(
          consumer.getGroupId(), group);
      if (existingGroup != null) {
        // Someone else added the group concurrently with us, use theirs
        group = existingGroup;
      }
    } else {
      group = this.consumerGroups.get(consumer.getConsumerId());
    }
    
    // If group has no entries available, wait for a push
    if (!group.hasEntriesAvailable()) {
      waitForPush();
      return pop(consumer, partitioner);
    }
    
    // Iterate entries to see if we should emit one
    synchronized (group) {
      Entry curEntry = group.getHead();
      while (curEntry != null) {
        // Check if this is already assigned to someone else in the same group
        // or if it's already assigned to us
        GroupConsumptionInfo info =
            curEntry.getConsumerInfo(consumer.getGroupId());
        if (info.isAvailable() ||
            info.getConsumerId() == consumer.getConsumerId()) {
          QueueEntry entry = curEntry.makeQueueEntry();
          if (partitioner.shouldEmit(consumer, entry)) {
            entry.setConsumer(consumer);
            info.setPopConsumer(consumer);
            return entry;
          }
        }
        // Entry is taken or not applicable to this consumer, iterate to next
        curEntry = curEntry.getNext();
      }
    }
      
    // Didn't find anything.  Wait and try again.
    waitForPush();
    return pop(consumer, partitioner);
  }

  public boolean ack(QueueEntry entry) {
    // Queue should not be empty
    if (this.head == null) return false;
    
    QueueConsumer consumer = entry.getConsumer();

    // Group info should not be null
    ConsumerGroup group = this.consumerGroups.get(consumer.getGroupId());
    if (group == null) return false;
    
    synchronized (group) {
      Entry curEntry = group.getHead();
      while (curEntry != null) {
        // Check if this is the entry we want to ack
        GroupConsumptionInfo info =
            curEntry.getConsumerInfo(consumer.getGroupId());
        if (info.isPopped() &&
            info.getConsumerId() == consumer.getConsumerId()) {
          // Verified it was previously popped and owned by us, ack
          info.ack();
          // See if we need to update HEAD pointer
          if (curEntry.equals(group.getHead())) {
            // We just ACKd the head, try move the head down
            curEntry = curEntry.getNext();
            while (curEntry != null) {
              group.setHead(curEntry);
              if (curEntry.getConsumerInfo(consumer.getGroupId()).isAcked()) {
                curEntry = curEntry.getNext();
              } else {
                break;
              }
            }
          }
          return true;
        }
        // Entry is taken or not applicable to this consumer, iterate to next
        curEntry = curEntry.getNext();
      }
    }
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

  class ConsumerGroup {
    private final int id;
    private Entry head;
    private boolean empty;
    ConsumerGroup(int id) {
      this.id = id;
      this.head = null;
    }
    public void setEmpty(boolean b) {
      this.empty = true;
    }
    public boolean hasEntriesAvailable() {
      return !empty;
    }
    public void setHead(Entry head) {
      this.head = head;
    }
    public Entry getHead() {
      return this.head;
    }
    int getId() {
      return this.id;
    }
  }

  class Entry {
    private final byte [] value;
    private final long id;
    private Entry next;
    
    private final Map<Integer,GroupConsumptionInfo> consumers =
        new TreeMap<Integer,GroupConsumptionInfo>();

    Entry(byte [] value, long id) {
      this.value = value;
      this.id = id;
      this.next = null;
    }
    byte [] getValue() {
      return this.value;
    }
    long getId() {
      return this.id;
    }
    void setNext(Entry entry) {
      this.next = entry;
    }
    Entry getNext() {
      return this.next;
    }
    synchronized GroupConsumptionInfo getConsumerInfo(int groupId) {
      GroupConsumptionInfo info = this.consumers.get(groupId);
      if (info == null) {
        info = new GroupConsumptionInfo();
        this.consumers.put(groupId, info);
      }
      return info;
    }
    QueueEntry makeQueueEntry() {
      return new QueueEntry(this.value, this.id);
    }
    public boolean equals(Entry other) {
      if (other.getId() == getId()) return true;
      return false;
    }
  }
  
  class GroupConsumptionInfo {
    private int consumerId;
    private long stamp;
    private ConsumptionState state;
    GroupConsumptionInfo() {
      this.consumerId = -1;
      this.state = ConsumptionState.AVAILABLE;
      this.updateStamp();
    }
    /**
     * @return
     */
    public boolean isAcked() {
      return this.state == ConsumptionState.ACKED;
    }
    public void ack() {
      this.state = ConsumptionState.ACKED;
      updateStamp();
    }
    public boolean isPopped() {
      return this.state == ConsumptionState.POPPED;
    }
    /**
     * @param consumer
     */
    public void setPopConsumer(QueueConsumer consumer) {
      this.consumerId = consumer.getConsumerId();
      this.state = ConsumptionState.POPPED;
      updateStamp();
    }
    boolean isAvailable() {
      if (this.state == ConsumptionState.AVAILABLE ||
          this.state == ConsumptionState.TIMED_OUT) {
        return true;
      }
      if (this.state == ConsumptionState.POPPED &&
          this.stamp < (System.currentTimeMillis() - TIMEOUT)) {
        // Timed out!
        this.state = ConsumptionState.TIMED_OUT;
        return true;
      }
      return false;
    }
    public long getStamp() {
      return stamp;
    }
    public void updateStamp() {
      this.stamp = System.currentTimeMillis();
    }
    public int getConsumerId() {
      return consumerId;
    }
  }
  
  enum ConsumptionState {
    AVAILABLE, POPPED, ACKED, TIMED_OUT
  }
}
